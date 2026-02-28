import os
import json
import secrets
from typing import List, Optional, Dict, Any
from urllib.parse import urlencode

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse, JSONResponse

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

app = FastAPI()

# -----------------------------
# Environment variables (Render)
# -----------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "").strip()
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_REDIRECT_URL = os.getenv("GOOGLE_REDIRECT_URL", "").strip()

# Scopes
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/calendar",
    "openid",
    "email",
    "profile",
]

# Google endpoints
GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"

# Google Calendar endpoints
GOOGLE_FREEBUSY_URL = "https://www.googleapis.com/calendar/v3/freeBusy"
GOOGLE_EVENTS_INSERT_URL = "https://www.googleapis.com/calendar/v3/calendars/{calendarId}/events"
GOOGLE_CALENDAR_LIST_URL = "https://www.googleapis.com/calendar/v3/users/me/calendarList"

engine = None


def require_env():
    missing = []
    for k, v in [
        ("DATABASE_URL", DATABASE_URL),
        ("GOOGLE_CLIENT_ID", GOOGLE_CLIENT_ID),
        ("GOOGLE_CLIENT_SECRET", GOOGLE_CLIENT_SECRET),
        ("GOOGLE_REDIRECT_URL", GOOGLE_REDIRECT_URL),
    ]:
        if not v:
            missing.append(k)
    if missing:
        raise HTTPException(status_code=500, detail=f"Missing env vars: {', '.join(missing)}")


def make_engine():
    """
    Render often provides:
      postgres://user:pass@host/db
    SQLAlchemy prefers a driver-qualified URL:
      postgresql+psycopg://user:pass@host/db
    """
    url = DATABASE_URL
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg://", 1)
    elif url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)
    return create_engine(url, pool_pre_ping=True)


def init_db():
    global engine
    require_env()
    engine = make_engine()
    try:
        with engine.begin() as conn:
            # Tokens keyed by (provider, customer_id)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS oauth_tokens (
                  provider TEXT NOT NULL,
                  customer_id TEXT NOT NULL,
                  user_email TEXT,
                  refresh_token TEXT NOT NULL,
                  scope TEXT,
                  token_type TEXT,
                  created_at TIMESTAMPTZ DEFAULT NOW(),
                  PRIMARY KEY (provider, customer_id)
                );
            """))

            # OAuth state -> customer_id mapping
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS oauth_states (
                  state TEXT PRIMARY KEY,
                  customer_id TEXT NOT NULL,
                  created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """))

            # Store calendars per customer; selected determines which are checked
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS customer_calendars (
                  provider TEXT NOT NULL,
                  customer_id TEXT NOT NULL,
                  calendar_id TEXT NOT NULL,
                  summary TEXT,
                  primary_cal BOOLEAN DEFAULT FALSE,
                  selected BOOLEAN DEFAULT TRUE,
                  created_at TIMESTAMPTZ DEFAULT NOW(),
                  PRIMARY KEY (provider, customer_id, calendar_id)
                );
            """))
    except SQLAlchemyError as e:
        raise RuntimeError(f"DB init failed: {e}") from e


init_db()

# -----------------------------
# DB helpers
# -----------------------------
def save_state(state: str, customer_id: str):
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO oauth_states(state, customer_id)
                VALUES (:state, :customer_id)
                ON CONFLICT (state) DO UPDATE SET customer_id = EXCLUDED.customer_id
            """),
            {"state": state, "customer_id": customer_id},
        )


def consume_state(state: str) -> Optional[str]:
    """Return customer_id for a state, then delete the state."""
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT customer_id FROM oauth_states WHERE state=:state"),
            {"state": state},
        ).fetchone()
        if not row:
            return None
        customer_id = row[0]
        conn.execute(text("DELETE FROM oauth_states WHERE state=:state"), {"state": state})
        return customer_id


def upsert_refresh_token(customer_id: str, refresh_token: str, scope: str, token_type: str, user_email: Optional[str]):
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO oauth_tokens(provider, customer_id, user_email, refresh_token, scope, token_type)
                VALUES ('google', :customer_id, :user_email, :rt, :scope, :tt)
                ON CONFLICT (provider, customer_id) DO UPDATE SET
                  user_email = EXCLUDED.user_email,
                  refresh_token = EXCLUDED.refresh_token,
                  scope = EXCLUDED.scope,
                  token_type = EXCLUDED.token_type
            """),
            {
                "customer_id": customer_id,
                "user_email": user_email,
                "rt": refresh_token,
                "scope": scope,
                "tt": token_type,
            },
        )


def load_refresh_token_by_customer(customer_id: str) -> Optional[str]:
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT refresh_token
                FROM oauth_tokens
                WHERE provider='google' AND customer_id=:customer_id
            """),
            {"customer_id": customer_id},
        ).fetchone()
        return row[0] if row else None


def store_calendars(customer_id: str, calendars: List[Dict[str, Any]]):
    with engine.begin() as conn:
        for cal in calendars:
            cal_id = cal.get("id")
            if not cal_id:
                continue
            conn.execute(text("""
                INSERT INTO customer_calendars(provider, customer_id, calendar_id, summary, primary_cal, selected)
                VALUES ('google', :customer_id, :calendar_id, :summary, :primary_cal, TRUE)
                ON CONFLICT (provider, customer_id, calendar_id) DO UPDATE SET
                  summary = EXCLUDED.summary,
                  primary_cal = EXCLUDED.primary_cal
            """), {
                "customer_id": customer_id,
                "calendar_id": cal_id,
                "summary": cal.get("summary"),
                "primary_cal": bool(cal.get("primary")),
            })


def load_selected_calendar_ids(customer_id: str) -> List[str]:
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT calendar_id
            FROM customer_calendars
            WHERE provider='google' AND customer_id=:customer_id AND selected=TRUE
            ORDER BY primary_cal DESC, calendar_id ASC
        """), {"customer_id": customer_id}).fetchall()
    return [r[0] for r in rows]


# -----------------------------
# Google OAuth helpers
# -----------------------------
def exchange_code_for_tokens(code: str) -> Dict[str, Any]:
    payload = {
        "code": code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": GOOGLE_REDIRECT_URL,
        "grant_type": "authorization_code",
    }
    r = requests.post(GOOGLE_TOKEN_URL, data=payload, timeout=30)
    if r.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Token exchange failed: {r.text}")
    return r.json()


def refresh_access_token(refresh_token: str) -> str:
    payload = {
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    r = requests.post(GOOGLE_TOKEN_URL, data=payload, timeout=30)
    if r.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Token refresh failed: {r.text}")
    data = r.json()
    if "access_token" not in data:
        raise HTTPException(status_code=400, detail=f"Token refresh returned no access_token: {data}")
    return data["access_token"]


def get_user_email(access_token: str) -> str:
    r = requests.get(
        GOOGLE_USERINFO_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    if r.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Failed to fetch userinfo: {r.text}")
    email = r.json().get("email")
    if not email:
        raise HTTPException(status_code=400, detail="No email returned from userinfo.")
    return email


def fetch_calendar_list(access_token: str) -> List[Dict[str, Any]]:
    r = requests.get(
        GOOGLE_CALENDAR_LIST_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    if r.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Calendar list failed: {r.text}")
    return r.json().get("items", [])


# -----------------------------
# Basic routes
# -----------------------------
@app.get("/")
def root():
    return {"status": "server is alive"}


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/privacy")
def privacy():
    return {
        "policy": "This application accesses your calendar only to read availability and create bookings you request. We do not sell your data."
    }


@app.get("/terms")
def terms():
    return {"terms": "By using this service you authorize the app to read availability and create/update events on your behalf."}


# -----------------------------
# OAuth connect flow (customerId)
# -----------------------------
@app.get("/oauth/google/start")
def google_oauth_start(customerId: str = Query(..., description="Your internal customer/location id")):
    """
    Example:
      /oauth/google/start?customerId=pm_123
    """
    require_env()

    state = secrets.token_urlsafe(24)
    save_state(state, customerId)

    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URL,
        "response_type": "code",
        "scope": " ".join(GOOGLE_SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "true",
        "state": state,
    }
    return RedirectResponse(f"{GOOGLE_AUTH_URL}?{urlencode(params)}")


@app.get("/oauth/google/callback")
def google_oauth_callback(code: Optional[str] = None, state: Optional[str] = None, error: Optional[str] = None):
    require_env()

    if error:
        raise HTTPException(status_code=400, detail=f"Google OAuth error: {error}")
    if not code or not state:
        raise HTTPException(status_code=400, detail="Missing code/state.")

    customer_id = consume_state(state)
    if not customer_id:
        raise HTTPException(status_code=400, detail="Invalid or expired state.")

    tokens = exchange_code_for_tokens(code)

    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    scope = tokens.get("scope", "")
    token_type = tokens.get("token_type", "")

    if not access_token:
        raise HTTPException(status_code=400, detail=f"No access_token returned: {tokens}")

    user_email = get_user_email(access_token)

    # If Google doesn't return refresh_token (already consented), we require reconnect/remove access.
    if not refresh_token:
        existing = load_refresh_token_by_customer(customer_id)
        if not existing:
            return JSONResponse(
                {
                    "connected": False,
                    "customerId": customer_id,
                    "email": user_email,
                    "message": "No refresh token returned. Remove the app from Google Account permissions and try again.",
                }
            )
        refresh_token = existing

    upsert_refresh_token(customer_id, refresh_token, scope, token_type, user_email)

    # Auto-sync calendars on connect so freebusy can be automatic
    try:
        calendar_items = fetch_calendar_list(access_token)
        store_calendars(customer_id, calendar_items)
    except Exception:
        # Non-fatal; user can call /google/calendars/sync later
        pass

    return {
        "connected": True,
        "customerId": customer_id,
        "email": user_email,
        "message": "Google connected. Calendars synced. Use /google/freebusy and /google/create_event with customerId.",
    }


# -----------------------------
# Calendar sync / management
# -----------------------------
@app.post("/google/calendars/sync")
async def google_calendars_sync(payload: Dict[str, Any]):
    """
    { "customerId": "pm_1" }
    Pull calendar list and store. (Also marks them selected=TRUE by default.)
    """
    require_env()
    customer_id = payload.get("customerId")
    if not customer_id:
        raise HTTPException(status_code=400, detail="Missing customerId.")

    rt = load_refresh_token_by_customer(customer_id)
    if not rt:
        raise HTTPException(status_code=401, detail="Customer not connected. Run /oauth/google/start?customerId=...")

    access_token = refresh_access_token(rt)
    items = fetch_calendar_list(access_token)
    store_calendars(customer_id, items)

    return {"synced": len(items)}


@app.get("/google/calendars")
def google_calendars_list(customerId: str = Query(...)):
    """View stored calendars for a customer (safe; no tokens)."""
    require_env()
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT calendar_id, summary, primary_cal, selected, created_at
            FROM customer_calendars
            WHERE provider='google' AND customer_id=:customer_id
            ORDER BY primary_cal DESC, summary NULLS LAST, calendar_id
        """), {"customer_id": customerId}).fetchall()

    return [
        {
            "calendarId": r[0],
            "summary": r[1],
            "primary": bool(r[2]),
            "selected": bool(r[3]),
            "createdAt": str(r[4]),
        }
        for r in rows
    ]


# -----------------------------
# API: FreeBusy + Create Event (customerId)
# -----------------------------
@app.post("/google/freebusy")
async def google_freebusy(payload: Dict[str, Any]):
    """
    If calendarIds is omitted, uses selected calendars from DB; falls back to ["primary"].

    Body examples:
    {
      "customerId": "pm_1",
      "timeMin": "2026-02-24T08:00:00-07:00",
      "timeMax": "2026-02-24T18:00:00-07:00",
      "timeZone": "America/Denver"
    }

    Or explicitly:
    {
      "customerId": "pm_1",
      "timeMin": "...",
      "timeMax": "...",
      "timeZone": "America/Denver",
      "calendarIds": ["primary", "someone@group.calendar.google.com"]
    }
    """
    require_env()

    customer_id = payload.get("customerId")
    time_min = payload.get("timeMin")
    time_max = payload.get("timeMax")
    tz = payload.get("timeZone", "America/Denver")

    if not customer_id or not time_min or not time_max:
        raise HTTPException(status_code=400, detail="Missing customerId/timeMin/timeMax.")

    rt = load_refresh_token_by_customer(customer_id)
    if not rt:
        raise HTTPException(status_code=401, detail="Customer not connected. Run /oauth/google/start?customerId=...")

    access_token = refresh_access_token(rt)

    calendar_ids = payload.get("calendarIds")

    # Normalize calendarIds (protect against Make sending a string)
    if calendar_ids is None:
        calendar_ids = []
    elif isinstance(calendar_ids, str):
        calendar_ids = [calendar_ids]
    elif not isinstance(calendar_ids, list):
        raise HTTPException(status_code=400, detail="calendarIds must be a list of strings or a single string.")

    calendar_ids = [cid.strip() for cid in calendar_ids if isinstance(cid, str) and cid.strip()]

    # If not provided, use stored selected calendars; fallback to primary
    if not calendar_ids:
        calendar_ids = load_selected_calendar_ids(customer_id) or ["primary"]

    body = {
        "timeMin": time_min,
        "timeMax": time_max,
        "timeZone": tz,
        "items": [{"id": cid} for cid in calendar_ids],
    }

    r = requests.post(
        GOOGLE_FREEBUSY_URL,
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        data=json.dumps(body),
        timeout=30,
    )
    if r.status_code != 200:
        raise HTTPException(status_code=400, detail=f"FreeBusy failed: {r.text}")

    return r.json()


@app.post("/google/create_event")
async def google_create_event(payload: Dict[str, Any]):
    """
    {
      "customerId": "pm_1",
      "calendarId": "primary",
      "summary": "Maintenance visit",
      "description": "Fix sink",
      "start": {"dateTime":"2026-02-24T13:00:00-07:00","timeZone":"America/Denver"},
      "end":   {"dateTime":"2026-02-24T14:00:00-07:00","timeZone":"America/Denver"},
      "attendees": [{"email":"tenant@example.com"}]
    }
    """
    require_env()

    customer_id = payload.get("customerId")
    calendar_id = payload.get("calendarId", "primary")

    if not customer_id:
        raise HTTPException(status_code=400, detail="Missing customerId.")

    rt = load_refresh_token_by_customer(customer_id)
    if not rt:
        raise HTTPException(status_code=401, detail="Customer not connected. Run /oauth/google/start?customerId=...")

    access_token = refresh_access_token(rt)
    url = GOOGLE_EVENTS_INSERT_URL.format(calendarId=calendar_id)

    event_body = {
        "summary": payload.get("summary", "Booking"),
        "description": payload.get("description", ""),
        "start": payload.get("start"),
        "end": payload.get("end"),
    }

    if not event_body["start"] or not event_body["end"]:
        raise HTTPException(status_code=400, detail="Missing start/end in payload.")

    attendees = payload.get("attendees")
    if attendees:
        event_body["attendees"] = attendees

    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        data=json.dumps(event_body),
        timeout=30,
    )
    if r.status_code not in (200, 201):
        raise HTTPException(status_code=400, detail=f"Create event failed: {r.text}")

    return r.json()


# -----------------------------
# Safe helper: list connected customers
# -----------------------------
@app.get("/connections/google")
def list_google_connections():
    require_env()
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT customer_id, user_email, created_at
            FROM oauth_tokens
            WHERE provider='google'
            ORDER BY created_at DESC
            LIMIT 200
        """)).fetchall()

    return [{"customerId": r[0], "email": r[1], "connectedAt": str(r[2])} for r in rows]
    def validate_rfc3339(dt: str):
    try:
        # Accept Z or offset
        if dt.endswith("Z"):
            datetime.fromisoformat(dt.replace("Z", "+00:00"))
        else:
            datetime.fromisoformat(dt)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid RFC3339 datetime: {dt}")
@app.post("/google/freebusy_debug")
async def google_freebusy_debug(payload: Dict[str, Any]):
    require_env()

    customer_id = payload.get("customerId")
    time_min = payload.get("timeMin")
    time_max = payload.get("timeMax")
    tz = payload.get("timeZone", "America/Denver")

    if not customer_id or not time_min or not time_max:
        raise HTTPException(status_code=400, detail="Missing customerId/timeMin/timeMax.")

    rt = load_refresh_token_by_customer(customer_id)
    if not rt:
        raise HTTPException(status_code=401, detail="Customer not connected.")

    access_token = refresh_access_token(rt)

    calendar_ids = payload.get("calendarIds")

    # Normalize
    if calendar_ids is None:
        calendar_ids = []
    elif isinstance(calendar_ids, str):
        calendar_ids = [calendar_ids]
    elif not isinstance(calendar_ids, list):
        raise HTTPException(status_code=400, detail="calendarIds must be a list of strings or a single string.")

    calendar_ids = [cid.strip() for cid in calendar_ids if isinstance(cid, str) and cid.strip()]
    if not calendar_ids:
        calendar_ids = load_selected_calendar_ids(customer_id) or ["primary"]

    google_body = {
        "timeMin": time_min,
        "timeMax": time_max,
        "timeZone": tz,
        "items": [{"id": cid} for cid in calendar_ids],
    }

    # Call Google
    r = requests.post(
        GOOGLE_FREEBUSY_URL,
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        data=json.dumps(google_body),
        timeout=30,
    )

    return {
        "sent_to_google": google_body,
        "google_status": r.status_code,
        "google_response_text": r.text,
    }
