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

app = FastAPI()

# -----------------------------
# Environment variables (Render)
# -----------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URL = os.getenv("GOOGLE_REDIRECT_URL", "")

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/calendar",
    "openid",
    "email",
    "profile",
]

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"

GOOGLE_FREEBUSY_URL = "https://www.googleapis.com/calendar/v3/freeBusy"
GOOGLE_EVENTS_INSERT_URL = "https://www.googleapis.com/calendar/v3/calendars/{calendarId}/events"

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
    url = DATABASE_URL.strip()
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
            # Tokens keyed by customer_id (not email)
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

            # State table stores state -> customer_id mapping
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS oauth_states (
                  state TEXT PRIMARY KEY,
                  customer_id TEXT NOT NULL,
                  created_at TIMESTAMPTZ DEFAULT NOW()
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
    """
    Returns customer_id if state exists, then deletes it.
    """
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


def load_customer_email(customer_id: str) -> Optional[str]:
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT user_email
                FROM oauth_tokens
                WHERE provider='google' AND customer_id=:customer_id
            """),
            {"customer_id": customer_id},
        ).fetchone()
        return row[0] if row else None


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

    return {
        "connected": True,
        "customerId": customer_id,
        "email": user_email,
        "message": "Google connected for this customerId. Use customerId in /google/freebusy and /google/create_event.",
    }


# -----------------------------
# API: FreeBusy + Create Event (customerId)
# -----------------------------
@app.post("/google/freebusy")
async def google_freebusy(payload: Dict[str, Any]):
    """
    {
      "customerId": "pm_123",
      "timeMin": "2026-02-24T08:00:00-07:00",
      "timeMax": "2026-02-24T18:00:00-07:00",
      "timeZone": "America/Denver",
      "calendarIds": ["primary", "some_calendar_id@group.calendar.google.com"]
    }
    """
    require_env()

    customer_id = payload.get("customerId")
    time_min = payload.get("timeMin")
    time_max = payload.get("timeMax")
    tz = payload.get("timeZone", "America/Denver")
    calendar_ids: List[str] = payload.get("calendarIds", ["primary"])

    if not customer_id or not time_min or not time_max:
        raise HTTPException(status_code=400, detail="Missing customerId/timeMin/timeMax.")

    rt = load_refresh_token_by_customer(customer_id)
    if not rt:
        raise HTTPException(status_code=401, detail="Customer not connected. Run /oauth/google/start?customerId=... first.")

    access_token = refresh_access_token(rt)

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
      "customerId": "pm_123",
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
        raise HTTPException(status_code=401, detail="Customer not connected. Run /oauth/google/start?customerId=... first.")

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


# Optional helper endpoint (safe): list connected customers
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

    return [
        {"customerId": r[0], "email": r[1], "connectedAt": str(r[2])}
        for r in rows
    ]
# TEMP ADMIN VIEW (read-only)
@app.get("/__view_tokens")
def view_tokens():
    try:
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT provider, customer_id, user_email, created_at
                FROM oauth_tokens
                ORDER BY created_at DESC
            """)).fetchall()

        return [
            {
                "provider": r[0],
                "customerId": r[1],
                "email": r[2],
                "connectedAt": str(r[3])
            }
            for r in rows
        ]
    except Exception as e:
        return {"error": str(e)}
