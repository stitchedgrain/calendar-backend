import os
import json
import secrets
from typing import Optional, Dict, Any, List
from urllib.parse import urlencode
from datetime import datetime

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse, JSONResponse

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

app = FastAPI()

# -----------------------------
# ENV
# -----------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "").strip()
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_REDIRECT_URL = os.getenv("GOOGLE_REDIRECT_URL", "").strip()

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
GOOGLE_CALENDAR_LIST_URL = "https://www.googleapis.com/calendar/v3/users/me/calendarList"
GOOGLE_EVENTS_INSERT_URL = "https://www.googleapis.com/calendar/v3/calendars/{calendarId}/events"

engine = None


# -----------------------------
# UTIL
# -----------------------------
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
    url = DATABASE_URL
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg://", 1)
    elif url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)
    return create_engine(url, pool_pre_ping=True)


def validate_rfc3339(dt: str):
    """
    Validates RFC3339-ish datetimes.
    Accepts:
      - 2026-02-28T15:00:00Z
      - 2026-02-28T15:00:00-07:00
      - 2026-02-28T15:00:00+00:00
    """
    try:
        dt2 = dt.strip()
        if dt2.endswith("Z"):
            datetime.fromisoformat(dt2.replace("Z", "+00:00"))
        else:
            datetime.fromisoformat(dt2)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid RFC3339 datetime: {dt}")


# -----------------------------
# DB INIT
# -----------------------------
def init_db():
    global engine
    require_env()
    engine = make_engine()
    try:
        with engine.begin() as conn:
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

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS oauth_states (
                  state TEXT PRIMARY KEY,
                  customer_id TEXT NOT NULL,
                  created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """))

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
conn.execute(text("""
CREATE TABLE IF NOT EXISTS customer_settings (
  customer_id TEXT PRIMARY KEY,
  timezone TEXT NOT NULL DEFAULT 'America/Denver',
  work_start_hour INTEGER NOT NULL DEFAULT 9,
  work_end_hour INTEGER NOT NULL DEFAULT 17,
  work_days TEXT NOT NULL DEFAULT '1,2,3,4,5',
  created_at TIMESTAMPTZ DEFAULT NOW()
);
"""))

# -----------------------------
# DB HELPERS
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
# GOOGLE HELPERS
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
# BASIC ROUTES
# -----------------------------
@app.get("/")
def root():
    return {"status": "server is alive"}


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/privacy")
def privacy():
    return {"policy": "This app reads availability and creates bookings you request. We do not sell data."}


@app.get("/terms")
def terms():
    return {"terms": "By using this service you authorize reading availability and creating/updating events on your behalf."}


# -----------------------------
# OAUTH
# -----------------------------
@app.get("/oauth/google/start")
def google_oauth_start(customerId: str = Query(..., description="Your internal customer/location id")):
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
                    "message": "No refresh token returned. Remove app access in Google Account and try again.",
                }
            )
        refresh_token = existing

    upsert_refresh_token(customer_id, refresh_token, scope, token_type, user_email)

    # auto-sync calendars for automatic freebusy
    try:
        calendars = fetch_calendar_list(access_token)
        store_calendars(customer_id, calendars)
    except Exception:
        pass

    return {
        "connected": True,
        "customerId": customer_id,
        "email": user_email,
        "message": "Google connected. Calendars synced. Use /google/freebusy and /google/create_event with customerId.",
    }


# -----------------------------
# CALENDAR SYNC / VIEW
# -----------------------------
@app.post("/google/calendars/sync")
async def google_calendars_sync(payload: Dict[str, Any]):
    require_env()
    customer_id = payload.get("customerId")
    if not customer_id:
        raise HTTPException(status_code=400, detail="Missing customerId.")

    rt = load_refresh_token_by_customer(customer_id)
    if not rt:
        raise HTTPException(status_code=401, detail="Customer not connected. Run /oauth/google/start?customerId=...")

    access_token = refresh_access_token(rt)
    calendars = fetch_calendar_list(access_token)
    store_calendars(customer_id, calendars)
    return {"synced": len(calendars)}


@app.get("/google/calendars")
def google_calendars_list(customerId: str = Query(...)):
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
# FREEBUSY
# -----------------------------
@app.post("/google/freebusy")
async def google_freebusy(payload: Dict[str, Any]):
    require_env()

    customer_id = payload.get("customerId")
    time_min = payload.get("timeMin")
    time_max = payload.get("timeMax")
    tz = payload.get("timeZone", "America/Denver")

    if not customer_id or not time_min or not time_max:
        raise HTTPException(status_code=400, detail="Missing customerId/timeMin/timeMax.")

    # Validate times early (catches bad dates like Feb 29 on non-leap years)
    validate_rfc3339(time_min)
    validate_rfc3339(time_max)

    rt = load_refresh_token_by_customer(customer_id)
    if not rt:
        raise HTTPException(status_code=401, detail="Customer not connected. Run /oauth/google/start?customerId=...")

    access_token = refresh_access_token(rt)

    calendar_ids = payload.get("calendarIds")

    # Normalize calendarIds (Make sometimes sends a string)
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

    google_body = {
        "timeMin": time_min.strip(),
        "timeMax": time_max.strip(),
        "timeZone": tz,
        "items": [{"id": cid} for cid in calendar_ids],
    }

    r = requests.post(
        GOOGLE_FREEBUSY_URL,
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        data=json.dumps(google_body),
        timeout=30,
    )
    if r.status_code != 200:
        raise HTTPException(status_code=400, detail=f"FreeBusy failed: {r.text}")

    return r.json()


# -----------------------------
# CREATE EVENT
# -----------------------------
@app.post("/google/create_event")
async def google_create_event(payload: Dict[str, Any]):
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
# SAFE: list connections
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
@app.post("/google/calendars/select")
async def google_calendars_select(payload: Dict[str, Any]):
    customer_id = payload.get("customerId")
    selected_ids = payload.get("calendarIds")

    if not customer_id or not isinstance(selected_ids, list) or not selected_ids:
        raise HTTPException(status_code=400, detail="Send customerId and calendarIds (list).")

    selected_ids = [s.strip() for s in selected_ids if isinstance(s, str) and s.strip()]
    if not selected_ids:
        raise HTTPException(status_code=400, detail="calendarIds must be non-empty strings.")

    with engine.begin() as conn:
        # mark all false
        conn.execute(text("""
            UPDATE customer_calendars
            SET selected = FALSE
            WHERE provider='google' AND customer_id=:customer_id
        """), {"customer_id": customer_id})

        # mark only chosen true
        conn.execute(text("""
            UPDATE customer_calendars
            SET selected = TRUE
            WHERE provider='google' AND customer_id=:customer_id
              AND calendar_id = ANY(:ids)
        """), {"customer_id": customer_id, "ids": selected_ids})

    return {"customerId": customer_id, "selectedCalendarIds": selected_ids}
@app.post("/customer/settings")
async def set_customer_settings(payload: Dict[str, Any]):
    require_env()

    customer_id = payload.get("customerId")
    timezone_name = payload.get("timeZone", "America/Denver")
    work_start = int(payload.get("workStartHour", 9))
    work_end = int(payload.get("workEndHour", 17))
    work_days = payload.get("workDays", [1,2,3,4,5])

    if not customer_id:
        raise HTTPException(status_code=400, detail="Missing customerId.")

    days_str = ",".join(str(d) for d in work_days)

    with engine.begin() as conn:
        conn.execute(text("""
        INSERT INTO customer_settings(customer_id, timezone, work_start_hour, work_end_hour, work_days)
        VALUES (:cid, :tz, :ws, :we, :wd)
        ON CONFLICT (customer_id)
        DO UPDATE SET
          timezone=:tz,
          work_start_hour=:ws,
          work_end_hour=:we,
          work_days=:wd
        """), {
            "cid": customer_id,
            "tz": timezone_name,
            "ws": work_start,
            "we": work_end,
            "wd": days_str
        })

    return {"customerId": customer_id, "saved": True}
