import os
import json
import secrets
from typing import List, Optional, Dict, Any
from urllib.parse import urlencode

import requests
from fastapi import FastAPI, HTTPException
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

# Scopes (you said you added both; we request the broad one for MVP)
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/calendar",
    # "https://www.googleapis.com/auth/calendar.events",  # redundant if calendar is present
]

# -----------------------------
# Google endpoints
# -----------------------------
GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"

# Google Calendar endpoints
GOOGLE_FREEBUSY_URL = "https://www.googleapis.com/calendar/v3/freeBusy"
GOOGLE_EVENTS_INSERT_URL = "https://www.googleapis.com/calendar/v3/calendars/{calendarId}/events"

# -----------------------------
# DB setup (Postgres via Render)
# -----------------------------
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
    Render typically provides DATABASE_URL starting with:
      postgres://user:pass@host/db

    SQLAlchemy 2.x prefers a driver-qualified URL:
      postgresql+psycopg://user:pass@host/db

    This prevents SQLAlchemy from defaulting to psycopg2.
    """
    url = DATABASE_URL.strip()

    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg://", 1)
    elif url.startswith("postgresql://"):
        # If it's postgresql:// without driver, still force psycopg (v3)
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)

    return create_engine(url, pool_pre_ping=True)


def init_db():
    global engine
    require_env()
    engine = make_engine()
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS oauth_tokens (
                  provider TEXT NOT NULL,
                  user_email TEXT NOT NULL,
                  refresh_token TEXT NOT NULL,
                  scope TEXT,
                  token_type TEXT,
                  created_at TIMESTAMPTZ DEFAULT NOW(),
                  PRIMARY KEY (provider, user_email)
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS oauth_states (
                  state TEXT PRIMARY KEY,
                  created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """))
    except SQLAlchemyError as e:
        raise RuntimeError(f"DB init failed: {e}") from e


# Initialize DB at import time so the service fails fast if env/DB is wrong.
init_db()

# -----------------------------
# DB helpers
# -----------------------------
def save_state(state: str):
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO oauth_states(state) VALUES (:state) ON CONFLICT (state) DO NOTHING"),
            {"state": state},
        )


def consume_state(state: str) -> bool:
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT state FROM oauth_states WHERE state=:state"),
            {"state": state},
        ).fetchone()
        if not row:
            return False
        conn.execute(text("DELETE FROM oauth_states WHERE state=:state"), {"state": state})
        return True


def upsert_refresh_token(user_email: str, refresh_token: str, scope: str, token_type: str):
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO oauth_tokens(provider, user_email, refresh_token, scope, token_type)
                VALUES ('google', :email, :rt, :scope, :tt)
                ON CONFLICT (provider, user_email) DO UPDATE SET
                  refresh_token = EXCLUDED.refresh_token,
                  scope = EXCLUDED.scope,
                  token_type = EXCLUDED.token_type;
            """),
            {"email": user_email, "rt": refresh_token, "scope": scope, "tt": token_type},
        )


def load_refresh_token(user_email: str) -> Optional[str]:
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT refresh_token FROM oauth_tokens WHERE provider='google' AND user_email=:email"),
            {"email": user_email},
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
# Google OAuth routes
# -----------------------------
@app.get("/oauth/google/start")
def google_oauth_start():
    require_env()
    state = secrets.token_urlsafe(24)
    save_state(state)

    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URL,
        "response_type": "code",
        "scope": " ".join(GOOGLE_SCOPES),
        "access_type": "offline",
        "prompt": "consent",  # forces refresh_token the first time
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
    if not consume_state(state):
        raise HTTPException(status_code=400, detail="Invalid or expired state.")

    tokens = exchange_code_for_tokens(code)
    
    # TEMP DEBUG (remove later)
    if "access_token" not in tokens:
        return JSONResponse({"debug_tokens": tokens}, status_code=200)

    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    scope = tokens.get("scope", "")
    token_type = tokens.get("token_type", "")

    if not access_token:
        raise HTTPException(status_code=400, detail=f"No access_token returned: {tokens}")

    user_email = get_user_email(access_token)

    if not refresh_token:
        # Google sometimes doesn't return refresh_token if user already consented earlier.
        existing = load_refresh_token(user_email)
        if not existing:
            return JSONResponse(
                {
                    "connected": False,
                    "email": user_email,
                    "message": "No refresh token returned. Remove the app from Google Account permissions and try again.",
                }
            )
        refresh_token = existing

    upsert_refresh_token(user_email, refresh_token, scope, token_type)

    return {
        "connected": True,
        "email": user_email,
        "message": "Google connected. Call /google/freebusy and /google/create_event.",
    }


# -----------------------------
# API: FreeBusy + Create Event
# -----------------------------
@app.post("/google/freebusy")
async def google_freebusy(payload: Dict[str, Any]):
    """
    {
      "userEmail": "your@gmail.com",
      "timeMin": "2026-02-24T08:00:00-07:00",
      "timeMax": "2026-02-24T18:00:00-07:00",
      "timeZone": "America/Denver",
      "calendarIds": ["primary", "some_calendar_id@group.calendar.google.com"]
    }
    """
    require_env()

    user_email = payload.get("userEmail")
    time_min = payload.get("timeMin")
    time_max = payload.get("timeMax")
    tz = payload.get("timeZone", "America/Denver")
    calendar_ids: List[str] = payload.get("calendarIds", ["primary"])

    if not user_email or not time_min or not time_max:
        raise HTTPException(status_code=400, detail="Missing userEmail/timeMin/timeMax.")

    rt = load_refresh_token(user_email)
    if not rt:
        raise HTTPException(status_code=401, detail="User not connected. Visit /oauth/google/start first.")

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
      "userEmail": "your@gmail.com",
      "calendarId": "primary",
      "summary": "Maintenance visit",
      "description": "Fix sink",
      "start": {"dateTime":"2026-02-24T13:00:00-07:00","timeZone":"America/Denver"},
      "end":   {"dateTime":"2026-02-24T14:00:00-07:00","timeZone":"America/Denver"},
      "attendees": [{"email":"tenant@example.com"}]
    }
    """
    require_env()

    user_email = payload.get("userEmail")
    calendar_id = payload.get("calendarId", "primary")

    if not user_email:
        raise HTTPException(status_code=400, detail="Missing userEmail.")

    rt = load_refresh_token(user_email)
    if not rt:
        raise HTTPException(status_code=401, detail="User not connected. Visit /oauth/google/start first.")

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
