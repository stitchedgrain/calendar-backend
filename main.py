import os
import json
import secrets
from typing import Optional, Dict, Any, List
from urllib.parse import urlencode
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse, JSONResponse
from sqlalchemy import create_engine, text

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
GOOGLE_EVENTS_URL = "https://www.googleapis.com/calendar/v3/calendars/{calendarId}/events"
GOOGLE_EVENT_URL = "https://www.googleapis.com/calendar/v3/calendars/{calendarId}/events/{eventId}"

engine = None


# -----------------------------
# CORE UTIL
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
    # Render often provides postgres:// ; SQLAlchemy prefers postgresql+psycopg://
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg://", 1)
    elif url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)
    return create_engine(url, pool_pre_ping=True)


def parse_iso_to_utc(s: str) -> datetime:
    """RFC3339 with offset or Z required."""
    dt = datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise HTTPException(status_code=400, detail=f"Datetime missing timezone/offset: {s}")
    return dt.astimezone(timezone.utc)


def parse_iso_assume_tz(s: str, tz: ZoneInfo) -> datetime:
    """
    Accepts:
      - "2026-03-03T14:00:00-07:00" (has offset) -> UTC
      - "2026-03-03T21:00:00Z" (Z) -> UTC
      - "2026-03-03T14:00:00" (naive) -> assume tz, then UTC
    """
    raw = s.strip()
    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    return dt.astimezone(timezone.utc)


def iso_z(dt_utc: datetime) -> str:
    return dt_utc.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def merge_intervals(intervals: List[Dict[str, datetime]]) -> List[Dict[str, datetime]]:
    if not intervals:
        return []
    intervals = sorted(intervals, key=lambda x: x["start"])
    merged = [intervals[0]]
    for cur in intervals[1:]:
        last = merged[-1]
        if cur["start"] <= last["end"]:  # overlap or touch
            if cur["end"] > last["end"]:
                last["end"] = cur["end"]
        else:
            merged.append(cur)
    return merged


def subtract_busy_from_window(win_start: datetime, win_end: datetime, busy: List[Dict[str, datetime]]) -> List[Dict[str, datetime]]:
    free = []
    cursor = win_start
    for b in busy:
        if b["end"] <= cursor:
            continue
        if b["start"] >= win_end:
            break
        if b["start"] > cursor:
            free.append({"start": cursor, "end": min(b["start"], win_end)})
        cursor = max(cursor, b["end"])
        if cursor >= win_end:
            break
    if cursor < win_end:
        free.append({"start": cursor, "end": win_end})
    return [f for f in free if f["end"] > f["start"]]


def round_up_to_step(dt_utc: datetime, step_minutes: int) -> datetime:
    step = step_minutes * 60
    ts = int(dt_utc.timestamp())
    rounded = ((ts + step - 1) // step) * step
    return datetime.fromtimestamp(rounded, tz=timezone.utc)


def format_local(dt_utc: datetime, tz: ZoneInfo) -> str:
    return dt_utc.astimezone(tz).strftime("%a %b %d, %Y %I:%M %p %Z")


def weekday_name_to_int(name: str) -> Optional[int]:
    m = {
        "monday": 0, "mon": 0,
        "tuesday": 1, "tue": 1, "tues": 1,
        "wednesday": 2, "wed": 2,
        "thursday": 3, "thu": 3, "thur": 3, "thurs": 3,
        "friday": 4, "fri": 4,
        "saturday": 5, "sat": 5,
        "sunday": 6, "sun": 6,
    }
    if not name:
        return None
    return m.get(name.strip().lower())


# -----------------------------
# DB INIT (schema-safe)
# -----------------------------
def init_db():
    global engine
    require_env()
    engine = make_engine()
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
              access_role TEXT,
              primary_cal BOOLEAN DEFAULT FALSE,
              selected BOOLEAN DEFAULT TRUE,
              created_at TIMESTAMPTZ DEFAULT NOW(),
              PRIMARY KEY (provider, customer_id, calendar_id)
            );
        """))

        conn.execute(text("""
            ALTER TABLE customer_calendars
            ADD COLUMN IF NOT EXISTS access_role TEXT;
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS customer_settings (
              customer_id TEXT PRIMARY KEY,
              timezone TEXT NOT NULL DEFAULT 'America/Denver',
              work_start_hour INTEGER NOT NULL DEFAULT 9,
              work_end_hour INTEGER NOT NULL DEFAULT 17,
              work_days TEXT NOT NULL DEFAULT '0,1,2,3,4',
              created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """))


init_db()


# -----------------------------
# DEBUG ROUTES
# -----------------------------
@app.get("/debug/db")
def debug_db():
    try:
        with engine.begin() as conn:
            val = conn.execute(text("SELECT 1")).scalar()
        return {"db_ok": True, "select_1": val}
    except Exception as e:
        return JSONResponse({"db_ok": False, "error": repr(e)}, status_code=500)


@app.get("/debug/schema")
def debug_schema():
    try:
        with engine.begin() as conn:
            tables = conn.execute(text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='public'
                ORDER BY table_name
            """)).fetchall()

            table_names = [t[0] for t in tables]

            def cols(table: str):
                rows = conn.execute(text("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=:t
                    ORDER BY ordinal_position
                """), {"t": table}).fetchall()
                return [{"name": r[0], "type": r[1]} for r in rows]

            out = {"tables": table_names}
            for t in ["oauth_tokens", "oauth_states", "customer_calendars", "customer_settings"]:
                if t in table_names:
                    out[t] = cols(t)

            return out
    except Exception as e:
        return JSONResponse({"error": repr(e)}, status_code=500)


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


def load_refresh_token(customer_id: str) -> Optional[str]:
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
                INSERT INTO customer_calendars(provider, customer_id, calendar_id, summary, access_role, primary_cal, selected)
                VALUES ('google', :customer_id, :calendar_id, :summary, :access_role, :primary_cal, TRUE)
                ON CONFLICT (provider, customer_id, calendar_id) DO UPDATE SET
                  summary = EXCLUDED.summary,
                  access_role = EXCLUDED.access_role,
                  primary_cal = EXCLUDED.primary_cal
            """), {
                "customer_id": customer_id,
                "calendar_id": cal_id,
                "summary": cal.get("summary"),
                "access_role": cal.get("accessRole"),
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


def get_customer_settings(customer_id: str) -> Dict[str, Any]:
    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT timezone, work_start_hour, work_end_hour, work_days
            FROM customer_settings
            WHERE customer_id=:cid
        """), {"cid": customer_id}).fetchone()

    if not row:
        return {"timezone": "America/Denver", "work_start_hour": 9, "work_end_hour": 17, "work_days": [0, 1, 2, 3, 4]}

    tz_name, ws, we, days_str = row
    try:
        days = [int(x.strip()) for x in (days_str or "").split(",") if x.strip() != ""]
    except Exception:
        days = [0, 1, 2, 3, 4]

    return {
        "timezone": tz_name or "America/Denver",
        "work_start_hour": int(ws),
        "work_end_hour": int(we),
        "work_days": days if days else [0, 1, 2, 3, 4],
    }


def set_customer_settings_db(customer_id: str, tz_name: str, ws: int, we: int, days: List[int]):
    days_str = ",".join(str(int(d)) for d in days)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO customer_settings(customer_id, timezone, work_start_hour, work_end_hour, work_days)
            VALUES (:cid, :tz, :ws, :we, :wd)
            ON CONFLICT (customer_id) DO UPDATE SET
              timezone = EXCLUDED.timezone,
              work_start_hour = EXCLUDED.work_start_hour,
              work_end_hour = EXCLUDED.work_end_hour,
              work_days = EXCLUDED.work_days
        """), {"cid": customer_id, "tz": tz_name, "ws": ws, "we": we, "wd": days_str})


# -----------------------------
# GOOGLE HELPERS
# -----------------------------
def exchange_code_for_tokens(code: str) -> Dict[str, Any]:
    payload = {"code": code, "client_id": GOOGLE_CLIENT_ID, "client_secret": GOOGLE_CLIENT_SECRET, "redirect_uri": GOOGLE_REDIRECT_URL, "grant_type": "authorization_code"}
    r = requests.post(GOOGLE_TOKEN_URL, data=payload, timeout=30)
    if r.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Token exchange failed: {r.text}")
    return r.json()


def refresh_access_token(refresh_token: str) -> str:
    payload = {"client_id": GOOGLE_CLIENT_ID, "client_secret": GOOGLE_CLIENT_SECRET, "refresh_token": refresh_token, "grant_type": "refresh_token"}
    r = requests.post(GOOGLE_TOKEN_URL, data=payload, timeout=30)
    if r.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Token refresh failed: {r.text}")
    data = r.json()
    token = data.get("access_token")
    if not token:
        raise HTTPException(status_code=400, detail=f"Token refresh returned no access_token: {data}")
    return token


def get_user_email(access_token: str) -> str:
    r = requests.get(GOOGLE_USERINFO_URL, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    if r.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Failed to fetch userinfo: {r.text}")
    email = r.json().get("email")
    if not email:
        raise HTTPException(status_code=400, detail="No email returned from userinfo.")
    return email


def fetch_calendar_list(access_token: str) -> List[Dict[str, Any]]:
    r = requests.get(GOOGLE_CALENDAR_LIST_URL, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    if r.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Calendar list failed: {r.text}")
    return r.json().get("items", [])


def verify_slot_is_free(
    access_token: str,
    calendar_ids: List[str],
    start_utc: datetime,
    end_utc: datetime,
    tz_name: str
) -> Dict[str, Any]:
    """
    Never raises HTTPException. Always returns structured JSON.
    """
    body = {
        "timeMin": iso_z(start_utc),
        "timeMax": iso_z(end_utc),
        "timeZone": tz_name,
        "items": [{"id": cid} for cid in calendar_ids],
    }

    try:
        r = requests.post(
            GOOGLE_FREEBUSY_URL,
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            data=json.dumps(body),
            timeout=30,
        )
    except Exception as e:
        return {"ok": False, "slotFree": False, "busy": [], "error": f"freebusy_request_failed: {repr(e)}"}

    if r.status_code != 200:
        return {
            "ok": False,
            "slotFree": False,
            "busy": [],
            "error": "freebusy_http_error",
            "statusCode": r.status_code,
            "googleResponseText": r.text,
        }

    data = r.json()
    calendars_obj = data.get("calendars", {})

    busy_out = []
    slot_free = True

    for cal_id, cal_info in calendars_obj.items():
        cal_busy = cal_info.get("busy", []) or []
        if cal_busy:
            slot_free = False
        busy_out.append({"calendarId": cal_id, "busy": cal_busy})

    return {"ok": True, "slotFree": slot_free, "busy": busy_out, "checkedCalendars": calendar_ids}
def list_events_overlap(access_token: str, calendar_id: str, start_utc: datetime, end_utc: datetime) -> Dict[str, Any]:
    """
    Strict overlap check on ONE calendar, regardless of 'free/busy' transparency.
    Returns structured JSON and never throws.
    """
    url = GOOGLE_EVENTS_URL.format(calendarId=calendar_id)

    params = {
        "timeMin": iso_z(start_utc),
        "timeMax": iso_z(end_utc),
        "singleEvents": "true",
        "orderBy": "startTime",
        "maxResults": "50",
    }

    try:
        r = requests.get(
            url,
            headers={"Authorization": f"Bearer {access_token}"},
            params=params,
            timeout=30,
        )
    except Exception as e:
        return {"ok": False, "overlap": True, "error": f"events_list_request_failed: {repr(e)}"}

    if r.status_code != 200:
        return {"ok": False, "overlap": True, "statusCode": r.status_code, "googleResponseText": r.text}

    data = r.json()
    items = data.get("items", []) or []

    # Any event returned here overlaps the timeMin/timeMax window in some way.
    # Filter out cancelled.
    overlaps = []
    for ev in items:
        if ev.get("status") == "cancelled":
            continue
        overlaps.append({
            "id": ev.get("id"),
            "summary": ev.get("summary"),
            "start": ev.get("start"),
            "end": ev.get("end"),
            "transparency": ev.get("transparency"),  # can be "transparent"
        })

    return {"ok": True, "overlap": len(overlaps) > 0, "events": overlaps}

# -----------------------------
# BASIC ROUTES
# -----------------------------
@app.get("/")
def root():
    return {"status": "alive"}


@app.get("/health")
def health():
    return {"ok": True}


# -----------------------------
# CUSTOMER SETTINGS
# -----------------------------
@app.post("/customer/settings")
async def set_customer_settings(payload: Dict[str, Any]):
    require_env()

    customer_id = payload.get("customerId")
    if not customer_id:
        raise HTTPException(status_code=400, detail="Missing customerId.")

    tz_name = payload.get("timeZone", "America/Denver")
    ws = int(payload.get("workStartHour", 9))
    we = int(payload.get("workEndHour", 17))
    days = payload.get("workDays", [0, 1, 2, 3, 4])  # Mon-Fri

    if ws < 0 or ws > 23 or we < 1 or we > 24 or we <= ws:
        raise HTTPException(status_code=400, detail="Invalid working hours. Example: start 9 end 17.")

    if not isinstance(days, list) or not all(isinstance(d, int) and 0 <= d <= 6 for d in days):
        raise HTTPException(status_code=400, detail="workDays must be list of ints (Mon=0..Sun=6).")

    try:
        ZoneInfo(tz_name)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid timeZone: {tz_name}")

    set_customer_settings_db(customer_id, tz_name, ws, we, days)
    return {"customerId": customer_id, "saved": True}


# -----------------------------
# OAUTH
# -----------------------------
@app.get("/oauth/google/start")
def google_oauth_start(customerId: str = Query(...)):
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

    # If Google didn't return refresh_token, keep existing if present
    if not refresh_token:
        existing = load_refresh_token(customer_id)
        if not existing:
            raise HTTPException(
                status_code=400,
                detail="No refresh token returned. Remove app access in Google Account then reconnect.",
            )
        refresh_token = existing

    upsert_refresh_token(customer_id, refresh_token, scope, token_type, user_email)

    # Sync calendars immediately
    calendars = fetch_calendar_list(access_token)
    store_calendars(customer_id, calendars)

    # Ensure settings row exists
    s = get_customer_settings(customer_id)
    set_customer_settings_db(customer_id, s["timezone"], s["work_start_hour"], s["work_end_hour"], s["work_days"])

    return {
        "connected": True,
        "customerId": customer_id,
        "email": user_email,
        "message": "Google connected and calendars synced. Use /google/calendars then /google/calendars/select, then /google/availability.",
    }


# -----------------------------
# CALENDAR LIST / SYNC / SELECT
# -----------------------------
@app.get("/google/calendars")
def google_calendars(customerId: str = Query(...)):
    require_env()
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT calendar_id, summary, access_role, primary_cal, selected
            FROM customer_calendars
            WHERE provider='google' AND customer_id=:cid
            ORDER BY primary_cal DESC, summary NULLS LAST, calendar_id
        """), {"cid": customerId}).fetchall()

    return [
        {
            "calendarId": r[0],
            "summary": r[1],
            "accessRole": r[2],
            "primary": bool(r[3]),
            "selected": bool(r[4]),
        }
        for r in rows
    ]


@app.post("/google/calendars/sync")
async def google_calendars_sync(payload: Dict[str, Any]):
    require_env()
    customer_id = payload.get("customerId")
    if not customer_id:
        raise HTTPException(status_code=400, detail="Missing customerId.")

    rt = load_refresh_token(customer_id)
    if not rt:
        raise HTTPException(status_code=401, detail="Customer not connected. Run /oauth/google/start?customerId=...")

    access_token = refresh_access_token(rt)
    calendars = fetch_calendar_list(access_token)
    store_calendars(customer_id, calendars)
    return {"synced": len(calendars)}


@app.post("/google/calendars/select")
async def google_calendars_select(payload: Dict[str, Any]):
    require_env()
    customer_id = payload.get("customerId")
    calendar_ids = payload.get("calendarIds")

    if not customer_id:
        raise HTTPException(status_code=400, detail="Missing customerId.")
    if not isinstance(calendar_ids, list):
        raise HTTPException(status_code=400, detail="calendarIds must be a list of strings.")
    calendar_ids = [c.strip() for c in calendar_ids if isinstance(c, str) and c.strip()]
    if not calendar_ids:
        raise HTTPException(status_code=400, detail="calendarIds must contain at least one id.")

    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE customer_calendars
            SET selected = FALSE
            WHERE provider='google' AND customer_id=:cid
        """), {"cid": customer_id})

        for cid in calendar_ids:
            conn.execute(text("""
                UPDATE customer_calendars
                SET selected = TRUE
                WHERE provider='google' AND customer_id=:cid AND calendar_id=:calid
            """), {"cid": customer_id, "calid": cid})

    return {"customerId": customer_id, "selectedCalendarIds": calendar_ids}


# -----------------------------
# AVAILABILITY (DST SAFE)
# -----------------------------
@app.post("/google/availability")
async def google_availability(payload: Dict[str, Any]):
    require_env()
    customer_id = payload.get("customerId")
    if not customer_id:
        raise HTTPException(status_code=400, detail="Missing customerId.")

    duration_minutes = int(payload.get("durationMinutes", 60))
    if duration_minutes <= 0:
        raise HTTPException(status_code=400, detail="durationMinutes must be > 0.")

    step_minutes = int(payload.get("stepMinutes", 30))
    if step_minutes <= 0:
        raise HTTPException(status_code=400, detail="stepMinutes must be > 0.")

    rt = load_refresh_token(customer_id)
    if not rt:
        raise HTTPException(status_code=401, detail="Customer not connected. Run /oauth/google/start?customerId=...")

    settings = get_customer_settings(customer_id)

    tz_name = payload.get("timeZone", settings["timezone"])
    work_start = int(payload.get("workStartHour", settings["work_start_hour"]))
    work_end = int(payload.get("workEndHour", settings["work_end_hour"]))
    work_days = payload.get("workDays", settings["work_days"])

    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid timeZone: {tz_name}")

    if work_end <= work_start:
        raise HTTPException(status_code=400, detail="workEndHour must be > workStartHour.")

    if not isinstance(work_days, list) or not all(isinstance(d, int) and 0 <= d <= 6 for d in work_days):
        raise HTTPException(status_code=400, detail="workDays must be list of ints 0..6 (Mon=0..Sun=6).")

    access_token = refresh_access_token(rt)

    calendar_ids = payload.get("calendarIds")
    if calendar_ids is None:
        calendar_ids = []
    elif isinstance(calendar_ids, str):
        calendar_ids = [calendar_ids]
    elif not isinstance(calendar_ids, list):
        raise HTTPException(status_code=400, detail="calendarIds must be a list of strings or a single string.")

    calendar_ids = [cid.strip() for cid in calendar_ids if isinstance(cid, str) and cid.strip()]
    if not calendar_ids:
        calendar_ids = load_selected_calendar_ids(customer_id) or ["primary"]

    time_min_str = payload.get("timeMin")
    time_max_str = payload.get("timeMax")

    if time_min_str and time_max_str:
        time_min_utc = parse_iso_assume_tz(time_min_str, tz)
        time_max_utc = parse_iso_assume_tz(time_max_str, tz)
        if time_max_utc <= time_min_utc:
            raise HTTPException(status_code=400, detail="timeMax must be after timeMin.")
        start_local = time_min_utc.astimezone(tz)
        end_local = time_max_utc.astimezone(tz)
    else:
        days = int(payload.get("days", 7))
        if days <= 0 or days > 31:
            raise HTTPException(status_code=400, detail="days must be 1..31.")
        now_local = datetime.now(tz).replace(second=0, microsecond=0)
        start_local = now_local
        end_local = (start_local + timedelta(days=days)).replace(second=0, microsecond=0)
        time_min_utc = start_local.astimezone(timezone.utc)
        time_max_utc = end_local.astimezone(timezone.utc)

    google_body = {
        "timeMin": iso_z(time_min_utc),
        "timeMax": iso_z(time_max_utc),
        "timeZone": tz_name,
        "items": [{"id": cid} for cid in calendar_ids],
    }

    r = requests.post(
        GOOGLE_FREEBUSY_URL,
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        data=json.dumps(google_body),
        timeout=30,
    )
    if r.status_code != 200:
        # structured JSON instead of raw error
        return JSONResponse(
            {
                "ok": False,
                "reason": "freebusy_failed",
                "statusCode": r.status_code,
                "googleResponseText": r.text,
                "requestBody": google_body,
            },
            status_code=400,
        )

    fb = r.json()
    calendars_obj = fb.get("calendars", {})

    busy_intervals = []
    for _, info in calendars_obj.items():
        for b in info.get("busy", []):
            s = b.get("start")
            e = b.get("end")
            if not s or not e:
                continue
            s_utc = parse_iso_to_utc(s)
            e_utc = parse_iso_to_utc(e)
            if e_utc > s_utc:
                busy_intervals.append({"start": s_utc, "end": e_utc})

    busy_merged = merge_intervals(busy_intervals)

    dur = timedelta(minutes=duration_minutes)
    available = []

    cur_day = start_local.date()
    last_day = end_local.date()

    while cur_day < last_day:
        day0_local = datetime(cur_day.year, cur_day.month, cur_day.day, 0, 0, tzinfo=tz)
        weekday = day0_local.weekday()

        if weekday in work_days:
            win_start_local = day0_local.replace(hour=work_start, minute=0)
            win_end_local = day0_local.replace(hour=work_end, minute=0)

            if win_end_local > start_local and win_start_local < end_local:
                win_start_local = max(win_start_local, start_local)
                win_end_local = min(win_end_local, end_local)

                win_start_utc = win_start_local.astimezone(timezone.utc)
                win_end_utc = win_end_local.astimezone(timezone.utc)

                free_intervals = subtract_busy_from_window(win_start_utc, win_end_utc, busy_merged)

                for fi in free_intervals:
                    s = round_up_to_step(fi["start"], step_minutes)
                    while s + dur <= fi["end"]:
                        e = s + dur
                        loc = s.astimezone(tz)
                        available.append({
                            "startUtc": iso_z(s),
                            "endUtc": iso_z(e),
                            "startLocal": format_local(s, tz),
                            "endLocal": format_local(e, tz),
                            "weekdayLocal": loc.weekday(),
                            "hourLocal": loc.hour,
                        })
                        s = s + timedelta(minutes=step_minutes)

        cur_day = (day0_local + timedelta(days=1)).date()

    pref = payload.get("preference") or {}
    max_results = int(pref.get("maxResults", 3)) or 3
    if max_results < 1:
        max_results = 3

    time_of_day = (pref.get("timeOfDay") or "").strip().lower()

    def tod_ok(slot: Dict[str, Any]) -> bool:
        if not time_of_day:
            return True
        h = int(slot["hourLocal"])
        if time_of_day == "morning":
            return h < 12
        if time_of_day == "afternoon":
            return h >= 12
        return True

    filtered = [s for s in available if tod_ok(s)]

    pref_type = (pref.get("type") or "").strip().lower()
    suggestions: List[Dict[str, Any]] = []

    if pref_type == "weekday":
        wd = weekday_name_to_int(pref.get("weekday") or "")
        if wd is not None:
            filtered = [s for s in filtered if int(s["weekdayLocal"]) == wd]
        suggestions = filtered[:max_results]

    elif pref_type == "datetime":
        pref_start = pref.get("preferredStart")
        if not pref_start:
            suggestions = filtered[:max_results]
        else:
            preferred_utc = parse_iso_assume_tz(pref_start, tz)

            def dist(slot: Dict[str, Any]) -> int:
                s_utc = parse_iso_to_utc(slot["startUtc"])
                return abs(int((s_utc - preferred_utc).total_seconds()))

            suggestions = sorted(filtered, key=dist)[:max_results]
    else:
        suggestions = filtered[:max_results]

    def strip(slot: Dict[str, Any]) -> Dict[str, Any]:
        return {"startUtc": slot["startUtc"], "endUtc": slot["endUtc"], "startLocal": slot["startLocal"], "endLocal": slot["endLocal"]}

    return {
        "ok": True,
        "customerId": customer_id,
        "timeZone": tz_name,
        "window": {"timeMinUtc": iso_z(time_min_utc), "timeMaxUtc": iso_z(time_max_utc)},
        "workHours": {"startHour": work_start, "endHour": work_end, "days": work_days},
        "calendarIdsUsed": calendar_ids,
        "availableCount": len(available),
        "suggestions": [strip(s) for s in suggestions],
        "available": [strip(s) for s in available[:500]],
    }


# -----------------------------
# CREATE / CANCEL / RESCHEDULE EVENT
# -----------------------------
@app.post("/google/create_event")
async def google_create_event(payload: Dict[str, Any]):
    require_env()

    customer_id = payload.get("customerId")
    if not customer_id:
        return JSONResponse({"booked": False, "reason": "missing_customerId"}, status_code=400)

    rt = load_refresh_token(customer_id)
    if not rt:
        return JSONResponse({"booked": False, "reason": "not_connected"}, status_code=401)

    access_token = refresh_access_token(rt)

    calendar_id = payload.get("calendarId", "primary")
    url = GOOGLE_EVENTS_URL.format(calendarId=calendar_id)

    start_obj = payload.get("start")
    end_obj = payload.get("end")
    if not start_obj or not end_obj or not start_obj.get("dateTime") or not end_obj.get("dateTime"):
        return JSONResponse({"booked": False, "reason": "missing_start_end"}, status_code=400)

# -----------------------------
# PREVENT DOUBLE BOOKING (STRICT)
# -----------------------------
settings = get_customer_settings(customer_id)
tz_name = settings["timezone"]
tz = ZoneInfo(tz_name)

start_utc = parse_iso_assume_tz(start_obj["dateTime"], tz)
end_utc = parse_iso_assume_tz(end_obj["dateTime"], tz)

# Union of calendars: selected + the one we are writing to
selected = load_selected_calendar_ids(customer_id) or []
calendars_to_check = sorted(set(selected + [calendar_id] + (["primary"] if calendar_id != "primary" else [])))

# 1) Multi-calendar freebusy check
fb_check = verify_slot_is_free(access_token, calendars_to_check, start_utc, end_utc, tz_name)
if not fb_check.get("ok"):
    return JSONResponse(
        {"booked": False, "reason": "freebusy_check_failed", "debug": fb_check},
        status_code=503,
    )
if not fb_check.get("slotFree"):
    return JSONResponse(
        {"booked": False, "reason": "slot_taken", "source": "freebusy", "busy": fb_check.get("busy", [])},
        status_code=409,
    )

# 2) Strict check on the calendar we will INSERT INTO
ev_check = list_events_overlap(access_token, calendar_id, start_utc, end_utc)
if not ev_check.get("ok"):
    return JSONResponse(
        {"booked": False, "reason": "events_list_failed", "debug": ev_check},
        status_code=503,
    )
if ev_check.get("overlap"):
    return JSONResponse(
        {"booked": False, "reason": "slot_taken", "source": "events_list", "overlappingEvents": ev_check.get("events", [])},
        status_code=409,
    )


@app.post("/google/cancel_event")
async def google_cancel_event(payload: Dict[str, Any]):
    require_env()
    customer_id = payload.get("customerId")
    calendar_id = payload.get("calendarId", "primary")
    event_id = payload.get("eventId")

    if not customer_id or not event_id:
        return JSONResponse({"cancelled": False, "reason": "missing_fields"}, status_code=400)

    rt = load_refresh_token(customer_id)
    if not rt:
        return JSONResponse({"cancelled": False, "reason": "not_connected"}, status_code=401)

    access_token = refresh_access_token(rt)

    url = GOOGLE_EVENT_URL.format(calendarId=calendar_id, eventId=event_id)
    r = requests.delete(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    if r.status_code not in (200, 204):
        return JSONResponse({"cancelled": False, "reason": "cancel_failed", "statusCode": r.status_code, "googleResponseText": r.text}, status_code=400)

    return {"cancelled": True, "calendarId": calendar_id, "eventId": event_id}


@app.post("/google/reschedule_event")
async def google_reschedule_event(payload: Dict[str, Any]):
    require_env()
    customer_id = payload.get("customerId")
    calendar_id = payload.get("calendarId", "primary")
    event_id = payload.get("eventId")
    start_obj = payload.get("start")
    end_obj = payload.get("end")

    if not customer_id or not event_id or not start_obj or not end_obj:
        return JSONResponse({"rescheduled": False, "reason": "missing_fields"}, status_code=400)

    rt = load_refresh_token(customer_id)
    if not rt:
        return JSONResponse({"rescheduled": False, "reason": "not_connected"}, status_code=401)

    access_token = refresh_access_token(rt)

    url = GOOGLE_EVENT_URL.format(calendarId=calendar_id, eventId=event_id)
    patch_body = {"start": start_obj, "end": end_obj}

    r = requests.patch(
        url,
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        data=json.dumps(patch_body),
        timeout=30,
    )
    if r.status_code not in (200, 201):
        return JSONResponse({"rescheduled": False, "reason": "reschedule_failed", "statusCode": r.status_code, "googleResponseText": r.text}, status_code=400)

    return {"rescheduled": True, "event": r.json()}
