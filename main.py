import os
import json
import secrets
import re
from typing import Optional, Dict, Any, List
from urllib.parse import urlencode, quote
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse, JSONResponse
from sqlalchemy import create_engine, text

app = FastAPI()

# =============================
# ENV
# =============================
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


# =============================
# UTIL
# =============================
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


def parse_iso_to_utc(s: str) -> datetime:
    dt = datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise HTTPException(status_code=400, detail=f"Datetime missing timezone/offset: {s}")
    return dt.astimezone(timezone.utc)


_ISO_FIX = re.compile(r"^(?P<y>\d{4})-(?P<m>\d{1,2})-(?P<d>\d{1,2})T(?P<rest>.+)$")


def parse_iso_assume_tz(s: str, tz: ZoneInfo) -> datetime:
    """
    Accepts:
      - 2026-2-2T14:00:00-07:00   (normalized)
      - 2026-02-02T14:00:00-07:00
      - 2026-02-02T21:00:00Z
      - 2026-02-02T14:00:00       (naive -> assume tz)
    Returns UTC datetime.
    """
    raw = (s or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="Empty datetime string.")

    m = _ISO_FIX.match(raw)
    if m:
        y = m.group("y")
        mo = int(m.group("m"))
        d = int(m.group("d"))
        rest = m.group("rest")
        raw = f"{y}-{mo:02d}-{d:02d}T{rest}"

    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    return dt.astimezone(timezone.utc)


def iso_z(dt_utc: datetime) -> str:
    return dt_utc.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def safe_cal_id(cid: str) -> str:
    return quote(cid, safe="")


def merge_intervals(intervals: List[Dict[str, datetime]]) -> List[Dict[str, datetime]]:
    if not intervals:
        return []
    intervals = sorted(intervals, key=lambda x: x["start"])
    merged = [intervals[0]]
    for cur in intervals[1:]:
        last = merged[-1]
        if cur["start"] <= last["end"]:
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


def pick_three_suggestions(
    free_slots: List[Dict[str, str]],
    preferred_utc: Optional[datetime],
) -> List[Dict[str, str]]:
    if not free_slots:
        return []
    if not preferred_utc:
        return free_slots[:3]

    def _dist(slot):
        s = datetime.fromisoformat(slot["startUtc"].replace("Z", "+00:00"))
        return abs((s - preferred_utc).total_seconds())

    return sorted(free_slots, key=_dist)[:3]


# =============================
# DB INIT
# =============================
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


# =============================
# DEBUG ROUTES
# =============================
@app.get("/")
def root():
    return {"status": "alive"}


@app.get("/health")
def health():
    return {"ok": True}


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


# =============================
# DB HELPERS
# =============================
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
        row = conn.execute(text("SELECT customer_id FROM oauth_states WHERE state=:state"), {"state": state}).fetchone()
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
            {"customer_id": customer_id, "user_email": user_email, "rt": refresh_token, "scope": scope, "tt": token_type},
        )


def load_refresh_token(customer_id: str) -> Optional[str]:
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT refresh_token FROM oauth_tokens WHERE provider='google' AND customer_id=:customer_id"),
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
        days = [int(x.strip()) for x in (days_str or "").split(",") if x.strip()]
    except Exception:
        days = [0, 1, 2, 3, 4]
    return {"timezone": tz_name or "America/Denver", "work_start_hour": int(ws), "work_end_hour": int(we), "work_days": days or [0, 1, 2, 3, 4]}


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


# =============================
# GOOGLE HELPERS
# =============================
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


def freebusy_raw(access_token: str, calendar_ids: List[str], time_min_utc: datetime, time_max_utc: datetime, tz_name: str) -> Dict[str, Any]:
    body = {
        "timeMin": iso_z(time_min_utc),
        "timeMax": iso_z(time_max_utc),
        "timeZone": tz_name,
        "items": [{"id": cid} for cid in calendar_ids],
    }
    r = requests.post(
        GOOGLE_FREEBUSY_URL,
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        data=json.dumps(body),
        timeout=30,
    )
    return {"statusCode": r.status_code, "text": r.text, "json": (r.json() if r.headers.get("content-type", "").startswith("application/json") else None), "requestBody": body}


def verify_slot_is_free(access_token: str, calendar_ids: List[str], start_utc: datetime, end_utc: datetime, tz_name: str) -> Dict[str, Any]:
    resp = freebusy_raw(access_token, calendar_ids, start_utc, end_utc, tz_name)
    if resp["statusCode"] != 200 or not resp["json"]:
        return {"ok": False, "slotFree": False, "busy": [], "error": "freebusy_http_error", "statusCode": resp["statusCode"], "googleResponseText": resp["text"], "requestBody": resp["requestBody"]}

    data = resp["json"]
    calendars_obj = data.get("calendars", {})
    busy_out, slot_free = [], True
    for cal_id, cal_info in calendars_obj.items():
        cal_busy = cal_info.get("busy", []) or []
        if cal_busy:
            slot_free = False
        busy_out.append({"calendarId": cal_id, "busy": cal_busy})
    return {"ok": True, "slotFree": slot_free, "busy": busy_out, "checkedCalendars": calendar_ids}


def list_events_overlap(access_token: str, calendar_id: str, start_utc: datetime, end_utc: datetime) -> Dict[str, Any]:
    url = GOOGLE_EVENTS_URL.format(calendarId=safe_cal_id(calendar_id))
    params = {"timeMin": iso_z(start_utc), "timeMax": iso_z(end_utc), "singleEvents": "true", "orderBy": "startTime", "maxResults": "50"}
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, params=params, timeout=30)
    if r.status_code != 200:
        return {"ok": False, "overlap": True, "statusCode": r.status_code, "googleResponseText": r.text}

    data = r.json()
    items = data.get("items", []) or []
    overlaps = []
    for ev in items:
        if ev.get("status") == "cancelled":
            continue
        overlaps.append({"id": ev.get("id"), "summary": ev.get("summary"), "start": ev.get("start"), "end": ev.get("end"), "transparency": ev.get("transparency")})
    return {"ok": True, "overlap": len(overlaps) > 0, "events": overlaps}


def compute_availability(
    access_token: str,
    tz_name: str,
    work_start_hour: int,
    work_end_hour: int,
    work_days: List[int],
    calendar_ids: List[str],
    duration_minutes: int,
    step_minutes: int,
    days: int,
    preferred_utc: Optional[datetime] = None,
) -> Dict[str, Any]:
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz).replace(second=0, microsecond=0)
    start_local = now_local
    end_local = (start_local + timedelta(days=days)).replace(second=0, microsecond=0)

    time_min_utc = start_local.astimezone(timezone.utc)
    time_max_utc = end_local.astimezone(timezone.utc)

    fb = freebusy_raw(access_token, calendar_ids, time_min_utc, time_max_utc, tz_name)
    if fb["statusCode"] != 200 or not fb["json"]:
        return {"ok": False, "reason": "freebusy_failed", "statusCode": fb["statusCode"], "googleResponseText": fb["text"], "requestBody": fb["requestBody"]}

    calendars_obj = fb["json"].get("calendars", {})
    busy_intervals: List[Dict[str, datetime]] = []

    for _, info in calendars_obj.items():
        for b in info.get("busy", []):
            s, e = b.get("start"), b.get("end")
            if not s or not e:
                continue
            s_utc = parse_iso_to_utc(s)
            e_utc = parse_iso_to_utc(e)
            if e_utc > s_utc:
                busy_intervals.append({"start": s_utc, "end": e_utc})

    busy_merged = merge_intervals(busy_intervals)
    dur = timedelta(minutes=duration_minutes)
    available: List[Dict[str, str]] = []

    cur_day = start_local.date()
    last_day = end_local.date()

    while cur_day < last_day:
        day0_local = datetime(cur_day.year, cur_day.month, cur_day.day, 0, 0, tzinfo=tz)
        weekday = day0_local.weekday()

        if weekday in work_days:
            win_start_local = day0_local.replace(hour=work_start_hour, minute=0)
            win_end_local = day0_local.replace(hour=work_end_hour, minute=0)

            free_intervals = subtract_busy_from_window(
                win_start_local.astimezone(timezone.utc),
                win_end_local.astimezone(timezone.utc),
                busy_merged,
            )

            for fi in free_intervals:
                s = round_up_to_step(fi["start"], step_minutes)
                while s + dur <= fi["end"]:
                    e = s + dur
                    available.append({
                        "startUtc": iso_z(s),
                        "endUtc": iso_z(e),
                        "startLocal": format_local(s, tz),
                        "endLocal": format_local(e, tz),
                    })
                    s = s + timedelta(minutes=step_minutes)

        cur_day = (day0_local + timedelta(days=1)).date()

    suggestions = pick_three_suggestions(available, preferred_utc)
    return {
        "ok": True,
        "timeZone": tz_name,
        "calendarIdsUsed": calendar_ids,
        "availableCount": len(available),
        "suggestions": suggestions,
        "available": available[:500],
    }


# =============================
# SETTINGS
# =============================
@app.post("/customer/settings")
async def set_customer_settings(payload: Dict[str, Any]):
    require_env()
    customer_id = payload.get("customerId")
    if not customer_id:
        raise HTTPException(status_code=400, detail="Missing customerId.")
    tz_name = payload.get("timeZone", "America/Denver")
    ws = int(payload.get("workStartHour", 9))
    we = int(payload.get("workEndHour", 17))
    days = payload.get("workDays", [0, 1, 2, 3, 4])
    ZoneInfo(tz_name)
    set_customer_settings_db(customer_id, tz_name, ws, we, days)
    return {"customerId": customer_id, "saved": True}


# =============================
# OAUTH
# =============================
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
    if not refresh_token:
        existing = load_refresh_token(customer_id)
        if not existing:
            raise HTTPException(status_code=400, detail="No refresh token returned. Remove app access then reconnect.")
        refresh_token = existing

    upsert_refresh_token(customer_id, refresh_token, scope, token_type, user_email)
    calendars = fetch_calendar_list(access_token)
    store_calendars(customer_id, calendars)

    s = get_customer_settings(customer_id)
    set_customer_settings_db(customer_id, s["timezone"], s["work_start_hour"], s["work_end_hour"], s["work_days"])

    return {"connected": True, "customerId": customer_id, "email": user_email, "message": "Google connected and calendars synced."}


# =============================
# CALENDARS
# =============================
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
    return [{"calendarId": r[0], "summary": r[1], "accessRole": r[2], "primary": bool(r[3]), "selected": bool(r[4])} for r in rows]


@app.post("/google/calendars/sync")
async def google_calendars_sync(payload: Dict[str, Any]):
    require_env()
    customer_id = payload.get("customerId")
    if not customer_id:
        raise HTTPException(status_code=400, detail="Missing customerId.")
    rt = load_refresh_token(customer_id)
    if not rt:
        raise HTTPException(status_code=401, detail="Customer not connected.")
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
    calendar_ids = sorted(set(calendar_ids))

    with engine.begin() as conn:
        conn.execute(text("UPDATE customer_calendars SET selected=FALSE WHERE provider='google' AND customer_id=:cid"), {"cid": customer_id})
        for cid in calendar_ids:
            conn.execute(text("""
                UPDATE customer_calendars
                SET selected=TRUE
                WHERE provider='google' AND customer_id=:cid AND calendar_id=:calid
            """), {"cid": customer_id, "calid": cid})

    return {"customerId": customer_id, "selectedCalendarIds": calendar_ids}


# =============================
# AVAILABILITY
# =============================
@app.post("/google/availability")
async def google_availability(payload: Dict[str, Any]):
    require_env()
    customer_id = payload.get("customerId")
    if not customer_id:
        raise HTTPException(status_code=400, detail="Missing customerId.")

    duration_minutes = int(payload.get("durationMinutes", 60))
    step_minutes = int(payload.get("stepMinutes", 30))
    days = int(payload.get("days", 7))
    if duration_minutes <= 0 or step_minutes <= 0 or days <= 0 or days > 31:
        raise HTTPException(status_code=400, detail="Invalid durationMinutes/stepMinutes/days.")

    rt = load_refresh_token(customer_id)
    if not rt:
        raise HTTPException(status_code=401, detail="Customer not connected.")

    settings = get_customer_settings(customer_id)
    tz_name = payload.get("timeZone", settings["timezone"])
    work_start = int(payload.get("workStartHour", settings["work_start_hour"]))
    work_end = int(payload.get("workEndHour", settings["work_end_hour"]))
    work_days = payload.get("workDays", settings["work_days"])

    calendar_ids = payload.get("calendarIds") or []
    if isinstance(calendar_ids, str):
        calendar_ids = [calendar_ids]
    calendar_ids = [cid.strip() for cid in calendar_ids if isinstance(cid, str) and cid.strip()]
    if not calendar_ids:
        calendar_ids = load_selected_calendar_ids(customer_id) or ["primary"]

    preferred = payload.get("preferredDateTimeUtc")  # optional: "2026-02-02T21:00:00Z"
    preferred_utc = parse_iso_to_utc(preferred) if isinstance(preferred, str) and preferred.strip() else None

    access_token = refresh_access_token(rt)

    out = compute_availability(
        access_token=access_token,
        tz_name=tz_name,
        work_start_hour=work_start,
        work_end_hour=work_end,
        work_days=work_days,
        calendar_ids=calendar_ids,
        duration_minutes=duration_minutes,
        step_minutes=step_minutes,
        days=days,
        preferred_utc=preferred_utc,
    )

    if not out.get("ok"):
        return JSONResponse(out, status_code=200)  # do not fail Make

    out["customerId"] = customer_id
    return out


# =============================
# FIND EVENTS (for cancel/reschedule lookup)
# =============================
@app.post("/google/find_events")
async def google_find_events(payload: Dict[str, Any]):
    """
    Find events to cancel/reschedule.
    Body:
    {
      "customerId": "pm_1",
      "calendarId": "primary",           (optional, default primary)
      "timeMin": "2026-03-01T00:00:00Z",
      "timeMax": "2026-03-10T00:00:00Z",
      "query": "Maintenance"             (optional)
    }
    """
    require_env()
    customer_id = payload.get("customerId")
    if not customer_id:
        return JSONResponse({"ok": False, "reason": "missing_customerId"}, status_code=200)

    rt = load_refresh_token(customer_id)
    if not rt:
        return JSONResponse({"ok": False, "reason": "not_connected"}, status_code=200)

    calendar_id = payload.get("calendarId", "primary")
    time_min = payload.get("timeMin")
    time_max = payload.get("timeMax")
    if not time_min or not time_max:
        return JSONResponse({"ok": False, "reason": "missing_timeMin_timeMax"}, status_code=200)

    q = payload.get("query", None)
    max_results = int(payload.get("maxResults", 50))

    access_token = refresh_access_token(rt)
    url = GOOGLE_EVENTS_URL.format(calendarId=safe_cal_id(calendar_id))
    params = {
        "timeMin": time_min,
        "timeMax": time_max,
        "singleEvents": "true",
        "orderBy": "startTime",
        "maxResults": str(max_results),
    }
    if isinstance(q, str) and q.strip():
        params["q"] = q.strip()

    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, params=params, timeout=30)
    if r.status_code != 200:
        return JSONResponse({"ok": False, "reason": "list_failed", "statusCode": r.status_code, "googleResponseText": r.text}, status_code=200)

    items = (r.json() or {}).get("items", []) or []
    # keep only useful fields
    slim = []
    for ev in items:
        if ev.get("status") == "cancelled":
            continue
        slim.append({
            "eventId": ev.get("id"),
            "summary": ev.get("summary"),
            "start": ev.get("start"),
            "end": ev.get("end"),
            "htmlLink": ev.get("htmlLink"),
        })

    return {"ok": True, "calendarId": calendar_id, "count": len(slim), "events": slim}


# =============================
# CREATE EVENT (returns 200 even if taken + provides suggestions)
# =============================
@app.post("/google/create_event")
async def google_create_event(payload: Dict[str, Any]):
    try:
        require_env()

        customer_id = payload.get("customerId")
        if not customer_id:
            return JSONResponse({"booked": False, "reason": "missing_customerId", "message": "Missing customerId."}, status_code=200)

        rt = load_refresh_token(customer_id)
        if not rt:
            return JSONResponse({"booked": False, "reason": "not_connected", "message": "Calendar is not connected for this customer."}, status_code=200)

        access_token = refresh_access_token(rt)

        calendar_id = payload.get("calendarId", "primary")
        url = GOOGLE_EVENTS_URL.format(calendarId=safe_cal_id(calendar_id))

        start_obj = payload.get("start")
        end_obj = payload.get("end")
        if not start_obj or not end_obj or not start_obj.get("dateTime") or not end_obj.get("dateTime"):
            return JSONResponse({"booked": False, "reason": "missing_start_end", "message": "Missing start/end dateTime."}, status_code=200)

        settings = get_customer_settings(customer_id)
        tz_name = payload.get("timeZone", settings["timezone"])
        tz = ZoneInfo(tz_name)

        start_utc = parse_iso_assume_tz(start_obj["dateTime"], tz)
        end_utc = parse_iso_assume_tz(end_obj["dateTime"], tz)

        selected = load_selected_calendar_ids(customer_id) or []
        calendars_to_check = sorted(set(selected + [calendar_id] + (["primary"] if calendar_id != "primary" else [])))
        if not calendars_to_check:
            calendars_to_check = ["primary"]

        # 1) Multi-calendar freebusy conflict check
        fb_check = verify_slot_is_free(access_token, calendars_to_check, start_utc, end_utc, tz_name)
        if not fb_check.get("ok"):
            return JSONResponse({"booked": False, "reason": "freebusy_check_failed", "message": "Could not verify availability right now.", "debug": fb_check}, status_code=200)

        if not fb_check.get("slotFree"):
            # return 200 + suggestions
            suggest = compute_availability(
                access_token=access_token,
                tz_name=tz_name,
                work_start_hour=int(payload.get("workStartHour", settings["work_start_hour"])),
                work_end_hour=int(payload.get("workEndHour", settings["work_end_hour"])),
                work_days=payload.get("workDays", settings["work_days"]),
                calendar_ids=calendars_to_check,
                duration_minutes=int(payload.get("durationMinutes", 60)),
                step_minutes=int(payload.get("stepMinutes", 30)),
                days=int(payload.get("days", 7)),
                preferred_utc=start_utc,
            )
            suggestions = suggest.get("suggestions", []) if suggest.get("ok") else []

            return JSONResponse(
                {
                    "booked": False,
                    "reason": "slot_taken",
                    "message": "That time is already booked. Please choose a different time.",
                    "source": "freebusy",
                    "busy": fb_check.get("busy", []),
                    "checkedCalendars": fb_check.get("checkedCalendars", []),
                    "suggestions": suggestions,
                },
                status_code=200,  # IMPORTANT: do not fail Make
            )

        # 2) Target calendar events list overlap check (extra safety)
        ev_check = list_events_overlap(access_token, calendar_id, start_utc, end_utc)
        if not ev_check.get("ok"):
            return JSONResponse({"booked": False, "reason": "events_list_failed", "message": "Could not verify overlaps right now.", "debug": ev_check}, status_code=200)

        if ev_check.get("overlap"):
            suggest = compute_availability(
                access_token=access_token,
                tz_name=tz_name,
                work_start_hour=int(payload.get("workStartHour", settings["work_start_hour"])),
                work_end_hour=int(payload.get("workEndHour", settings["work_end_hour"])),
                work_days=payload.get("workDays", settings["work_days"]),
                calendar_ids=calendars_to_check,
                duration_minutes=int(payload.get("durationMinutes", 60)),
                step_minutes=int(payload.get("stepMinutes", 30)),
                days=int(payload.get("days", 7)),
                preferred_utc=start_utc,
            )
            suggestions = suggest.get("suggestions", []) if suggest.get("ok") else []

            return JSONResponse(
                {
                    "booked": False,
                    "reason": "slot_taken",
                    "message": "That time is already booked. Please choose a different time.",
                    "source": "events_list",
                    "overlappingEvents": ev_check.get("events", []),
                    "suggestions": suggestions,
                },
                status_code=200,
            )

        # 3) Create event
        event_body = {
            "summary": payload.get("summary", "Booking"),
            "description": payload.get("description", ""),
            "start": start_obj,
            "end": end_obj,
        }
        attendees = payload.get("attendees")
        if attendees:
            event_body["attendees"] = attendees

        # Optional: create as tentative first (hold)
        # If you want that behavior, pass: "tentative": true
        if payload.get("tentative") is True:
            event_body["status"] = "tentative"

        r = requests.post(
            url,
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            data=json.dumps(event_body),
            timeout=30,
        )

        if r.status_code not in (200, 201):
            return JSONResponse(
                {"booked": False, "reason": "create_event_failed", "message": "Could not create the event.", "statusCode": r.status_code, "googleResponseText": r.text},
                status_code=200,
            )

        return {"booked": True, "message": "Appointment booked successfully.", "event": r.json()}

    except Exception as e:
        import traceback
        return JSONResponse({"booked": False, "reason": "internal_exception", "message": "Unexpected server error.", "error": repr(e), "traceback": traceback.format_exc()}, status_code=200)


# =============================
# CANCEL EVENT
# =============================
@app.post("/google/cancel_event")
async def google_cancel_event(payload: Dict[str, Any]):
    try:
        require_env()
        customer_id = payload.get("customerId")
        calendar_id = payload.get("calendarId", "primary")
        event_id = payload.get("eventId")

        if not customer_id or not event_id:
            return JSONResponse({"cancelled": False, "reason": "missing_fields", "message": "Missing customerId or eventId."}, status_code=200)

        rt = load_refresh_token(customer_id)
        if not rt:
            return JSONResponse({"cancelled": False, "reason": "not_connected", "message": "Calendar is not connected."}, status_code=200)

        access_token = refresh_access_token(rt)
        url = GOOGLE_EVENT_URL.format(calendarId=safe_cal_id(calendar_id), eventId=quote(event_id, safe=""))

        r = requests.delete(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
        if r.status_code not in (200, 204):
            return JSONResponse({"cancelled": False, "reason": "cancel_failed", "message": "Could not cancel the event.", "statusCode": r.status_code, "googleResponseText": r.text}, status_code=200)

        return {"cancelled": True, "message": "Appointment cancelled.", "calendarId": calendar_id, "eventId": event_id}

    except Exception as e:
        import traceback
        return JSONResponse({"cancelled": False, "reason": "internal_exception", "message": "Unexpected server error.", "error": repr(e), "traceback": traceback.format_exc()}, status_code=200)


# =============================
# RESCHEDULE EVENT (PATCH START/END)
# =============================
@app.post("/google/reschedule_event")
async def google_reschedule_event(payload: Dict[str, Any]):
    try:
        require_env()
        customer_id = payload.get("customerId")
        calendar_id = payload.get("calendarId", "primary")
        event_id = payload.get("eventId")
        start_obj = payload.get("start")
        end_obj = payload.get("end")

        if not customer_id or not event_id or not start_obj or not end_obj:
            return JSONResponse({"rescheduled": False, "reason": "missing_fields", "message": "Missing required fields."}, status_code=200)

        rt = load_refresh_token(customer_id)
        if not rt:
            return JSONResponse({"rescheduled": False, "reason": "not_connected", "message": "Calendar is not connected."}, status_code=200)

        settings = get_customer_settings(customer_id)
        tz_name = payload.get("timeZone", settings["timezone"])
        tz = ZoneInfo(tz_name)

        access_token = refresh_access_token(rt)

        # Verify new time is free (same logic as create)
        start_utc = parse_iso_assume_tz(start_obj["dateTime"], tz)
        end_utc = parse_iso_assume_tz(end_obj["dateTime"], tz)

        selected = load_selected_calendar_ids(customer_id) or []
        calendars_to_check = sorted(set(selected + [calendar_id] + (["primary"] if calendar_id != "primary" else [])))
        if not calendars_to_check:
            calendars_to_check = ["primary"]

        fb_check = verify_slot_is_free(access_token, calendars_to_check, start_utc, end_utc, tz_name)
        if not fb_check.get("ok"):
            return JSONResponse({"rescheduled": False, "reason": "freebusy_check_failed", "message": "Could not verify availability.", "debug": fb_check}, status_code=200)
        if not fb_check.get("slotFree"):
            suggest = compute_availability(
                access_token=access_token,
                tz_name=tz_name,
                work_start_hour=int(payload.get("workStartHour", settings["work_start_hour"])),
                work_end_hour=int(payload.get("workEndHour", settings["work_end_hour"])),
                work_days=payload.get("workDays", settings["work_days"]),
                calendar_ids=calendars_to_check,
                duration_minutes=int(payload.get("durationMinutes", 60)),
                step_minutes=int(payload.get("stepMinutes", 30)),
                days=int(payload.get("days", 7)),
                preferred_utc=start_utc,
            )
            suggestions = suggest.get("suggestions", []) if suggest.get("ok") else []
            return JSONResponse({"rescheduled": False, "reason": "slot_taken", "message": "That new time is already booked.", "busy": fb_check.get("busy", []), "suggestions": suggestions}, status_code=200)

        url = GOOGLE_EVENT_URL.format(calendarId=safe_cal_id(calendar_id), eventId=quote(event_id, safe=""))
        patch_body = {"start": start_obj, "end": end_obj}

        r = requests.patch(
            url,
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            data=json.dumps(patch_body),
            timeout=30,
        )

        if r.status_code not in (200, 201):
            return JSONResponse({"rescheduled": False, "reason": "reschedule_failed", "message": "Could not reschedule.", "statusCode": r.status_code, "googleResponseText": r.text}, status_code=200)

        return {"rescheduled": True, "message": "Appointment rescheduled.", "event": r.json()}

    except Exception as e:
        import traceback
        return JSONResponse({"rescheduled": False, "reason": "internal_exception", "message": "Unexpected server error.", "error": repr(e), "traceback": traceback.format_exc()}, status_code=200)
