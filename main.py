from __future__ import annotations

import os
import json
import secrets
import urllib.parse
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# -------------------------
# App
# -------------------------
app = FastAPI(title="Calendar Backend", version="1.0.0")

# -------------------------
# Google Config (env)
# -------------------------
GOOGLE_CLIENT_ID = (os.environ.get("GOOGLE_CLIENT_ID") or "").strip()
GOOGLE_CLIENT_SECRET = (os.environ.get("GOOGLE_CLIENT_SECRET") or "").strip()
GOOGLE_REDIRECT_URI = (os.environ.get("GOOGLE_REDIRECT_URI") or "").strip()
APP_BASE_URL = (os.environ.get("APP_BASE_URL") or "").strip()
DEBUG_API_KEY = (os.environ.get("DEBUG_API_KEY") or "").strip()

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"
GOOGLE_CALENDAR_LIST_URL = "https://www.googleapis.com/calendar/v3/users/me/calendarList"
GOOGLE_FREEBUSY_URL = "https://www.googleapis.com/calendar/v3/freeBusy"
GOOGLE_EVENTS_URL = "https://www.googleapis.com/calendar/v3/calendars/{calendarId}/events"

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/calendar",
    "openid",
    "email",
    "profile",
]


# -------------------------
# DB
# -------------------------
def make_engine() -> Engine:
    """
    Robust DATABASE_URL parsing:
    - strips whitespace/newlines
    - removes accidental quotes
    - normalizes scheme to SQLAlchemy + psycopg v3 driver
    """
    raw = (os.environ.get("DATABASE_URL") or "").strip()

    if not raw:
        raise RuntimeError("DATABASE_URL env var is missing/empty in Render")

    # remove wrapping quotes if user pasted them
    if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
        raw = raw[1:-1].strip()

    # normalize common postgres URL schemes
    if raw.startswith("postgres://"):
        raw = raw.replace("postgres://", "postgresql+psycopg://", 1)
    elif raw.startswith("postgresql://"):
        raw = raw.replace("postgresql://", "postgresql+psycopg://", 1)
    elif raw.startswith("postgresql+psycopg2://"):
        raw = raw.replace("postgresql+psycopg2://", "postgresql+psycopg://", 1)

    # minimal sanity check (don’t leak creds)
    if "://" not in raw:
        raise RuntimeError(f"DATABASE_URL malformed (prefix={raw[:20]!r})")

    return create_engine(raw, pool_pre_ping=True, future=True)


engine = make_engine()


def init_db() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS oauth_states (
      state       TEXT PRIMARY KEY,
      customer_id TEXT NOT NULL,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS oauth_tokens (
      provider      TEXT NOT NULL,
      customer_id   TEXT NOT NULL,
      user_email    TEXT NOT NULL,
      refresh_token TEXT NOT NULL,
      scope         TEXT,
      token_type    TEXT,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (provider, customer_id)
    );

    CREATE TABLE IF NOT EXISTS customer_calendars (
      provider    TEXT NOT NULL,
      customer_id TEXT NOT NULL,
      calendar_id TEXT NOT NULL,
      summary     TEXT,
      primary_cal BOOLEAN NOT NULL DEFAULT FALSE,
      selected    BOOLEAN NOT NULL DEFAULT FALSE,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (provider, customer_id, calendar_id)
    );

    CREATE TABLE IF NOT EXISTS customer_settings (
      customer_id     TEXT PRIMARY KEY,
      timezone        TEXT NOT NULL DEFAULT 'America/Denver',
      work_start_hour INT  NOT NULL DEFAULT 9,
      work_end_hour   INT  NOT NULL DEFAULT 17,
      work_days       TEXT NOT NULL DEFAULT '[0,1,2,3,4]',
      created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


init_db()


# -------------------------
# Debug key helper
# -------------------------
def require_debug_key(request: Request) -> None:
    if not DEBUG_API_KEY:
        return
    key = request.headers.get("x-debug-key") or request.query_params.get("key")
    if key != DEBUG_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


# -------------------------
# Time helpers
# -------------------------
def iso_z(dt_utc: datetime) -> str:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    dt_utc = dt_utc.astimezone(timezone.utc)
    return dt_utc.isoformat().replace("+00:00", "Z")


def parse_iso_to_utc(raw: str) -> datetime:
    s = (raw or "").strip()
    if not s:
        raise ValueError("Empty datetime")
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_local_naive_assume_tz(raw: str, tz_name: str) -> datetime:
    s = (raw or "").strip()
    dt = datetime.fromisoformat(s)  # naive
    tz = ZoneInfo(tz_name)
    return dt.replace(tzinfo=tz).astimezone(timezone.utc)


def format_local(dt_utc: datetime, tz: ZoneInfo) -> str:
    return dt_utc.astimezone(tz).strftime("%a %b %d, %Y %I:%M %p %Z")


def safe_cal_id(cal_id: str) -> str:
    return urllib.parse.quote(cal_id, safe="")


def normalize_work_days(x: Any) -> List[int]:
    if x is None:
        return [0, 1, 2, 3, 4]
    if isinstance(x, list):
        return [int(v) for v in x]
    if isinstance(x, str):
        try:
            arr = json.loads(x)
            if isinstance(arr, list):
                return [int(v) for v in arr]
        except Exception:
            pass
    return [0, 1, 2, 3, 4]


# -------------------------
# Interval helpers
# -------------------------
def merge_intervals(intervals: List[Dict[str, datetime]]) -> List[Dict[str, datetime]]:
    cleaned = [i for i in intervals if i["end"] > i["start"]]
    if not cleaned:
        return []
    cleaned.sort(key=lambda x: x["start"])
    out = [cleaned[0].copy()]
    for cur in cleaned[1:]:
        last = out[-1]
        if cur["start"] <= last["end"]:
            last["end"] = max(last["end"], cur["end"])
        else:
            out.append(cur.copy())
    return out


def subtract_busy_from_window(win_start: datetime, win_end: datetime, busy_merged: List[Dict[str, datetime]]) -> List[Dict[str, datetime]]:
    if win_end <= win_start:
        return []
    free = []
    cursor = win_start
    for b in busy_merged:
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
    if step_minutes <= 1:
        return dt_utc.replace(second=0, microsecond=0)
    dt_utc = dt_utc.replace(second=0, microsecond=0)
    epoch = int(dt_utc.timestamp())
    step = step_minutes * 60
    rounded = ((epoch + step - 1) // step) * step
    return datetime.fromtimestamp(rounded, tz=timezone.utc)


def slot_overlaps_busy(slot_start: datetime, slot_end: datetime, busy_merged: List[Dict[str, datetime]]) -> bool:
    for b in busy_merged:
        if slot_start < b["end"] and slot_end > b["start"]:
            return True
    return False


def pick_three_suggestions(available: List[Dict[str, str]], preferred_utc: Optional[datetime]) -> List[Dict[str, str]]:
    if not available:
        return []
    if preferred_utc is None:
        return available[:3]

    def dist(item: Dict[str, str]) -> float:
        s = parse_iso_to_utc(item["startUtc"])
        return abs((s - preferred_utc).total_seconds())

    return sorted(available, key=dist)[:3]


# -------------------------
# Google OAuth helpers
# -------------------------
def build_google_auth_url(state: str) -> str:
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope": " ".join(GOOGLE_SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "true",
        "state": state,
    }
    return f"{GOOGLE_AUTH_URL}?{urllib.parse.urlencode(params)}"


def safe_json(r: requests.Response) -> Optional[Dict[str, Any]]:
    try:
        return r.json()
    except Exception:
        return None


def exchange_code_for_tokens(code: str) -> Dict[str, Any]:
    data = {
        "code": code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "grant_type": "authorization_code",
    }
    r = requests.post(GOOGLE_TOKEN_URL, data=data, timeout=30)
    return {"status": r.status_code, "json": safe_json(r), "text": r.text}


def refresh_access_token(refresh_token: str) -> str:
    data = {
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    r = requests.post(GOOGLE_TOKEN_URL, data=data, timeout=30)
    j = safe_json(r) or {}
    if r.status_code != 200 or "access_token" not in j:
        raise HTTPException(status_code=500, detail=f"Failed to refresh access token: {r.text}")
    return j["access_token"]


def google_userinfo(access_token: str) -> Dict[str, Any]:
    r = requests.get(GOOGLE_USERINFO_URL, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    j = safe_json(r)
    if r.status_code != 200 or not j:
        raise HTTPException(status_code=500, detail=f"Failed to fetch userinfo: {r.text}")
    return j


# -------------------------
# DB accessors
# -------------------------
def upsert_oauth_state(state: str, customer_id: str) -> None:
    q = text("""
        INSERT INTO oauth_states(state, customer_id)
        VALUES (:state, :cid)
        ON CONFLICT (state) DO UPDATE SET customer_id = EXCLUDED.customer_id
    """)
    with engine.begin() as conn:
        conn.execute(q, {"state": state, "cid": customer_id})


def consume_oauth_state(state: str) -> Optional[str]:
    with engine.begin() as conn:
        row = conn.execute(text("SELECT customer_id FROM oauth_states WHERE state=:s"), {"s": state}).fetchone()
        if not row:
            return None
        conn.execute(text("DELETE FROM oauth_states WHERE state=:s"), {"s": state})
        return row[0]


def save_google_token(customer_id: str, user_email: str, refresh_token: str, scope: str, token_type: str) -> None:
    q = text("""
        INSERT INTO oauth_tokens(provider, customer_id, user_email, refresh_token, scope, token_type)
        VALUES ('google', :cid, :email, :rt, :scope, :tt)
        ON CONFLICT (provider, customer_id) DO UPDATE SET
          user_email = EXCLUDED.user_email,
          refresh_token = EXCLUDED.refresh_token,
          scope = EXCLUDED.scope,
          token_type = EXCLUDED.token_type
    """)
    with engine.begin() as conn:
        conn.execute(q, {"cid": customer_id, "email": user_email, "rt": refresh_token, "scope": scope, "tt": token_type})


def load_refresh_token(customer_id: str) -> str:
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT refresh_token FROM oauth_tokens WHERE provider='google' AND customer_id=:cid"),
            {"cid": customer_id},
        ).fetchone()
        if not row:
            raise HTTPException(status_code=400, detail="Google not connected for this customerId")
        return row[0]


def ensure_customer_settings(customer_id: str) -> Dict[str, Any]:
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT timezone, work_start_hour, work_end_hour, work_days FROM customer_settings WHERE customer_id=:cid"),
            {"cid": customer_id},
        ).fetchone()
        if row:
            return {"timezone": row[0], "work_start_hour": row[1], "work_end_hour": row[2], "work_days": normalize_work_days(row[3])}

        conn.execute(
            text("""
                INSERT INTO customer_settings(customer_id, timezone, work_start_hour, work_end_hour, work_days)
                VALUES (:cid, 'America/Denver', 9, 17, '[0,1,2,3,4]')
            """),
            {"cid": customer_id},
        )
        return {"timezone": "America/Denver", "work_start_hour": 9, "work_end_hour": 17, "work_days": [0, 1, 2, 3, 4]}


def upsert_calendars(customer_id: str, calendars: List[Dict[str, Any]]) -> None:
    q = text("""
        INSERT INTO customer_calendars(provider, customer_id, calendar_id, summary, primary_cal, selected)
        VALUES ('google', :cid, :calid, :summary, :primary, :selected)
        ON CONFLICT (provider, customer_id, calendar_id) DO UPDATE SET
          summary = EXCLUDED.summary,
          primary_cal = EXCLUDED.primary_cal
    """)
    with engine.begin() as conn:
        for c in calendars:
            calid = c.get("id")
            if not calid:
                continue
            primary = bool(c.get("primary", False))
            selected = True if primary else False
            conn.execute(q, {"cid": customer_id, "calid": calid, "summary": c.get("summary"), "primary": primary, "selected": selected})

        count_sel = conn.execute(
            text("SELECT COUNT(*) FROM customer_calendars WHERE provider='google' AND customer_id=:cid AND selected=true"),
            {"cid": customer_id},
        ).scalar_one()
        if count_sel == 0:
            conn.execute(
                text("""
                    UPDATE customer_calendars
                    SET selected=true
                    WHERE provider='google' AND customer_id=:cid AND primary_cal=true
                """),
                {"cid": customer_id},
            )


def list_calendars_db(customer_id: str) -> List[Dict[str, Any]]:
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT calendar_id, summary, primary_cal, selected
                FROM customer_calendars
                WHERE provider='google' AND customer_id=:cid
                ORDER BY primary_cal DESC, summary NULLS LAST, calendar_id
            """),
            {"cid": customer_id},
        ).fetchall()
        return [{"calendarId": r[0], "summary": r[1], "primary": bool(r[2]), "selected": bool(r[3])} for r in rows]


def set_selected_calendars(customer_id: str, calendar_ids: List[str]) -> None:
    if not calendar_ids:
        raise HTTPException(status_code=400, detail="calendarIds must not be empty")
    with engine.begin() as conn:
        conn.execute(text("UPDATE customer_calendars SET selected=false WHERE provider='google' AND customer_id=:cid"), {"cid": customer_id})
        conn.execute(
            text("""
                UPDATE customer_calendars
                SET selected=true
                WHERE provider='google' AND customer_id=:cid AND calendar_id = ANY(:ids)
            """),
            {"cid": customer_id, "ids": calendar_ids},
        )
        cnt = conn.execute(
            text("SELECT COUNT(*) FROM customer_calendars WHERE provider='google' AND customer_id=:cid AND selected=true"),
            {"cid": customer_id},
        ).scalar_one()
        if cnt == 0:
            raise HTTPException(status_code=400, detail="None of the provided calendarIds exist for this customerId")


def selected_calendar_ids(customer_id: str) -> List[str]:
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT calendar_id
                FROM customer_calendars
                WHERE provider='google' AND customer_id=:cid AND selected=true
                ORDER BY primary_cal DESC, calendar_id
            """),
            {"cid": customer_id},
        ).fetchall()
        ids = [r[0] for r in rows]
        return ids if ids else ["primary"]


# -------------------------
# Google API calls
# -------------------------
def google_calendar_list(access_token: str) -> Dict[str, Any]:
    r = requests.get(GOOGLE_CALENDAR_LIST_URL, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    return {"statusCode": r.status_code, "json": safe_json(r), "text": r.text}


def freebusy_raw(access_token: str, calendar_ids: List[str], time_min_utc: datetime, time_max_utc: datetime, time_zone: str) -> Dict[str, Any]:
    body = {
        "timeMin": iso_z(time_min_utc),
        "timeMax": iso_z(time_max_utc),
        "timeZone": time_zone,
        "items": [{"id": cid} for cid in calendar_ids],
    }
    r = requests.post(
        GOOGLE_FREEBUSY_URL,
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        data=json.dumps(body),
        timeout=30,
    )
    return {"statusCode": r.status_code, "json": safe_json(r), "text": r.text, "requestBody": body}


def google_create_event_api(access_token: str, calendar_id: str, summary: str, description: str, start_utc: datetime, end_utc: datetime, tz_name: str) -> Dict[str, Any]:
    tz = ZoneInfo(tz_name)
    body = {
        "summary": summary,
        "description": description,
        "start": {"dateTime": start_utc.astimezone(tz).isoformat(), "timeZone": tz_name},
        "end": {"dateTime": end_utc.astimezone(tz).isoformat(), "timeZone": tz_name},
    }
    url = GOOGLE_EVENTS_URL.format(calendarId=safe_cal_id(calendar_id))
    r = requests.post(url, headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}, data=json.dumps(body), timeout=30)
    return {"statusCode": r.status_code, "json": safe_json(r), "text": r.text, "requestBody": body}


# -------------------------
# Availability engine (overlap-safe)
# -------------------------
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

    busy_intervals: List[Dict[str, datetime]] = []
    calendars_obj = fb["json"].get("calendars", {}) or {}
    for _, info in calendars_obj.items():
        for b in (info.get("busy") or []):
            s, e = b.get("start"), b.get("end")
            if not s or not e:
                continue
            s_utc = parse_iso_to_utc(s)
            e_utc = parse_iso_to_utc(e)
            if e_utc > s_utc:
                busy_intervals.append({"start": s_utc, "end": e_utc})

    # Events fallback (authoritative)
    for cid in calendar_ids:
        url = GOOGLE_EVENTS_URL.format(calendarId=safe_cal_id(cid))
        params = {"timeMin": iso_z(time_min_utc), "timeMax": iso_z(time_max_utc), "singleEvents": "true", "orderBy": "startTime", "maxResults": "2500"}
        r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, params=params, timeout=30)
        if r.status_code != 200:
            continue
        items = (r.json() or {}).get("items", []) or []
        for ev in items:
            if ev.get("status") == "cancelled":
                continue
            start = (ev.get("start") or {}).get("dateTime")
            end = (ev.get("end") or {}).get("dateTime")
            if not start or not end:
                continue
            try:
                s_utc = parse_iso_to_utc(start)
                e_utc = parse_iso_to_utc(end)
                if e_utc > s_utc:
                    busy_intervals.append({"start": s_utc, "end": e_utc})
            except Exception:
                continue

    busy_merged = merge_intervals(busy_intervals)

    dur = timedelta(minutes=duration_minutes)
    available: List[Dict[str, str]] = []

    cur_day = start_local.date()
    last_day = end_local.date()

    while cur_day <= last_day:
        day0_local = datetime(cur_day.year, cur_day.month, cur_day.day, 0, 0, tzinfo=tz)
        weekday = day0_local.weekday()

        if weekday in work_days:
            win_start_local = day0_local.replace(hour=work_start_hour, minute=0, second=0, microsecond=0)
            win_end_local = day0_local.replace(hour=work_end_hour, minute=0, second=0, microsecond=0)

            if day0_local.date() == now_local.date() and now_local > win_start_local:
                win_start_local = now_local

            if win_end_local > win_start_local:
                win_start_utc = win_start_local.astimezone(timezone.utc)
                win_end_utc = win_end_local.astimezone(timezone.utc)

                free_intervals = subtract_busy_from_window(win_start_utc, win_end_utc, busy_merged)
                for fi in free_intervals:
                    s = round_up_to_step(fi["start"], step_minutes)
                    while s + dur <= fi["end"]:
                        e = s + dur
                        if not slot_overlaps_busy(s, e, busy_merged):
                            available.append({"startUtc": iso_z(s), "endUtc": iso_z(e), "startLocal": format_local(s, tz), "endLocal": format_local(e, tz)})
                        s = s + timedelta(minutes=step_minutes)

        cur_day = (day0_local + timedelta(days=1)).date()

    suggestions = pick_three_suggestions(available, preferred_utc)
    return {
        "ok": True,
        "timeZone": tz_name,
        "calendarIdsUsed": calendar_ids,
        "window": {"timeMinUtc": iso_z(time_min_utc), "timeMaxUtc": iso_z(time_max_utc)},
        "busyMergedCount": len(busy_merged),
        "availableCount": len(available),
        "suggestions": suggestions,
        "available": available[:500],
    }


# -------------------------
# Routes
# -------------------------
@app.get("/health")
def health():
    return {"ok": True}


@app.get("/debug/db")
def debug_db(request: Request):
    require_debug_key(request)
    with engine.begin() as conn:
        v = conn.execute(text("SELECT 1")).scalar_one()
    return {"db_ok": True, "select_1": v}


@app.get("/oauth/google/start")
def oauth_google_start(customerId: str = Query(...)):
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REDIRECT_URI and APP_BASE_URL):
        raise HTTPException(status_code=500, detail="Missing Google OAuth env vars")
    state = secrets.token_urlsafe(24)
    upsert_oauth_state(state, customerId)
    return RedirectResponse(build_google_auth_url(state))


@app.get("/oauth/google/callback")
def oauth_google_callback(code: str = "", state: str = "", error: str = ""):
    if error:
        return JSONResponse({"connected": False, "error": error}, status_code=400)
    if not code or not state:
        return JSONResponse({"connected": False, "error": "Missing code/state"}, status_code=400)

    customer_id = consume_oauth_state(state)
    if not customer_id:
        return JSONResponse({"connected": False, "error": "Invalid/expired state"}, status_code=400)

    tok = exchange_code_for_tokens(code)
    if tok["status"] != 200 or not tok["json"]:
        return JSONResponse({"connected": False, "error": "Token exchange failed", "google": tok["text"]}, status_code=500)

    tokens = tok["json"]
    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    scope = tokens.get("scope", "")
    token_type = tokens.get("token_type", "")

    if not access_token or not refresh_token:
        return JSONResponse({"connected": False, "error": "Missing access_token or refresh_token (try re-consent)"}, status_code=500)

    email = (google_userinfo(access_token) or {}).get("email", "unknown")
    save_google_token(customer_id, email, refresh_token, scope, token_type)

    ensure_customer_settings(customer_id)

    cal_list = google_calendar_list(access_token)
    if cal_list["statusCode"] == 200 and cal_list["json"]:
        items = (cal_list["json"].get("items") or [])
        keep = [{"id": it.get("id"), "summary": it.get("summary"), "primary": bool(it.get("primary", False))} for it in items]
        upsert_calendars(customer_id, keep)

    return JSONResponse({"connected": True, "customerId": customer_id, "email": email, "message": "Google connected."})


@app.get("/google/calendars")
def google_calendars(customerId: str):
    rt = load_refresh_token(customerId)
    access_token = refresh_access_token(rt)
    cal_list = google_calendar_list(access_token)
    if cal_list["statusCode"] == 200 and cal_list["json"]:
        items = (cal_list["json"].get("items") or [])
        keep = [{"id": it.get("id"), "summary": it.get("summary"), "primary": bool(it.get("primary", False))} for it in items]
        upsert_calendars(customerId, keep)
    return {"customerId": customerId, "calendars": list_calendars_db(customerId)}


@app.post("/google/calendars/select")
async def google_select_calendars(payload: Dict[str, Any]):
    customer_id = payload.get("customerId")
    calendar_ids = payload.get("calendarIds") or []
    if not customer_id:
        raise HTTPException(status_code=400, detail="customerId required")
    if not isinstance(calendar_ids, list) or not calendar_ids:
        raise HTTPException(status_code=400, detail="calendarIds must be a non-empty list")
    set_selected_calendars(customer_id, calendar_ids)
    return {"ok": True, "customerId": customer_id, "selected": selected_calendar_ids(customer_id)}


@app.post("/google/availability")
async def google_availability(payload: Dict[str, Any]):
    customer_id = payload.get("customerId")
    if not customer_id:
        raise HTTPException(status_code=400, detail="customerId required")

    settings = ensure_customer_settings(customer_id)

    tz_name = payload.get("timeZone") or settings["timezone"]
    work_start = int(payload.get("workStartHour", settings["work_start_hour"]))
    work_end = int(payload.get("workEndHour", settings["work_end_hour"]))
    work_days = normalize_work_days(payload.get("workDays", settings["work_days"]))

    duration = int(payload.get("durationMinutes", 60))
    step = int(payload.get("stepMinutes", 30))
    days = int(payload.get("days", 7))

    cal_ids = payload.get("calendarIds")
    calendar_ids = cal_ids if isinstance(cal_ids, list) and cal_ids else selected_calendar_ids(customer_id)

    preferred_raw = payload.get("preferredDateTimeUtc")
    preferred_utc = parse_iso_to_utc(preferred_raw) if preferred_raw else None

    rt = load_refresh_token(customer_id)
    access_token = refresh_access_token(rt)

    return compute_availability(
        access_token=access_token,
        tz_name=tz_name,
        work_start_hour=work_start,
        work_end_hour=work_end,
        work_days=work_days,
        calendar_ids=calendar_ids,
        duration_minutes=duration,
        step_minutes=step,
        days=days,
        preferred_utc=preferred_utc,
    )
