from __future__ import annotations

import os
import json
import secrets
import urllib.parse
import re
from typing import Any, Dict, List, Optional, Tuple

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
# Auth (simple API key)
# -------------------------
API_KEY = (os.environ.get("API_KEY") or "").strip()

def require_api_key(request: Request) -> None:
    """
    If API_KEY env var is set, every request (except /health) must include:
      x-api-key: <API_KEY>
    """
    if not API_KEY:
        return
    key = request.headers.get("x-api-key") or request.query_params.get("key")
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


# -------------------------
# Google Config (env)
# -------------------------
GOOGLE_CLIENT_ID = (os.environ.get("GOOGLE_CLIENT_ID") or "").strip()
GOOGLE_CLIENT_SECRET = (os.environ.get("GOOGLE_CLIENT_SECRET") or "").strip()
GOOGLE_REDIRECT_URI = (os.environ.get("GOOGLE_REDIRECT_URI") or "").strip()
APP_BASE_URL = (os.environ.get("APP_BASE_URL") or "").strip()

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"
GOOGLE_CALENDAR_LIST_URL = "https://www.googleapis.com/calendar/v3/users/me/calendarList"
GOOGLE_FREEBUSY_URL = "https://www.googleapis.com/calendar/v3/freeBusy"
GOOGLE_EVENTS_URL = "https://www.googleapis.com/calendar/v3/calendars/{calendarId}/events"
GOOGLE_EVENT_URL = "https://www.googleapis.com/calendar/v3/calendars/{calendarId}/events/{eventId}"

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
    raw = (os.environ.get("DATABASE_URL") or "").strip()
    if not raw:
        raise RuntimeError("DATABASE_URL env var is missing/empty")

    if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
        raw = raw[1:-1].strip()

    if raw.startswith("postgres://"):
        raw = raw.replace("postgres://", "postgresql+psycopg://", 1)
    elif raw.startswith("postgresql://"):
        raw = raw.replace("postgresql://", "postgresql+psycopg://", 1)
    elif raw.startswith("postgresql+psycopg2://"):
        raw = raw.replace("postgresql+psycopg2://", "postgresql+psycopg://", 1)

    if "://" not in raw:
        raise RuntimeError("DATABASE_URL malformed (missing scheme)")

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

def parse_any_datetime_to_utc(raw: str, tz_name: str) -> datetime:
    s = (raw or "").strip()
    if not s:
        raise ValueError("Empty datetime")

    if s.endswith("Z"):
        return parse_iso_to_utc(s)

    tail = s[-6:]
    if len(s) >= 6 and (tail[0] in "+-") and tail[3] == ":":
        return parse_iso_to_utc(s)

    dt = datetime.fromisoformat(s)
    tz = ZoneInfo(tz_name)
    return dt.replace(tzinfo=tz).astimezone(timezone.utc)

def format_local(dt_utc: datetime, tz: ZoneInfo) -> str:
    return dt_utc.astimezone(tz).strftime("%a %b %d, %Y %I:%M %p %Z")

def safe_cal_id(cal_id: str) -> str:
    return urllib.parse.quote(cal_id, safe="")

def safe_event_id(event_id: str) -> str:
    return urllib.parse.quote(event_id, safe="")

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
def merge_intervals_dt(intervals: List[Tuple[datetime, datetime]]) -> List[Tuple[datetime, datetime]]:
    cleaned = [(s, e) for (s, e) in intervals if e > s]
    if not cleaned:
        return []
    cleaned.sort(key=lambda x: x[0])
    out: List[List[datetime]] = [[cleaned[0][0], cleaned[0][1]]]
    for s, e in cleaned[1:]:
        last = out[-1]
        if s <= last[1]:
            last[1] = max(last[1], e)
        else:
            out.append([s, e])
    return [(a, b) for a, b in out]

def overlaps(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return a_start < b_end and b_start < a_end

def slot_is_free(
    slot_start: datetime,
    slot_end: datetime,
    merged_busy: List[Tuple[datetime, datetime]],
    start_idx: int,
) -> Tuple[bool, int]:
    i = start_idx
    while i < len(merged_busy) and merged_busy[i][1] <= slot_start:
        i += 1
    j = i
    while j < len(merged_busy) and merged_busy[j][0] < slot_end:
        bs, be = merged_busy[j]
        if overlaps(slot_start, slot_end, bs, be):
            return (False, i)
        j += 1
    return (True, i)

def round_up_to_step(dt_utc: datetime, step_minutes: int) -> datetime:
    dt_utc = dt_utc.astimezone(timezone.utc).replace(second=0, microsecond=0)
    if step_minutes <= 1:
        return dt_utc
    epoch = int(dt_utc.timestamp())
    step = step_minutes * 60
    rounded = ((epoch + step - 1) // step) * step
    return datetime.fromtimestamp(rounded, tz=timezone.utc)

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
            return {
                "timezone": row[0],
                "work_start_hour": row[1],
                "work_end_hour": row[2],
                "work_days": normalize_work_days(row[3]),
            }

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

def google_create_event_api(
    access_token: str,
    calendar_id: str,
    summary: str,
    description: str,
    start_utc: datetime,
    end_utc: datetime,
    tz_name: str,
    attendees: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    tz = ZoneInfo(tz_name)
    body: Dict[str, Any] = {
        "summary": summary,
        "description": description,
        "start": {"dateTime": start_utc.astimezone(tz).isoformat(), "timeZone": tz_name},
        "end": {"dateTime": end_utc.astimezone(tz).isoformat(), "timeZone": tz_name},
    }
    if attendees:
        body["attendees"] = [{"email": a.get("email")} for a in attendees if isinstance(a, dict) and a.get("email")]

    url = GOOGLE_EVENTS_URL.format(calendarId=safe_cal_id(calendar_id))
    r = requests.post(
        url,
        params={"sendUpdates": "none"},  # no emails
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        data=json.dumps(body),
        timeout=30,
    )
    return {"statusCode": r.status_code, "json": safe_json(r), "text": r.text, "requestBody": body}

def google_patch_event_api(access_token: str, calendar_id: str, event_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
    url = GOOGLE_EVENT_URL.format(calendarId=safe_cal_id(calendar_id), eventId=safe_event_id(event_id))
    r = requests.patch(
        url,
        params={"sendUpdates": "none"},  # no emails
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        data=json.dumps(patch),
        timeout=30,
    )
    return {"statusCode": r.status_code, "json": safe_json(r), "text": r.text, "requestBody": patch}

def google_delete_event_api(access_token: str, calendar_id: str, event_id: str) -> Dict[str, Any]:
    url = GOOGLE_EVENT_URL.format(calendarId=safe_cal_id(calendar_id), eventId=safe_event_id(event_id))
    r = requests.delete(url, params={"sendUpdates": "none"}, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    return {"statusCode": r.status_code, "text": r.text}

# -------------------------
# Busy collection
# -------------------------
def collect_busy_utc(access_token: str, tz_name: str, calendar_ids: List[str], time_min_utc: datetime, time_max_utc: datetime) -> List[Tuple[datetime, datetime]]:
    fb = freebusy_raw(access_token, calendar_ids, time_min_utc, time_max_utc, tz_name)

    busy_intervals: List[Tuple[datetime, datetime]] = []
    if fb["statusCode"] == 200 and fb["json"]:
        calendars_obj = fb["json"].get("calendars", {}) or {}
        for _, info in calendars_obj.items():
            for b in (info.get("busy") or []):
                s, e = b.get("start"), b.get("end")
                if not s or not e:
                    continue
                try:
                    s_utc = parse_iso_to_utc(s)
                    e_utc = parse_iso_to_utc(e)
                    if e_utc > s_utc:
                        busy_intervals.append((s_utc, e_utc))
                except Exception:
                    continue

    # events fallback
    for cid in calendar_ids:
        url = GOOGLE_EVENTS_URL.format(calendarId=safe_cal_id(cid))
        params = {
            "timeMin": iso_z(time_min_utc),
            "timeMax": iso_z(time_max_utc),
            "singleEvents": "true",
            "orderBy": "startTime",
            "maxResults": "2500",
        }
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
                    busy_intervals.append((s_utc, e_utc))
            except Exception:
                continue

    return merge_intervals_dt(busy_intervals)

def check_slot_taken(access_token: str, tz_name: str, calendars_to_check: List[str], start_utc: datetime, end_utc: datetime) -> Dict[str, Any]:
    time_min = start_utc - timedelta(minutes=1)
    time_max = end_utc + timedelta(minutes=1)
    merged_busy = collect_busy_utc(access_token, tz_name, calendars_to_check, time_min, time_max)

    for bs, be in merged_busy:
        if overlaps(start_utc, end_utc, bs, be):
            return {
                "taken": True,
                "checkedCalendars": calendars_to_check,
                "busyMerged": [{"startUtc": iso_z(bs), "endUtc": iso_z(be)} for bs, be in merged_busy],
            }
    return {
        "taken": False,
        "checkedCalendars": calendars_to_check,
        "busyMerged": [{"startUtc": iso_z(bs), "endUtc": iso_z(be)} for bs, be in merged_busy],
    }

# -------------------------
# Search helpers
# -------------------------
def normalize_phone(phone: str) -> str:
    return re.sub(r"\D+", "", phone or "")

def event_match_score(event: Dict[str, Any], email: Optional[str], phone_digits: Optional[str]) -> int:
    """
    Score:
      +3 if attendee email matches (or organizer/creator)
      +2 if phone digits found in description
      +1 if email found in description text too
    """
    score = 0
    desc = (event.get("description") or "")
    desc_lower = desc.lower()

    if email:
        em = email.strip().lower()
        # attendees
        attendees = event.get("attendees") or []
        for a in attendees:
            if isinstance(a, dict) and (a.get("email") or "").strip().lower() == em:
                score += 3
                break
        # creator/organizer
        creator = (event.get("creator") or {}).get("email")
        organizer = (event.get("organizer") or {}).get("email")
        if (creator or "").strip().lower() == em:
            score += 3
        if (organizer or "").strip().lower() == em:
            score += 3
        # email in description
        if em in desc_lower:
            score += 1

    if phone_digits:
        if phone_digits and phone_digits in normalize_phone(desc):
            score += 2

    return score

def list_events(access_token: str, calendar_id: str, time_min_utc: datetime, time_max_utc: datetime, q: Optional[str] = None, max_results: int = 2500) -> List[Dict[str, Any]]:
    url = GOOGLE_EVENTS_URL.format(calendarId=safe_cal_id(calendar_id))
    params: Dict[str, Any] = {
        "timeMin": iso_z(time_min_utc),
        "timeMax": iso_z(time_max_utc),
        "singleEvents": "true",
        "orderBy": "startTime",
        "maxResults": max_results,
    }
    if q:
        params["q"] = q

    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, params=params, timeout=30)
    if r.status_code != 200:
        return []
    return (r.json() or {}).get("items", []) or []

# -------------------------
# Routes
# -------------------------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/oauth/google/start")
def oauth_google_start(customerId: str = Query(...)):
    # auth not required for oauth start
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REDIRECT_URI and APP_BASE_URL):
        raise HTTPException(status_code=500, detail="Missing Google OAuth env vars")
    state = secrets.token_urlsafe(24)
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO oauth_states(state, customer_id)
                VALUES (:state, :cid)
                ON CONFLICT (state) DO UPDATE SET customer_id = EXCLUDED.customer_id
            """),
            {"state": state, "cid": customerId},
        )
    return RedirectResponse(build_google_auth_url(state))

@app.get("/oauth/google/callback")
def oauth_google_callback(code: str = "", state: str = "", error: str = ""):
    # auth not required for oauth callback
    if error:
        return JSONResponse({"connected": False, "error": error}, status_code=400)
    if not code or not state:
        return JSONResponse({"connected": False, "error": "Missing code/state"}, status_code=400)

    with engine.begin() as conn:
        row = conn.execute(text("SELECT customer_id FROM oauth_states WHERE state=:s"), {"s": state}).fetchone()
        if not row:
            return JSONResponse({"connected": False, "error": "Invalid/expired state"}, status_code=400)
        customer_id = row[0]
        conn.execute(text("DELETE FROM oauth_states WHERE state=:s"), {"s": state})

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

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO oauth_tokens(provider, customer_id, user_email, refresh_token, scope, token_type)
                VALUES ('google', :cid, :email, :rt, :scope, :tt)
                ON CONFLICT (provider, customer_id) DO UPDATE SET
                  user_email = EXCLUDED.user_email,
                  refresh_token = EXCLUDED.refresh_token,
                  scope = EXCLUDED.scope,
                  token_type = EXCLUDED.token_type
            """),
            {"cid": customer_id, "email": email, "rt": refresh_token, "scope": scope, "tt": token_type},
        )

    ensure_customer_settings(customer_id)

    cal_list = google_calendar_list(access_token)
    if cal_list["statusCode"] == 200 and cal_list["json"]:
        items = (cal_list["json"].get("items") or [])
        keep = [{"id": it.get("id"), "summary": it.get("summary"), "primary": bool(it.get("primary", False))} for it in items]
        upsert_calendars(customer_id, keep)

    return JSONResponse({"connected": True, "customerId": customer_id, "email": email, "message": "Google connected."})

@app.post("/google/search_events")
async def google_search_events(request: Request, payload: Dict[str, Any]):
    require_api_key(request)

    customer_id = payload.get("customerId")
    if not customer_id:
        raise HTTPException(status_code=400, detail="customerId required")

    email = (payload.get("email") or "").strip().lower() or None
    phone = (payload.get("phone") or "").strip() or None
    phone_digits = normalize_phone(phone) if phone else None

    if not email and not phone_digits:
        raise HTTPException(status_code=400, detail="Provide email and/or phone")

    days_back = int(payload.get("daysBack", 365))
    days_forward = int(payload.get("daysForward", 365))
    limit = int(payload.get("limit", 25))

    settings = ensure_customer_settings(customer_id)
    tz_name = payload.get("timeZone") or settings["timezone"]

    cal_ids = payload.get("calendarIds")
    calendar_ids = cal_ids if isinstance(cal_ids, list) and cal_ids else selected_calendar_ids(customer_id)

    rt = load_refresh_token(customer_id)
    access_token = refresh_access_token(rt)

    now_utc = datetime.now(timezone.utc)
    time_min = now_utc - timedelta(days=days_back)
    time_max = now_utc + timedelta(days=days_forward)

    # If we have email, use q=email to reduce results. Phone isn't good for q (often not indexed).
    q = email if email else None

    matches: List[Dict[str, Any]] = []
    tz = ZoneInfo(tz_name)

    for cid in calendar_ids:
        events = list_events(access_token, cid, time_min, time_max, q=q, max_results=2500)
        for ev in events:
            if ev.get("status") == "cancelled":
                continue
            score = event_match_score(ev, email=email, phone_digits=phone_digits)
            if score <= 0:
                continue

            start = (ev.get("start") or {}).get("dateTime")
            end = (ev.get("end") or {}).get("dateTime")
            if not start or not end:
                continue

            try:
                s_utc = parse_iso_to_utc(start)
                e_utc = parse_iso_to_utc(end)
            except Exception:
                continue

            matches.append(
                {
                    "calendarId": cid,
                    "eventId": ev.get("id"),
                    "summary": ev.get("summary"),
                    "startUtc": iso_z(s_utc),
                    "endUtc": iso_z(e_utc),
                    "startLocal": format_local(s_utc, tz),
                    "endLocal": format_local(e_utc, tz),
                    "matchScore": score,
                }
            )

    matches.sort(key=lambda x: (-int(x["matchScore"]), x["startUtc"]))
    matches = matches[: max(1, limit)]

    return {
        "ok": True,
        "customerId": customer_id,
        "calendarIdsSearched": calendar_ids,
        "query": {"email": email, "phone": phone_digits},
        "count": len(matches),
        "matches": matches,
    }

@app.post("/google/create_event")
async def google_create_event(request: Request, payload: Dict[str, Any]):
    require_api_key(request)

    customer_id = payload.get("customerId")
    if not customer_id:
        raise HTTPException(status_code=400, detail="customerId required")

    settings = ensure_customer_settings(customer_id)
    tz_name = payload.get("timeZone") or settings["timezone"]

    calendar_id = (payload.get("calendarId") or "primary").strip() or "primary"
    summary = (payload.get("summary") or "").strip() or "Appointment"
    description = (payload.get("description") or "").strip()

    attendees = payload.get("attendees") or []
    if not isinstance(attendees, list):
        attendees = []

    start_obj = payload.get("start") or {}
    end_obj = payload.get("end") or {}
    raw_start = (start_obj.get("dateTime") or "").strip()
    raw_end = (end_obj.get("dateTime") or "").strip()

    try:
        start_utc = parse_any_datetime_to_utc(raw_start, tz_name)
        end_utc = parse_any_datetime_to_utc(raw_end, tz_name)
    except Exception as e:
        return {"booked": False, "reason": "invalid_datetime", "message": "Invalid start/end datetime", "error": repr(e)}

    if end_utc <= start_utc:
        return {"booked": False, "reason": "invalid_range", "message": "end must be after start"}

    rt = load_refresh_token(customer_id)
    access_token = refresh_access_token(rt)

    calendars_to_check = selected_calendar_ids(customer_id)
    if calendar_id not in calendars_to_check:
        calendars_to_check = [calendar_id] + calendars_to_check

    taken = check_slot_taken(access_token, tz_name, calendars_to_check, start_utc, end_utc)
    if taken["taken"]:
        return {
            "booked": False,
            "reason": "slot_taken",
            "message": "That time is already booked. Please pick another slot.",
            "checkedCalendars": taken["checkedCalendars"],
            "busyMerged": taken["busyMerged"],
        }

    created = google_create_event_api(
        access_token,
        calendar_id,
        summary,
        description,
        start_utc,
        end_utc,
        tz_name,
        attendees=attendees,
    )
    if created["statusCode"] not in (200, 201):
        return {"booked": False, "reason": "google_create_failed", "statusCode": created["statusCode"], "googleResponseText": created["text"]}

    return {"booked": True, "calendarId": calendar_id, "event": created.get("json"), "startUtc": iso_z(start_utc), "endUtc": iso_z(end_utc)}

@app.post("/google/reschedule_event")
async def google_reschedule_event(request: Request, payload: Dict[str, Any]):
    require_api_key(request)

    customer_id = payload.get("customerId")
    if not customer_id:
        raise HTTPException(status_code=400, detail="customerId required")

    calendar_id = (payload.get("calendarId") or "primary").strip() or "primary"
    event_id = (payload.get("eventId") or "").strip()
    if not event_id:
        raise HTTPException(status_code=400, detail="eventId required")

    settings = ensure_customer_settings(customer_id)
    tz_name = payload.get("timeZone") or settings["timezone"]

    start_obj = payload.get("start") or {}
    end_obj = payload.get("end") or {}
    raw_start = (start_obj.get("dateTime") or "").strip()
    raw_end = (end_obj.get("dateTime") or "").strip()

    try:
        start_utc = parse_any_datetime_to_utc(raw_start, tz_name)
        end_utc = parse_any_datetime_to_utc(raw_end, tz_name)
    except Exception as e:
        return {"rescheduled": False, "reason": "invalid_datetime", "message": "Invalid start/end datetime", "error": repr(e)}

    if end_utc <= start_utc:
        return {"rescheduled": False, "reason": "invalid_range", "message": "end must be after start"}

    rt = load_refresh_token(customer_id)
    access_token = refresh_access_token(rt)

    calendars_to_check = selected_calendar_ids(customer_id)
    if calendar_id not in calendars_to_check:
        calendars_to_check = [calendar_id] + calendars_to_check

    taken = check_slot_taken(access_token, tz_name, calendars_to_check, start_utc, end_utc)
    if taken["taken"]:
        return {
            "rescheduled": False,
            "reason": "slot_taken",
            "message": "That time is already booked. Please pick another slot.",
            "checkedCalendars": taken["checkedCalendars"],
            "busyMerged": taken["busyMerged"],
        }

    tz = ZoneInfo(tz_name)
    patch = {
        "start": {"dateTime": start_utc.astimezone(tz).isoformat(), "timeZone": tz_name},
        "end": {"dateTime": end_utc.astimezone(tz).isoformat(), "timeZone": tz_name},
    }

    updated = google_patch_event_api(access_token, calendar_id, event_id, patch)
    if updated["statusCode"] not in (200, 201):
        return {"rescheduled": False, "reason": "google_patch_failed", "statusCode": updated["statusCode"], "googleResponseText": updated["text"]}

    return {"rescheduled": True, "calendarId": calendar_id, "eventId": event_id, "event": updated.get("json"), "startUtc": iso_z(start_utc), "endUtc": iso_z(end_utc)}

@app.post("/google/reschedule_events")
async def google_reschedule_events(request: Request, payload: Dict[str, Any]):
    require_api_key(request)

    customer_id = payload.get("customerId")
    if not customer_id:
        raise HTTPException(status_code=400, detail="customerId required")

    items = payload.get("items") or []
    if not isinstance(items, list) or not items:
        raise HTTPException(status_code=400, detail="items must be a non-empty list")

    settings = ensure_customer_settings(customer_id)
    tz_name = payload.get("timeZone") or settings["timezone"]

    rt = load_refresh_token(customer_id)
    access_token = refresh_access_token(rt)

    results = []
    for it in items:
        if not isinstance(it, dict):
            continue
        cal_id = (it.get("calendarId") or "primary").strip() or "primary"
        ev_id = (it.get("eventId") or "").strip()
        if not ev_id:
            results.append({"rescheduled": False, "reason": "missing_eventId"})
            continue

        start_obj = it.get("start") or {}
        end_obj = it.get("end") or {}
        raw_start = (start_obj.get("dateTime") or "").strip()
        raw_end = (end_obj.get("dateTime") or "").strip()

        try:
            start_utc = parse_any_datetime_to_utc(raw_start, tz_name)
            end_utc = parse_any_datetime_to_utc(raw_end, tz_name)
        except Exception as e:
            results.append({"calendarId": cal_id, "eventId": ev_id, "rescheduled": False, "reason": "invalid_datetime", "error": repr(e)})
            continue

        calendars_to_check = selected_calendar_ids(customer_id)
        if cal_id not in calendars_to_check:
            calendars_to_check = [cal_id] + calendars_to_check

        taken = check_slot_taken(access_token, tz_name, calendars_to_check, start_utc, end_utc)
        if taken["taken"]:
            results.append({"calendarId": cal_id, "eventId": ev_id, "rescheduled": False, "reason": "slot_taken"})
            continue

        tz = ZoneInfo(tz_name)
        patch = {
            "start": {"dateTime": start_utc.astimezone(tz).isoformat(), "timeZone": tz_name},
            "end": {"dateTime": end_utc.astimezone(tz).isoformat(), "timeZone": tz_name},
        }
        updated = google_patch_event_api(access_token, cal_id, ev_id, patch)
        if updated["statusCode"] not in (200, 201):
            results.append({"calendarId": cal_id, "eventId": ev_id, "rescheduled": False, "reason": "google_patch_failed", "statusCode": updated["statusCode"]})
            continue

        results.append({"calendarId": cal_id, "eventId": ev_id, "rescheduled": True, "startUtc": iso_z(start_utc), "endUtc": iso_z(end_utc)})

    return {"ok": True, "customerId": customer_id, "count": len(results), "results": results}

@app.post("/google/cancel_event")
async def google_cancel_event(request: Request, payload: Dict[str, Any]):
    require_api_key(request)

    customer_id = payload.get("customerId")
    if not customer_id:
        raise HTTPException(status_code=400, detail="customerId required")

    calendar_id = (payload.get("calendarId") or "primary").strip() or "primary"
    event_id = (payload.get("eventId") or "").strip()
    if not event_id:
        raise HTTPException(status_code=400, detail="eventId required")

    rt = load_refresh_token(customer_id)
    access_token = refresh_access_token(rt)

    deleted = google_delete_event_api(access_token, calendar_id, event_id)
    if deleted["statusCode"] not in (200, 204):
        return {"cancelled": False, "reason": "google_delete_failed", "statusCode": deleted["statusCode"], "googleResponseText": deleted["text"]}

    return {"cancelled": True, "calendarId": calendar_id, "eventId": event_id}

@app.post("/google/cancel_events")
async def google_cancel_events(request: Request, payload: Dict[str, Any]):
    require_api_key(request)

    customer_id = payload.get("customerId")
    if not customer_id:
        raise HTTPException(status_code=400, detail="customerId required")

    events = payload.get("events") or []
    if not isinstance(events, list) or not events:
        raise HTTPException(status_code=400, detail="events must be a non-empty list")

    rt = load_refresh_token(customer_id)
    access_token = refresh_access_token(rt)

    results = []
    for it in events:
        if not isinstance(it, dict):
            continue
        calendar_id = (it.get("calendarId") or "primary").strip() or "primary"
        event_id = (it.get("eventId") or "").strip()
        if not event_id:
            results.append({"cancelled": False, "reason": "missing_eventId"})
            continue

        deleted = google_delete_event_api(access_token, calendar_id, event_id)
        if deleted["statusCode"] not in (200, 204):
            results.append({"calendarId": calendar_id, "eventId": event_id, "cancelled": False, "reason": "google_delete_failed", "statusCode": deleted["statusCode"]})
        else:
            results.append({"calendarId": calendar_id, "eventId": event_id, "cancelled": True})

    return {"ok": True, "customerId": customer_id, "count": len(results), "results": results}
def google_patch_event_api(
    access_token: str,
    calendar_id: str,
    event_id: str,
    start_utc: datetime,
    end_utc: datetime,
    tz_name: str,
) -> Dict[str, Any]:
    tz = ZoneInfo(tz_name)
    body = {
        "start": {"dateTime": start_utc.astimezone(tz).isoformat(), "timeZone": tz_name},
        "end":   {"dateTime": end_utc.astimezone(tz).isoformat(), "timeZone": tz_name},
    }
    url = GOOGLE_EVENTS_URL.format(calendarId=safe_cal_id(calendar_id)) + f"/{safe_cal_id(event_id)}"
    r = requests.patch(
        url,
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        data=json.dumps(body),
        timeout=30,
    )
    return {"statusCode": r.status_code, "json": safe_json(r), "text": r.text, "requestBody": body}


@app.post("/google/reschedule_event")
async def google_reschedule_event(payload: Dict[str, Any]):
    """
    Reschedule ONE event.
    Body:
    {
      "customerId":"pm_1",
      "calendarId":"primary",
      "eventId":"....",
      "timeZone":"America/Denver",
      "start":{"dateTime":"2026-03-05T20:00:00Z"},
      "end":{"dateTime":"2026-03-05T21:00:00Z"}
    }
    """
    customer_id = (payload.get("customerId") or "").strip()
    calendar_id = (payload.get("calendarId") or "").strip()
    event_id = (payload.get("eventId") or "").strip()

    if not customer_id:
        raise HTTPException(status_code=400, detail="customerId required")
    if not calendar_id:
        raise HTTPException(status_code=400, detail="calendarId required")
    if not event_id:
        raise HTTPException(status_code=400, detail="eventId required")

    settings = ensure_customer_settings(customer_id)
    tz_name = payload.get("timeZone") or settings["timezone"]

    start_obj = payload.get("start") or {}
    end_obj = payload.get("end") or {}
    raw_start = (start_obj.get("dateTime") or "").strip()
    raw_end = (end_obj.get("dateTime") or "").strip()

    try:
        start_utc = parse_any_datetime_to_utc(raw_start, tz_name)
        end_utc = parse_any_datetime_to_utc(raw_end, tz_name)
    except Exception as e:
        return {"ok": False, "rescheduled": False, "reason": "invalid_datetime", "error": repr(e)}

    if end_utc <= start_utc:
        return {"ok": False, "rescheduled": False, "reason": "invalid_range", "message": "end must be after start"}

    rt = load_refresh_token(customer_id)
    access_token = refresh_access_token(rt)

    # Check conflicts across selected calendars + the target calendar
    calendars_to_check = selected_calendar_ids(customer_id)
    if calendar_id not in calendars_to_check:
        calendars_to_check = [calendar_id] + calendars_to_check

    time_min = start_utc - timedelta(minutes=1)
    time_max = end_utc + timedelta(minutes=1)

    busy_pack = collect_busy_utc(access_token, tz_name, calendars_to_check, time_min, time_max)
    merged_busy: List[Tuple[datetime, datetime]] = []
    for it in busy_pack["busyMerged"]:
        merged_busy.append((parse_iso_to_utc(it["startUtc"]), parse_iso_to_utc(it["endUtc"])))
    merged_busy = merge_intervals_dt(merged_busy)

    for bs, be in merged_busy:
        if overlaps(start_utc, end_utc, bs, be):
            return {
                "ok": True,
                "rescheduled": False,
                "reason": "slot_taken",
                "message": "That time is already booked. Please pick another slot.",
                "checkedCalendars": calendars_to_check,
                "busyMerged": [{"startUtc": iso_z(bs), "endUtc": iso_z(be)} for bs, be in merged_busy],
            }

    patched = google_patch_event_api(access_token, calendar_id, event_id, start_utc, end_utc, tz_name)
    if patched["statusCode"] not in (200,):
        return {
            "ok": False,
            "rescheduled": False,
            "reason": "google_patch_failed",
            "statusCode": patched["statusCode"],
            "googleResponseText": patched["text"],
            "requestBody": patched["requestBody"],
        }

    return {
        "ok": True,
        "rescheduled": True,
        "calendarId": calendar_id,
        "eventId": event_id,
        "startUtc": iso_z(start_utc),
        "endUtc": iso_z(end_utc),
        "event": patched.get("json"),
    }


@app.post("/google/reschedule_events")
async def google_reschedule_events(payload: Dict[str, Any]):
    """
    Reschedule MULTIPLE events.
    Body:
    {
      "customerId":"pm_1",
      "timeZone":"America/Denver",
      "items":[{...},{...}]
    }
    """
    customer_id = (payload.get("customerId") or "").strip()
    if not customer_id:
        raise HTTPException(status_code=400, detail="customerId required")

    settings = ensure_customer_settings(customer_id)
    tz_name = payload.get("timeZone") or settings["timezone"]

    items = payload.get("items") or []
    if not isinstance(items, list) or not items:
        raise HTTPException(status_code=400, detail="items must be a non-empty list")

    # Filter out blank rows safely
    clean_items = []
    for it in items:
        if not isinstance(it, dict):
            continue
        cal_id = (it.get("calendarId") or "").strip()
        ev_id = (it.get("eventId") or "").strip()
        s = ((it.get("start") or {}).get("dateTime") or "").strip()
        e = ((it.get("end") or {}).get("dateTime") or "").strip()
        if cal_id and ev_id and s and e:
            clean_items.append(it)

    if not clean_items:
        raise HTTPException(status_code=400, detail="No valid items found (calendarId/eventId/start/end required)")

    rt = load_refresh_token(customer_id)
    access_token = refresh_access_token(rt)

    results = []
    for it in clean_items:
        # Reuse the single endpoint logic by calling it directly
        single_payload = {
            "customerId": customer_id,
            "calendarId": it["calendarId"],
            "eventId": it["eventId"],
            "timeZone": tz_name,
            "start": it["start"],
            "end": it["end"],
        }
        res = await google_reschedule_event(single_payload)
        results.append(res)

    return {"ok": True, "count": len(results), "results": results}
