from __future__ import annotations

import json
import os
import re
import secrets
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

app = FastAPI(title="Calendar Backend", version="3.1.0")

APP_BASE_URL = (os.environ.get("APP_BASE_URL") or "").strip()
API_KEY = (os.environ.get("API_KEY") or "").strip()
DEBUG_API_KEY = (os.environ.get("DEBUG_API_KEY") or "").strip()

GOOGLE_CLIENT_ID = (os.environ.get("GOOGLE_CLIENT_ID") or "").strip()
GOOGLE_CLIENT_SECRET = (os.environ.get("GOOGLE_CLIENT_SECRET") or "").strip()
GOOGLE_REDIRECT_URI = (os.environ.get("GOOGLE_REDIRECT_URI") or "").strip()

MS_CLIENT_ID = (os.environ.get("MS_CLIENT_ID") or "").strip()
MS_CLIENT_SECRET = (os.environ.get("MS_CLIENT_SECRET") or "").strip()
MS_REDIRECT_URI = (os.environ.get("MS_REDIRECT_URI") or "").strip()
MS_TENANT = (os.environ.get("MS_TENANT") or "common").strip()

PROVIDER_GOOGLE = "google"
PROVIDER_MICROSOFT = "microsoft"
SUPPORTED_PROVIDERS = {PROVIDER_GOOGLE, PROVIDER_MICROSOFT}

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

MS_AUTHORIZE_URL = f"https://login.microsoftonline.com/{MS_TENANT}/oauth2/v2.0/authorize"
MS_TOKEN_URL = f"https://login.microsoftonline.com/{MS_TENANT}/oauth2/v2.0/token"

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
MS_ME_URL = f"{GRAPH_BASE}/me"
MS_LIST_CALENDARS_URL = f"{GRAPH_BASE}/me/calendars"
MS_CALENDARVIEW_URL = f"{GRAPH_BASE}/me/calendars/{{calendarId}}/calendarView"
MS_CREATE_EVENT_URL = f"{GRAPH_BASE}/me/calendars/{{calendarId}}/events"
MS_EVENT_URL = f"{GRAPH_BASE}/me/events/{{eventId}}"
MS_GET_SCHEDULE_URL = f"{GRAPH_BASE}/me/calendar/getSchedule"

MS_SCOPES = [
    "offline_access",
    "openid",
    "profile",
    "email",
    "https://graph.microsoft.com/User.Read",
    "https://graph.microsoft.com/Calendars.ReadWrite",
]


def require_api_key(request: Request) -> None:
    if not API_KEY:
        return
    key = request.headers.get("x-api-key") or request.headers.get("X-API-Key")
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


def require_debug_key(request: Request) -> None:
    if not DEBUG_API_KEY:
        return
    key = request.headers.get("x-debug-key") or request.headers.get("X-Debug-Key")
    if key != DEBUG_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


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
      provider    TEXT NOT NULL DEFAULT 'google',
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
        cols = conn.execute(
            text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name='oauth_states'
            """)
        ).fetchall()
        col_names = {r[0] for r in cols}
        if "provider" not in col_names:
            conn.execute(text("ALTER TABLE oauth_states ADD COLUMN provider TEXT NOT NULL DEFAULT 'google'"))


init_db()


def validate_provider(provider: str) -> str:
    p = (provider or "").strip().lower()
    if p not in SUPPORTED_PROVIDERS:
        raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider}")
    return p


def safe_json(r: requests.Response) -> Optional[Dict[str, Any]]:
    try:
        return r.json()
    except Exception:
        return None


def iso_z(dt_utc: datetime) -> str:
    dt = dt_utc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


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
    if s.endswith("Z") or ("+" in s[10:] or "-" in s[10:]):
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


def weekday_name_to_int(name: str) -> Optional[int]:
    m = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
        "saturday": 5,
        "sunday": 6,
    }
    if not name:
        return None
    return m.get(name.strip().lower())


def digits_only(s: str) -> str:
    return re.sub(r"\D+", "", s or "")


def merge_intervals_dt(intervals: List[Tuple[datetime, datetime]]) -> List[Tuple[datetime, datetime]]:
    cleaned = [(s, e) for s, e in intervals if e > s]
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


def pick_by_preference(
    available: List[Dict[str, str]],
    tz: ZoneInfo,
    preference: Optional[Dict[str, Any]],
    preferred_utc: Optional[datetime],
) -> List[Dict[str, str]]:
    if not available:
        return []
    pref = preference or {}
    max_results = int(pref.get("maxResults", 3))

    wanted_weekday = None
    if str(pref.get("type", "")).lower() == "weekday":
        wanted_weekday = weekday_name_to_int(str(pref.get("weekday", "")))

    tod = str(pref.get("timeOfDay", "any")).strip().lower()
    if tod not in ("morning", "afternoon", "any"):
        tod = "any"

    def is_morning(dt_utc: datetime) -> bool:
        return dt_utc.astimezone(tz).hour < 12

    def matches_filters(item: Dict[str, str], force_tod: str) -> bool:
        try:
            s = parse_iso_to_utc(item["startUtc"])
        except Exception:
            return False
        if wanted_weekday is not None and s.astimezone(tz).weekday() != wanted_weekday:
            return False
        if force_tod == "morning" and not is_morning(s):
            return False
        if force_tod == "afternoon" and is_morning(s):
            return False
        return True

    filtered = [x for x in available if matches_filters(x, tod)]
    if not filtered and tod in ("morning", "afternoon"):
        alt = "afternoon" if tod == "morning" else "morning"
        filtered = [x for x in available if matches_filters(x, alt)]

    if not filtered:
        return []

    strategy = str(pref.get("strategy", "soonest")).strip().lower()
    if strategy == "closest" and preferred_utc is not None:
        filtered.sort(key=lambda x: abs((parse_iso_to_utc(x["startUtc"]) - preferred_utc).total_seconds()))
        return filtered[:max_results]

    if strategy == "spread":
        if len(filtered) <= max_results:
            return filtered
        step = max(1, len(filtered) // max_results)
        out = []
        idx = 0
        while len(out) < max_results and idx < len(filtered):
            out.append(filtered[idx])
            idx += step
        return out[:max_results]

    filtered.sort(key=lambda x: x["startUtc"])
    return filtered[:max_results]


def upsert_oauth_state(state: str, customer_id: str, provider: str) -> None:
    q = text("""
        INSERT INTO oauth_states(state, customer_id, provider)
        VALUES (:state, :cid, :provider)
        ON CONFLICT (state) DO UPDATE SET
          customer_id = EXCLUDED.customer_id,
          provider = EXCLUDED.provider
    """)
    with engine.begin() as conn:
        conn.execute(q, {"state": state, "cid": customer_id, "provider": provider})


def consume_oauth_state(state: str) -> Optional[Dict[str, str]]:
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT customer_id, provider FROM oauth_states WHERE state=:s"),
            {"s": state},
        ).fetchone()
        if not row:
            return None
        conn.execute(text("DELETE FROM oauth_states WHERE state=:s"), {"s": state})
        return {"customer_id": row[0], "provider": row[1]}


def save_oauth_token(
    provider: str,
    customer_id: str,
    user_email: str,
    refresh_token: str,
    scope: str,
    token_type: str,
) -> None:
    q = text("""
        INSERT INTO oauth_tokens(provider, customer_id, user_email, refresh_token, scope, token_type)
        VALUES (:p, :cid, :email, :rt, :scope, :tt)
        ON CONFLICT (provider, customer_id) DO UPDATE SET
          user_email = EXCLUDED.user_email,
          refresh_token = EXCLUDED.refresh_token,
          scope = EXCLUDED.scope,
          token_type = EXCLUDED.token_type
    """)
    with engine.begin() as conn:
        conn.execute(
            q,
            {
                "p": provider,
                "cid": customer_id,
                "email": user_email,
                "rt": refresh_token,
                "scope": scope,
                "tt": token_type,
            },
        )


def load_refresh_token(provider: str, customer_id: str) -> str:
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT refresh_token FROM oauth_tokens WHERE provider=:p AND customer_id=:cid"),
            {"p": provider, "cid": customer_id},
        ).fetchone()
        if not row:
            raise HTTPException(status_code=400, detail=f"{provider} not connected for this customerId")
        return row[0]


def ensure_customer_settings(customer_id: str) -> Dict[str, Any]:
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT timezone, work_start_hour, work_end_hour, work_days
                FROM customer_settings
                WHERE customer_id=:cid
            """),
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
        return {
            "timezone": "America/Denver",
            "work_start_hour": 9,
            "work_end_hour": 17,
            "work_days": [0, 1, 2, 3, 4],
        }


def update_customer_settings(
    customer_id: str,
    tz_name: str,
    work_start: int,
    work_end: int,
    work_days: List[int],
) -> Dict[str, Any]:
    try:
        ZoneInfo(tz_name)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid timeZone")

    if not (0 <= work_start <= 23 and 1 <= work_end <= 24 and work_end > work_start):
        raise HTTPException(status_code=400, detail="Invalid working hours")
    if not work_days:
        raise HTTPException(status_code=400, detail="workDays cannot be empty")

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO customer_settings(customer_id, timezone, work_start_hour, work_end_hour, work_days)
                VALUES (:cid, :tz, :ws, :we, :wd)
                ON CONFLICT (customer_id) DO UPDATE SET
                  timezone = EXCLUDED.timezone,
                  work_start_hour = EXCLUDED.work_start_hour,
                  work_end_hour = EXCLUDED.work_end_hour,
                  work_days = EXCLUDED.work_days
            """),
            {"cid": customer_id, "tz": tz_name, "ws": work_start, "we": work_end, "wd": json.dumps(work_days)},
        )
    return ensure_customer_settings(customer_id)


def upsert_calendars(provider: str, customer_id: str, calendars: List[Dict[str, Any]]) -> None:
    q = text("""
        INSERT INTO customer_calendars(provider, customer_id, calendar_id, summary, primary_cal, selected)
        VALUES (:p, :cid, :calid, :summary, :primary, :selected)
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
            conn.execute(
                q,
                {
                    "p": provider,
                    "cid": customer_id,
                    "calid": calid,
                    "summary": c.get("summary"),
                    "primary": primary,
                    "selected": selected,
                },
            )

        count_sel = conn.execute(
            text("""
                SELECT COUNT(*)
                FROM customer_calendars
                WHERE provider=:p AND customer_id=:cid AND selected=true
            """),
            {"p": provider, "cid": customer_id},
        ).scalar_one()

        if count_sel == 0:
            conn.execute(
                text("""
                    UPDATE customer_calendars
                    SET selected=true
                    WHERE provider=:p AND customer_id=:cid AND primary_cal=true
                """),
                {"p": provider, "cid": customer_id},
            )


def list_calendars_db(provider: str, customer_id: str) -> List[Dict[str, Any]]:
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT calendar_id, summary, primary_cal, selected
                FROM customer_calendars
                WHERE provider=:p AND customer_id=:cid
                ORDER BY primary_cal DESC, summary NULLS LAST, calendar_id
            """),
            {"p": provider, "cid": customer_id},
        ).fetchall()

    return [{"calendarId": r[0], "summary": r[1], "primary": bool(r[2]), "selected": bool(r[3])} for r in rows]


def set_selected_calendars(provider: str, customer_id: str, calendar_ids: List[str]) -> None:
    if not calendar_ids:
        raise HTTPException(status_code=400, detail="calendarIds must not be empty")

    with engine.begin() as conn:
        conn.execute(
            text("UPDATE customer_calendars SET selected=false WHERE provider=:p AND customer_id=:cid"),
            {"p": provider, "cid": customer_id},
        )
        conn.execute(
            text("""
                UPDATE customer_calendars
                SET selected=true
                WHERE provider=:p AND customer_id=:cid AND calendar_id = ANY(:ids)
            """),
            {"p": provider, "cid": customer_id, "ids": calendar_ids},
        )
        cnt = conn.execute(
            text("""
                SELECT COUNT(*)
                FROM customer_calendars
                WHERE provider=:p AND customer_id=:cid AND selected=true
            """),
            {"p": provider, "cid": customer_id},
        ).scalar_one()

        if cnt == 0:
            raise HTTPException(status_code=400, detail="None of the provided calendarIds exist for this customerId")


def selected_calendar_ids(provider: str, customer_id: str, default_fallback: Optional[str] = None) -> List[str]:
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT calendar_id
                FROM customer_calendars
                WHERE provider=:p AND customer_id=:cid AND selected=true
                ORDER BY primary_cal DESC, calendar_id
            """),
            {"p": provider, "cid": customer_id},
        ).fetchall()
    ids = [r[0] for r in rows]
    return ids if ids else ([default_fallback] if default_fallback else [])


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


def exchange_google_code_for_tokens(code: str) -> Dict[str, Any]:
    r = requests.post(
        GOOGLE_TOKEN_URL,
        data={
            "code": code,
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "redirect_uri": GOOGLE_REDIRECT_URI,
            "grant_type": "authorization_code",
        },
        timeout=30,
    )
    return {"status": r.status_code, "json": safe_json(r), "text": r.text}


def refresh_google_access_token(refresh_token: str) -> str:
    r = requests.post(
        GOOGLE_TOKEN_URL,
        data={
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    j = safe_json(r) or {}
    if r.status_code != 200 or "access_token" not in j:
        raise HTTPException(status_code=500, detail=f"Failed to refresh Google access token: {r.text}")
    return j["access_token"]


def google_userinfo(access_token: str) -> Dict[str, Any]:
    r = requests.get(GOOGLE_USERINFO_URL, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    j = safe_json(r)
    if r.status_code != 200 or not j:
        raise HTTPException(status_code=500, detail=f"Failed to fetch Google userinfo: {r.text}")
    return j


def google_calendar_list_api(access_token: str) -> Dict[str, Any]:
    r = requests.get(GOOGLE_CALENDAR_LIST_URL, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    return {"statusCode": r.status_code, "json": safe_json(r), "text": r.text}


def google_freebusy_raw(
    access_token: str,
    calendar_ids: List[str],
    time_min_utc: datetime,
    time_max_utc: datetime,
    time_zone: str,
) -> Dict[str, Any]:
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


def google_list_events_api(access_token: str, calendar_id: str, time_min_utc: datetime, time_max_utc: datetime) -> Dict[str, Any]:
    url = GOOGLE_EVENTS_URL.format(calendarId=safe_cal_id(calendar_id))
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        params={
            "timeMin": iso_z(time_min_utc),
            "timeMax": iso_z(time_max_utc),
            "singleEvents": "true",
            "orderBy": "startTime",
            "maxResults": "2500",
        },
        timeout=30,
    )
    return {"statusCode": r.status_code, "json": safe_json(r), "text": r.text}


def google_create_event_api(
    access_token: str,
    calendar_id: str,
    summary: str,
    description: str,
    start_utc: datetime,
    end_utc: datetime,
    tz_name: str,
    attendees: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    tz = ZoneInfo(tz_name)
    body: Dict[str, Any] = {
        "summary": summary,
        "description": description,
        "start": {"dateTime": start_utc.astimezone(tz).isoformat(), "timeZone": tz_name},
        "end": {"dateTime": end_utc.astimezone(tz).isoformat(), "timeZone": tz_name},
    }
    if attendees and isinstance(attendees, list):
        body["attendees"] = [{"email": (a.get("email") or "").strip()} for a in attendees if isinstance(a, dict) and (a.get("email") or "").strip()]

    url = GOOGLE_EVENTS_URL.format(calendarId=safe_cal_id(calendar_id))
    r = requests.post(
        url,
        params={"sendUpdates": "none"},
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        data=json.dumps(body),
        timeout=30,
    )
    return {"statusCode": r.status_code, "json": safe_json(r), "text": r.text, "requestBody": body}


def google_delete_event_api(access_token: str, calendar_id: str, event_id: str) -> Dict[str, Any]:
    url = GOOGLE_EVENT_URL.format(calendarId=safe_cal_id(calendar_id), eventId=safe_event_id(event_id))
    r = requests.delete(url, params={"sendUpdates": "none"}, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    return {"statusCode": r.status_code, "text": r.text}


def google_patch_event_time_api(
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
        "end": {"dateTime": end_utc.astimezone(tz).isoformat(), "timeZone": tz_name},
    }
    url = GOOGLE_EVENT_URL.format(calendarId=safe_cal_id(calendar_id), eventId=safe_event_id(event_id))
    r = requests.patch(
        url,
        params={"sendUpdates": "none"},
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        data=json.dumps(body),
        timeout=30,
    )
    return {"statusCode": r.status_code, "json": safe_json(r), "text": r.text, "requestBody": body}


def google_collect_busy_utc(
    access_token: str,
    tz_name: str,
    calendar_ids: List[str],
    time_min_utc: datetime,
    time_max_utc: datetime,
) -> Dict[str, Any]:
    fb = google_freebusy_raw(access_token, calendar_ids, time_min_utc, time_max_utc, tz_name)
    busy_intervals: List[Tuple[datetime, datetime]] = []

    if fb["statusCode"] == 200 and fb["json"]:
        calendars_obj = fb["json"].get("calendars", {}) or {}
        for _, info in calendars_obj.items():
            for b in info.get("busy") or []:
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

    for cid in calendar_ids:
        r = google_list_events_api(access_token, cid, time_min_utc, time_max_utc)
        if r["statusCode"] != 200 or not r["json"]:
            continue
        for ev in (r["json"].get("items") or []):
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

    merged = merge_intervals_dt(busy_intervals)
    return {
        "ok": True,
        "checkedCalendars": list(calendar_ids),
        "timeMinUtc": iso_z(time_min_utc),
        "timeMaxUtc": iso_z(time_max_utc),
        "busyMerged": [{"startUtc": iso_z(s), "endUtc": iso_z(e)} for s, e in merged],
        "freebusyStatusCode": fb["statusCode"],
        "freebusyRequestBody": fb.get("requestBody"),
        "freebusyResponseText": fb.get("text"),
    }


def google_collect_busy_utc_excluding_event(
    access_token: str,
    calendar_ids: List[str],
    time_min_utc: datetime,
    time_max_utc: datetime,
    exclude_calendar_id: str,
    exclude_event_id: str,
) -> List[Tuple[datetime, datetime]]:
    busy_intervals: List[Tuple[datetime, datetime]] = []
    for cid in calendar_ids:
        r = google_list_events_api(access_token, cid, time_min_utc, time_max_utc)
        if r["statusCode"] != 200 or not r["json"]:
            continue
        for ev in (r["json"].get("items") or []):
            if ev.get("status") == "cancelled":
                continue
            if cid == exclude_calendar_id and (ev.get("id") or "") == exclude_event_id:
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


def build_microsoft_auth_url(state: str) -> str:
    params = {
        "client_id": MS_CLIENT_ID,
        "redirect_uri": MS_REDIRECT_URI,
        "response_type": "code",
        "response_mode": "query",
        "scope": " ".join(MS_SCOPES),
        "state": state,
        "prompt": "select_account",
    }
    return f"{MS_AUTHORIZE_URL}?{urllib.parse.urlencode(params)}"


def exchange_microsoft_code_for_tokens(code: str) -> Dict[str, Any]:
    r = requests.post(
        MS_TOKEN_URL,
        data={
            "client_id": MS_CLIENT_ID,
            "client_secret": MS_CLIENT_SECRET,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": MS_REDIRECT_URI,
            "scope": " ".join(MS_SCOPES),
        },
        timeout=30,
    )
    return {"status": r.status_code, "json": safe_json(r), "text": r.text}


def refresh_microsoft_access_token(refresh_token: str) -> str:
    r = requests.post(
        MS_TOKEN_URL,
        data={
            "client_id": MS_CLIENT_ID,
            "client_secret": MS_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "scope": " ".join(MS_SCOPES),
        },
        timeout=30,
    )
    j = safe_json(r) or {}
    if r.status_code != 200 or "access_token" not in j:
        raise HTTPException(status_code=500, detail=f"Failed to refresh Microsoft access token: {r.text}")
    return j["access_token"]


def _graph_headers(access_token: str, prefer_tz: Optional[str] = None) -> Dict[str, str]:
    h = {"Authorization": f"Bearer {access_token}"}
    if prefer_tz:
        h["Prefer"] = f'outlook.timezone="{prefer_tz}"'
    return h


def microsoft_me(access_token: str) -> Dict[str, Any]:
    r = requests.get(MS_ME_URL, headers=_graph_headers(access_token), timeout=30)
    j = safe_json(r)
    if r.status_code != 200 or not j:
        raise HTTPException(status_code=500, detail=f"Failed to fetch Microsoft /me: {r.text}")
    return j


def microsoft_list_calendars_api(access_token: str) -> Dict[str, Any]:
    r = requests.get(MS_LIST_CALENDARS_URL, headers=_graph_headers(access_token), timeout=30)
    return {"statusCode": r.status_code, "json": safe_json(r), "text": r.text}


def microsoft_calendar_view_api(access_token: str, calendar_id: str, time_min_utc: datetime, time_max_utc: datetime) -> Dict[str, Any]:
    url = MS_CALENDARVIEW_URL.format(calendarId=safe_cal_id(calendar_id))
    r = requests.get(
        url,
        headers=_graph_headers(access_token, prefer_tz="UTC"),
        params={
            "startDateTime": iso_z(time_min_utc),
            "endDateTime": iso_z(time_max_utc),
            "$top": "1000",
        },
        timeout=30,
    )
    return {"statusCode": r.status_code, "json": safe_json(r), "text": r.text}


def microsoft_get_schedule_api(
    access_token: str,
    schedule_email: str,
    time_min_utc: datetime,
    time_max_utc: datetime,
    time_zone: str,
    availability_view_interval: int = 30,
) -> Dict[str, Any]:
    body = {
        "schedules": [schedule_email],
        "startTime": {"dateTime": time_min_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"), "timeZone": "UTC"},
        "endTime": {"dateTime": time_max_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"), "timeZone": "UTC"},
        "availabilityViewInterval": max(5, min(1440, int(availability_view_interval))),
    }
    r = requests.post(
        MS_GET_SCHEDULE_URL,
        headers={**_graph_headers(access_token, prefer_tz=time_zone), "Content-Type": "application/json"},
        data=json.dumps(body),
        timeout=30,
    )
    return {"statusCode": r.status_code, "json": safe_json(r), "text": r.text, "requestBody": body}


def microsoft_create_event_api(
    access_token: str,
    calendar_id: str,
    summary: str,
    description: str,
    start_utc: datetime,
    end_utc: datetime,
    attendees: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    body: Dict[str, Any] = {
        "subject": summary,
        "body": {"contentType": "text", "content": description or ""},
        "start": {"dateTime": start_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"), "timeZone": "UTC"},
        "end": {"dateTime": end_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"), "timeZone": "UTC"},
    }
    if attendees and isinstance(attendees, list):
        out = []
        for a in attendees:
            if not isinstance(a, dict):
                continue
            em = (a.get("email") or "").strip()
            if not em:
                continue
            out.append({"emailAddress": {"address": em}, "type": "required"})
        if out:
            body["attendees"] = out

    url = MS_CREATE_EVENT_URL.format(calendarId=safe_cal_id(calendar_id))
    r = requests.post(
        url,
        headers={**_graph_headers(access_token), "Content-Type": "application/json"},
        data=json.dumps(body),
        timeout=30,
    )
    return {"statusCode": r.status_code, "json": safe_json(r), "text": r.text, "requestBody": body}


def microsoft_delete_event_api(access_token: str, event_id: str) -> Dict[str, Any]:
    url = MS_EVENT_URL.format(eventId=safe_event_id(event_id))
    r = requests.delete(url, headers=_graph_headers(access_token), timeout=30)
    return {"statusCode": r.status_code, "text": r.text}


def microsoft_patch_event_time_api(access_token: str, event_id: str, start_utc: datetime, end_utc: datetime) -> Dict[str, Any]:
    body = {
        "start": {"dateTime": start_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"), "timeZone": "UTC"},
        "end": {"dateTime": end_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"), "timeZone": "UTC"},
    }
    url = MS_EVENT_URL.format(eventId=safe_event_id(event_id))
    r = requests.patch(
        url,
        headers={**_graph_headers(access_token), "Content-Type": "application/json"},
        data=json.dumps(body),
        timeout=30,
    )
    return {"statusCode": r.status_code, "json": safe_json(r), "text": r.text, "requestBody": body}


def microsoft_event_time_to_utc(dt_obj: Dict[str, Any]) -> Optional[datetime]:
    if not isinstance(dt_obj, dict):
        return None
    raw = (dt_obj.get("dateTime") or "").strip()
    tz_name = (dt_obj.get("timeZone") or "UTC").strip() or "UTC"
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo(tz_name))
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    try:
        raw2 = raw.split(".")[0]
        dt = datetime.fromisoformat(raw2)
        return dt.replace(tzinfo=ZoneInfo(tz_name)).astimezone(timezone.utc)
    except Exception:
        return None


def microsoft_schedule_item_to_utc(item: Dict[str, Any]) -> Optional[Tuple[datetime, datetime, str]]:
    try:
        s = microsoft_event_time_to_utc(item.get("start") or {})
        e = microsoft_event_time_to_utc(item.get("end") or {})
        status = str(item.get("status") or "").strip().lower()
        if not s or not e or e <= s:
            return None
        return (s, e, status)
    except Exception:
        return None


def microsoft_collect_busy_utc(
    access_token: str,
    tz_name: str,
    calendar_ids: List[str],
    time_min_utc: datetime,
    time_max_utc: datetime,
) -> Dict[str, Any]:
    busy: List[Tuple[datetime, datetime]] = []
    me = microsoft_me(access_token)
    schedule_email = (me.get("mail") or me.get("userPrincipalName") or "").strip()

    sched = None
    if schedule_email:
        sched = microsoft_get_schedule_api(
            access_token=access_token,
            schedule_email=schedule_email,
            time_min_utc=time_min_utc,
            time_max_utc=time_max_utc,
            time_zone=tz_name,
            availability_view_interval=30,
        )
        if sched["statusCode"] == 200 and sched["json"]:
            for info in (sched["json"].get("value") or []):
                for item in (info.get("scheduleItems") or []):
                    parsed = microsoft_schedule_item_to_utc(item)
                    if not parsed:
                        continue
                    s_utc, e_utc, status = parsed
                    if status == "free":
                        continue
                    busy.append((s_utc, e_utc))

    for cid in calendar_ids:
        r = microsoft_calendar_view_api(access_token, cid, time_min_utc, time_max_utc)
        if r["statusCode"] != 200 or not r["json"]:
            continue
        for ev in (r["json"].get("value") or []):
            if ev.get("isCancelled") is True:
                continue
            if str(ev.get("showAs") or "").lower() == "free":
                continue
            s_utc = microsoft_event_time_to_utc(ev.get("start") or {})
            e_utc = microsoft_event_time_to_utc(ev.get("end") or {})
            if not s_utc or not e_utc or e_utc <= s_utc:
                continue
            busy.append((s_utc, e_utc))

    merged = merge_intervals_dt(busy)
    return {
        "ok": True,
        "checkedCalendars": list(calendar_ids),
        "timeMinUtc": iso_z(time_min_utc),
        "timeMaxUtc": iso_z(time_max_utc),
        "busyMerged": [{"startUtc": iso_z(s), "endUtc": iso_z(e)} for s, e in merged],
        "freebusyStatusCode": sched["statusCode"] if sched else None,
        "freebusyRequestBody": sched.get("requestBody") if sched else None,
        "freebusyResponseText": sched.get("text") if sched else None,
    }


def microsoft_collect_busy_utc_excluding_event(
    access_token: str,
    calendar_ids: List[str],
    time_min_utc: datetime,
    time_max_utc: datetime,
    exclude_event_id: str,
) -> List[Tuple[datetime, datetime]]:
    busy: List[Tuple[datetime, datetime]] = []
    for cid in calendar_ids:
        r = microsoft_calendar_view_api(access_token, cid, time_min_utc, time_max_utc)
        if r["statusCode"] != 200 or not r["json"]:
            continue
        for ev in (r["json"].get("value") or []):
            if (ev.get("id") or "") == exclude_event_id:
                continue
            if ev.get("isCancelled") is True:
                continue
            if str(ev.get("showAs") or "").lower() == "free":
                continue
            s_utc = microsoft_event_time_to_utc(ev.get("start") or {})
            e_utc = microsoft_event_time_to_utc(ev.get("end") or {})
            if not s_utc or not e_utc or e_utc <= s_utc:
                continue
            busy.append((s_utc, e_utc))
    return merge_intervals_dt(busy)


def provider_default_calendar_id(provider: str) -> Optional[str]:
    return "primary" if provider == PROVIDER_GOOGLE else None


def build_auth_url(provider: str, state: str) -> str:
    if provider == PROVIDER_GOOGLE:
        if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REDIRECT_URI and APP_BASE_URL):
            raise HTTPException(status_code=500, detail="Missing Google OAuth env vars")
        return build_google_auth_url(state)
    if provider == PROVIDER_MICROSOFT:
        if not (MS_CLIENT_ID and MS_CLIENT_SECRET and MS_REDIRECT_URI and APP_BASE_URL):
            raise HTTPException(status_code=500, detail="Missing Microsoft OAuth env vars")
        return build_microsoft_auth_url(state)
    raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider}")


def exchange_code_for_tokens(provider: str, code: str) -> Dict[str, Any]:
    if provider == PROVIDER_GOOGLE:
        return exchange_google_code_for_tokens(code)
    if provider == PROVIDER_MICROSOFT:
        return exchange_microsoft_code_for_tokens(code)
    raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider}")


def refresh_access_token(provider: str, refresh_token: str) -> str:
    if provider == PROVIDER_GOOGLE:
        return refresh_google_access_token(refresh_token)
    if provider == PROVIDER_MICROSOFT:
        return refresh_microsoft_access_token(refresh_token)
    raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider}")


def fetch_user_email(provider: str, access_token: str) -> str:
    if provider == PROVIDER_GOOGLE:
        return (google_userinfo(access_token) or {}).get("email", "unknown")
    if provider == PROVIDER_MICROSOFT:
        me = microsoft_me(access_token)
        return (me.get("mail") or me.get("userPrincipalName") or "unknown").strip()
    raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider}")


def sync_calendars_from_provider(provider: str, customer_id: str, access_token: str) -> List[Dict[str, Any]]:
    keep: List[Dict[str, Any]] = []
    if provider == PROVIDER_GOOGLE:
        cal_list = google_calendar_list_api(access_token)
        if cal_list["statusCode"] == 200 and cal_list["json"]:
            items = cal_list["json"].get("items") or []
            keep = [{"id": it.get("id"), "summary": it.get("summary"), "primary": bool(it.get("primary", False))} for it in items]
    elif provider == PROVIDER_MICROSOFT:
        cal_list = microsoft_list_calendars_api(access_token)
        if cal_list["statusCode"] == 200 and cal_list["json"]:
            items = cal_list["json"].get("value") or []
            keep = [{"id": it.get("id"), "summary": it.get("name"), "primary": (idx == 0)} for idx, it in enumerate(items)]
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider}")

    if keep:
        upsert_calendars(provider, customer_id, keep)
    return keep


def collect_busy_utc(
    provider: str,
    access_token: str,
    tz_name: str,
    calendar_ids: List[str],
    time_min_utc: datetime,
    time_max_utc: datetime,
) -> Dict[str, Any]:
    if provider == PROVIDER_GOOGLE:
        return google_collect_busy_utc(access_token, tz_name, calendar_ids, time_min_utc, time_max_utc)
    if provider == PROVIDER_MICROSOFT:
        return microsoft_collect_busy_utc(access_token, tz_name, calendar_ids, time_min_utc, time_max_utc)
    raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider}")


def collect_busy_utc_excluding_event(
    provider: str,
    access_token: str,
    calendar_ids: List[str],
    time_min_utc: datetime,
    time_max_utc: datetime,
    exclude_calendar_id: Optional[str],
    exclude_event_id: str,
) -> List[Tuple[datetime, datetime]]:
    if provider == PROVIDER_GOOGLE:
        return google_collect_busy_utc_excluding_event(
            access_token=access_token,
            calendar_ids=calendar_ids,
            time_min_utc=time_min_utc,
            time_max_utc=time_max_utc,
            exclude_calendar_id=exclude_calendar_id or "",
            exclude_event_id=exclude_event_id,
        )
    if provider == PROVIDER_MICROSOFT:
        return microsoft_collect_busy_utc_excluding_event(
            access_token=access_token,
            calendar_ids=calendar_ids,
            time_min_utc=time_min_utc,
            time_max_utc=time_max_utc,
            exclude_event_id=exclude_event_id,
        )
    raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider}")


def compute_availability_from_busy(
    tz_name: str,
    work_start_hour: int,
    work_end_hour: int,
    work_days: List[int],
    merged_busy: List[Tuple[datetime, datetime]],
    duration_minutes: int,
    step_minutes: int,
    days: int,
    preferred_utc: Optional[datetime] = None,
    preference: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz).replace(second=0, microsecond=0)

    start_day = now_local.date()
    end_day = start_day + timedelta(days=max(1, int(days)))
    horizon_end_local = datetime(end_day.year, end_day.month, end_day.day, work_end_hour, 0, tzinfo=tz)

    time_min_utc = now_local.astimezone(timezone.utc)
    time_max_utc = horizon_end_local.astimezone(timezone.utc)

    dur = timedelta(minutes=int(duration_minutes))
    step = timedelta(minutes=int(step_minutes))

    available: List[Dict[str, str]] = []
    busy_ptr = 0

    for day_offset in range(int(days)):
        d = start_day + timedelta(days=day_offset)
        weekday = datetime(d.year, d.month, d.day, 0, 0, tzinfo=tz).weekday()
        if weekday not in work_days:
            continue

        win_start_local = datetime(d.year, d.month, d.day, work_start_hour, 0, tzinfo=tz)
        win_end_local = datetime(d.year, d.month, d.day, work_end_hour, 0, tzinfo=tz)

        if d == now_local.date() and now_local > win_start_local:
            win_start_local = now_local

        if win_end_local <= win_start_local:
            continue

        win_start_utc = win_start_local.astimezone(timezone.utc)
        win_end_utc = win_end_local.astimezone(timezone.utc)

        t = round_up_to_step(win_start_utc, int(step_minutes))
        last_start = win_end_utc - dur

        while t <= last_start:
            slot_start = t
            slot_end = t + dur
            free, busy_ptr = slot_is_free(slot_start, slot_end, merged_busy, busy_ptr)
            if free:
                available.append(
                    {
                        "startUtc": iso_z(slot_start),
                        "endUtc": iso_z(slot_end),
                        "startLocal": format_local(slot_start, tz),
                        "endLocal": format_local(slot_end, tz),
                    }
                )
            t = t + step

    available.sort(key=lambda x: x["startUtc"])
    suggestions = pick_by_preference(available, tz, preference, preferred_utc)

    return {
        "ok": True,
        "timeZone": tz_name,
        "window": {"timeMinUtc": iso_z(time_min_utc), "timeMaxUtc": iso_z(time_max_utc)},
        "busyMergedCount": len(merged_busy),
        "availableCount": len(available),
        "suggestions": suggestions,
        "available": available[:500],
    }


def provider_search_events(
    provider: str,
    access_token: str,
    calendar_ids: List[str],
    tz_name: str,
    email: str,
    phone_digits: str,
    search_attendees: bool,
    time_min: datetime,
    time_max: datetime,
) -> List[Dict[str, Any]]:
    tz = ZoneInfo(tz_name)
    matches: List[Dict[str, Any]] = []

    if provider == PROVIDER_GOOGLE:
        for cid in calendar_ids:
            r = google_list_events_api(access_token, cid, time_min, time_max)
            if r["statusCode"] != 200 or not r["json"]:
                continue
            for ev in (r["json"].get("items") or []):
                if ev.get("status") == "cancelled":
                    continue
                summ = ev.get("summary") or ""
                desc = ev.get("description") or ""
                hay_parts = [summ, desc]
                if search_attendees:
                    for a in (ev.get("attendees") or []):
                        if isinstance(a, dict):
                            ae = (a.get("email") or "").strip().lower()
                            if ae:
                                hay_parts.append(ae)

                hay = "\n".join(hay_parts).lower()
                hay_phone = digits_only(summ + "\n" + desc)

                ok = True
                if email:
                    ok = ok and (email in hay)
                if phone_digits:
                    ok = ok and (phone_digits in hay_phone)
                if not ok:
                    continue

                start_dt = (ev.get("start") or {}).get("dateTime")
                end_dt = (ev.get("end") or {}).get("dateTime")
                if not start_dt or not end_dt:
                    continue
                try:
                    start_utc = parse_iso_to_utc(start_dt)
                    end_utc = parse_iso_to_utc(end_dt)
                except Exception:
                    continue

                matches.append(
                    {
                        "calendarId": cid,
                        "eventId": ev.get("id"),
                        "summary": summ,
                        "startUtc": iso_z(start_utc),
                        "endUtc": iso_z(end_utc),
                        "startLocal": format_local(start_utc, tz),
                        "endLocal": format_local(end_utc, tz),
                    }
                )

    elif provider == PROVIDER_MICROSOFT:
        for cid in calendar_ids:
            r = microsoft_calendar_view_api(access_token, cid, time_min, time_max)
            if r["statusCode"] != 200 or not r["json"]:
                continue
            for ev in (r["json"].get("value") or []):
                if ev.get("isCancelled") is True:
                    continue
                subject = (ev.get("subject") or "").strip()
                body_preview = (ev.get("bodyPreview") or "").strip()
                hay_parts = [subject, body_preview]
                if search_attendees:
                    for a in (ev.get("attendees") or []):
                        if isinstance(a, dict):
                            addr = (((a.get("emailAddress") or {}) if isinstance(a.get("emailAddress"), dict) else {}).get("address") or "").strip().lower()
                            if addr:
                                hay_parts.append(addr)

                hay = "\n".join(hay_parts).lower()
                hay_phone = digits_only("\n".join([subject, body_preview]))

                ok = True
                if email:
                    ok = ok and (email in hay)
                if phone_digits:
                    ok = ok and (phone_digits in hay_phone)
                if not ok:
                    continue

                s_utc = microsoft_event_time_to_utc(ev.get("start") or {})
                e_utc = microsoft_event_time_to_utc(ev.get("end") or {})
                if not s_utc or not e_utc:
                    continue

                matches.append(
                    {
                        "calendarId": cid,
                        "eventId": ev.get("id"),
                        "summary": subject,
                        "startUtc": iso_z(s_utc),
                        "endUtc": iso_z(e_utc),
                        "startLocal": format_local(s_utc, tz),
                        "endLocal": format_local(e_utc, tz),
                    }
                )
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider}")

    matches.sort(key=lambda x: x["startUtc"])
    return matches


def provider_create_event(
    provider: str,
    access_token: str,
    calendar_id: str,
    summary: str,
    description: str,
    start_utc: datetime,
    end_utc: datetime,
    tz_name: str,
    attendees: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    if provider == PROVIDER_GOOGLE:
        return google_create_event_api(access_token, calendar_id, summary, description, start_utc, end_utc, tz_name, attendees)
    if provider == PROVIDER_MICROSOFT:
        return microsoft_create_event_api(access_token, calendar_id, summary, description, start_utc, end_utc, attendees)
    raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider}")


def provider_delete_event(provider: str, access_token: str, calendar_id: str, event_id: str) -> Dict[str, Any]:
    if provider == PROVIDER_GOOGLE:
        return google_delete_event_api(access_token, calendar_id, event_id)
    if provider == PROVIDER_MICROSOFT:
        return microsoft_delete_event_api(access_token, event_id)
    raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider}")


def provider_patch_event_time(
    provider: str,
    access_token: str,
    calendar_id: str,
    event_id: str,
    start_utc: datetime,
    end_utc: datetime,
    tz_name: str,
) -> Dict[str, Any]:
    if provider == PROVIDER_GOOGLE:
        return google_patch_event_time_api(access_token, calendar_id, event_id, start_utc, end_utc, tz_name)
    if provider == PROVIDER_MICROSOFT:
        return microsoft_patch_event_time_api(access_token, event_id, start_utc, end_utc)
    raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider}")


def oauth_start_handler(provider: str, customer_id: str):
    provider = validate_provider(provider)
    state = secrets.token_urlsafe(24)
    upsert_oauth_state(state, customer_id, provider)
    return RedirectResponse(build_auth_url(provider, state))


def oauth_callback_handler(code: str, state: str, error: str):
    if error:
        return JSONResponse({"connected": False, "error": error}, status_code=400)
    if not code or not state:
        return JSONResponse({"connected": False, "error": "Missing code/state"}, status_code=400)

    state_data = consume_oauth_state(state)
    if not state_data:
        return JSONResponse({"connected": False, "error": "Invalid/expired state"}, status_code=400)

    customer_id = state_data["customer_id"]
    provider = validate_provider(state_data["provider"])

    tok = exchange_code_for_tokens(provider, code)
    if tok["status"] != 200 or not tok["json"]:
        return JSONResponse(
            {"connected": False, "provider": provider, "error": "Token exchange failed", "responseText": tok["text"]},
            status_code=500,
        )

    tokens = tok["json"]
    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    scope = tokens.get("scope", "")
    token_type = tokens.get("token_type", "")

    if not access_token or not refresh_token:
        return JSONResponse(
            {"connected": False, "provider": provider, "error": "Missing access_token or refresh_token"},
            status_code=500,
        )

    email = fetch_user_email(provider, access_token)
    save_oauth_token(provider, customer_id, email, refresh_token, scope, token_type)
    ensure_customer_settings(customer_id)
    sync_calendars_from_provider(provider, customer_id, access_token)

    return JSONResponse(
        {
            "connected": True,
            "provider": provider,
            "customerId": customer_id,
            "email": email,
            "message": f"{provider.capitalize()} connected.",
        }
    )


def calendars_handler(provider: str, request: Request, customer_id: str):
    require_api_key(request)
    provider = validate_provider(provider)
    rt = load_refresh_token(provider, customer_id)
    access_token = refresh_access_token(provider, rt)
    sync_calendars_from_provider(provider, customer_id, access_token)
    return {"customerId": customer_id, "provider": provider, "calendars": list_calendars_db(provider, customer_id)}


def calendars_select_handler(provider: str, request: Request, payload: Dict[str, Any]):
    require_api_key(request)
    provider = validate_provider(provider)

    customer_id = payload.get("customerId")
    calendar_ids = payload.get("calendarIds") or []
    if not customer_id:
        raise HTTPException(status_code=400, detail="customerId required")
    if not isinstance(calendar_ids, list) or not calendar_ids:
        raise HTTPException(status_code=400, detail="calendarIds must be a non-empty list")

    set_selected_calendars(provider, customer_id, calendar_ids)
    return {
        "ok": True,
        "customerId": customer_id,
        "provider": provider,
        "selected": selected_calendar_ids(provider, customer_id, provider_default_calendar_id(provider)),
    }


def settings_handler(request: Request, payload: Dict[str, Any]):
    require_api_key(request)
    customer_id = payload.get("customerId")
    if not customer_id:
        raise HTTPException(status_code=400, detail="customerId required")

    settings = ensure_customer_settings(customer_id)
    tz_name = payload.get("timeZone") or settings["timezone"]
    ws = int(payload.get("workStartHour", settings["work_start_hour"]))
    we = int(payload.get("workEndHour", settings["work_end_hour"]))
    wd = normalize_work_days(payload.get("workDays", settings["work_days"]))
    updated = update_customer_settings(customer_id, tz_name, ws, we, wd)
    return {"ok": True, "customerId": customer_id, "settings": updated}


def freebusy_handler(provider: str, request: Request, payload: Dict[str, Any]):
    require_api_key(request)
    provider = validate_provider(provider)

    customer_id = payload.get("customerId")
    if not customer_id:
        raise HTTPException(status_code=400, detail="customerId required")

    settings = ensure_customer_settings(customer_id)
    tz_name = payload.get("timeZone") or settings["timezone"]

    try:
        time_min = parse_iso_to_utc(payload.get("timeMinUtc"))
        time_max = parse_iso_to_utc(payload.get("timeMaxUtc"))
    except Exception:
        raise HTTPException(status_code=400, detail="timeMinUtc and timeMaxUtc required and must be ISO (Z or offset)")

    cal_ids = payload.get("calendarIds")
    calendar_ids = cal_ids if isinstance(cal_ids, list) and cal_ids else selected_calendar_ids(provider, customer_id, provider_default_calendar_id(provider))
    calendar_ids = [c for c in calendar_ids if c]
    if not calendar_ids:
        raise HTTPException(status_code=400, detail=f"No {provider} calendars selected/found")

    rt = load_refresh_token(provider, customer_id)
    access_token = refresh_access_token(provider, rt)

    pack = collect_busy_utc(provider, access_token, tz_name, calendar_ids, time_min, time_max)
    pack["isFree"] = len(pack.get("busyMerged", [])) == 0
    pack["status"] = "free" if pack["isFree"] else "busy"
    return pack


def check_availability_handler(provider: str, request: Request, payload: Dict[str, Any]):
    require_api_key(request)
    provider = validate_provider(provider)

    customer_id = payload.get("customerId")
    if not customer_id:
        raise HTTPException(status_code=400, detail="customerId required")

    settings = ensure_customer_settings(customer_id)
    tz_name = payload.get("timeZone") or settings["timezone"]

    start_raw = payload.get("startUtc") or payload.get("timeMinUtc")
    end_raw = payload.get("endUtc") or payload.get("timeMaxUtc")
    if not start_raw or not end_raw:
        raise HTTPException(status_code=400, detail="startUtc/endUtc required")

    try:
        start_utc = parse_iso_to_utc(start_raw)
        end_utc = parse_iso_to_utc(end_raw)
    except Exception:
        raise HTTPException(status_code=400, detail="startUtc/endUtc must be ISO with Z or offset")

    if end_utc <= start_utc:
        raise HTTPException(status_code=400, detail="endUtc must be after startUtc")

    cal_ids = payload.get("calendarIds")
    calendar_ids = cal_ids if isinstance(cal_ids, list) and cal_ids else selected_calendar_ids(provider, customer_id, provider_default_calendar_id(provider))
    calendar_ids = [c for c in calendar_ids if c]
    if not calendar_ids:
        raise HTTPException(status_code=400, detail=f"No {provider} calendars selected/found")

    rt = load_refresh_token(provider, customer_id)
    access_token = refresh_access_token(provider, rt)

    time_min = start_utc - timedelta(minutes=1)
    time_max = end_utc + timedelta(minutes=1)

    busy_pack = collect_busy_utc(provider, access_token, tz_name, calendar_ids, time_min, time_max)
    merged_busy = [(parse_iso_to_utc(x["startUtc"]), parse_iso_to_utc(x["endUtc"])) for x in busy_pack["busyMerged"]]
    merged_busy = merge_intervals_dt(merged_busy)

    overlaps_found = [{"startUtc": iso_z(bs), "endUtc": iso_z(be)} for bs, be in merged_busy if overlaps(start_utc, end_utc, bs, be)]
    is_free = len(overlaps_found) == 0
    tz = ZoneInfo(tz_name)

    return {
        "ok": True,
        "provider": provider,
        "customerId": customer_id,
        "timeZone": tz_name,
        "startUtc": iso_z(start_utc),
        "endUtc": iso_z(end_utc),
        "startLocal": format_local(start_utc, tz),
        "endLocal": format_local(end_utc, tz),
        "calendarIdsUsed": calendar_ids,
        "isFree": is_free,
        "status": "free" if is_free else "busy",
        "busyMerged": busy_pack["busyMerged"],
        "overlaps": overlaps_found,
    }


def availability_handler(provider: str, request: Request, payload: Dict[str, Any]):
    require_api_key(request)
    provider = validate_provider(provider)

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
    calendar_ids = cal_ids if isinstance(cal_ids, list) and cal_ids else selected_calendar_ids(provider, customer_id, provider_default_calendar_id(provider))
    calendar_ids = [c for c in calendar_ids if c]
    if not calendar_ids:
        raise HTTPException(status_code=400, detail=f"No {provider} calendars selected/found")

    preferred_raw = payload.get("preferredDateTimeUtc")
    preferred_utc = None
    if preferred_raw:
        try:
            preferred_utc = parse_iso_to_utc(preferred_raw)
        except Exception:
            return {"ok": False, "reason": "invalid_preferredDateTimeUtc", "message": "preferredDateTimeUtc must be ISO with Z or offset"}

    preference = payload.get("preference")
    if preference is not None and not isinstance(preference, dict):
        preference = None

    rt = load_refresh_token(provider, customer_id)
    access_token = refresh_access_token(provider, rt)

    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz).replace(second=0, microsecond=0)
    start_day = now_local.date()
    end_day = start_day + timedelta(days=max(1, days))
    horizon_end_local = datetime(end_day.year, end_day.month, end_day.day, work_end, 0, tzinfo=tz)
    time_min_utc = now_local.astimezone(timezone.utc)
    time_max_utc = horizon_end_local.astimezone(timezone.utc)

    busy_pack = collect_busy_utc(provider, access_token, tz_name, calendar_ids, time_min_utc, time_max_utc)
    merged_busy = [(parse_iso_to_utc(x["startUtc"]), parse_iso_to_utc(x["endUtc"])) for x in busy_pack["busyMerged"]]
    merged_busy = merge_intervals_dt(merged_busy)

    out = compute_availability_from_busy(
        tz_name=tz_name,
        work_start_hour=work_start,
        work_end_hour=work_end,
        work_days=work_days,
        merged_busy=merged_busy,
        duration_minutes=duration,
        step_minutes=step,
        days=days,
        preferred_utc=preferred_utc,
        preference=preference,
    )
    out["provider"] = provider
    out["calendarIdsUsed"] = calendar_ids
    out["busyMerged"] = busy_pack["busyMerged"]
    return out


def create_event_handler(provider: str, request: Request, payload: Dict[str, Any]):
    require_api_key(request)
    provider = validate_provider(provider)

    customer_id = payload.get("customerId")
    if not customer_id:
        raise HTTPException(status_code=400, detail="customerId required")

    settings = ensure_customer_settings(customer_id)
    tz_name = payload.get("timeZone") or settings["timezone"]

    calendar_id = (payload.get("calendarId") or "").strip()
    if not calendar_id:
        sel = selected_calendar_ids(provider, customer_id, provider_default_calendar_id(provider))
        sel = [c for c in sel if c]
        if not sel:
            raise HTTPException(status_code=400, detail=f"No {provider} calendarId provided and none selected")
        calendar_id = sel[0]

    summary = (payload.get("summary") or "").strip() or "Appointment"
    description = (payload.get("description") or "").strip()

    attendees = payload.get("attendees")
    if attendees is not None and not isinstance(attendees, list):
        attendees = None

    start_obj = payload.get("start") or {}
    end_obj = payload.get("end") or {}
    raw_start = (start_obj.get("dateTime") or "").strip()
    raw_end = (end_obj.get("dateTime") or "").strip()

    try:
        start_utc = parse_any_datetime_to_utc(raw_start, tz_name)
        end_utc = parse_any_datetime_to_utc(raw_end, tz_name)
    except Exception as e:
        return {
            "booked": False,
            "reason": "invalid_datetime",
            "message": "start.dateTime and end.dateTime must be valid ISO datetimes (Z/offset) or naive ISO assumed in timeZone",
            "error": repr(e),
        }

    if end_utc <= start_utc:
        return {"booked": False, "reason": "invalid_range", "message": "end must be after start"}

    rt = load_refresh_token(provider, customer_id)
    access_token = refresh_access_token(provider, rt)

    calendars_to_check = selected_calendar_ids(provider, customer_id, provider_default_calendar_id(provider))
    calendars_to_check = [c for c in calendars_to_check if c]
    if calendar_id not in calendars_to_check:
        calendars_to_check = [calendar_id] + calendars_to_check

    time_min = start_utc - timedelta(minutes=1)
    time_max = end_utc + timedelta(minutes=1)

    busy_pack = collect_busy_utc(provider, access_token, tz_name, calendars_to_check, time_min, time_max)
    merged_busy = [(parse_iso_to_utc(x["startUtc"]), parse_iso_to_utc(x["endUtc"])) for x in busy_pack["busyMerged"]]
    merged_busy = merge_intervals_dt(merged_busy)

    for bs, be in merged_busy:
        if overlaps(start_utc, end_utc, bs, be):
            return {
                "booked": False,
                "reason": "slot_taken",
                "message": "That time is already booked. Please pick another slot.",
                "checkedCalendars": calendars_to_check,
                "busyMerged": [{"startUtc": iso_z(bs), "endUtc": iso_z(be)} for bs, be in merged_busy],
            }

    created = provider_create_event(
        provider=provider,
        access_token=access_token,
        calendar_id=calendar_id,
        summary=summary,
        description=description,
        start_utc=start_utc,
        end_utc=end_utc,
        tz_name=tz_name,
        attendees=attendees,
    )
    if created["statusCode"] not in (200, 201):
        return {
            "booked": False,
            "reason": f"{provider}_create_failed",
            "message": f"{provider.capitalize()} rejected the create event request",
            "statusCode": created["statusCode"],
            "providerResponseText": created["text"],
            "requestBody": created["requestBody"],
        }

    return {
        "booked": True,
        "provider": provider,
        "calendarId": calendar_id,
        "event": created.get("json"),
        "startUtc": iso_z(start_utc),
        "endUtc": iso_z(end_utc),
    }


def search_events_handler(provider: str, request: Request, payload: Dict[str, Any]):
    require_api_key(request)
    provider = validate_provider(provider)

    customer_id = (payload.get("customerId") or "").strip()
    if not customer_id:
        raise HTTPException(status_code=400, detail="customerId required")

    settings = ensure_customer_settings(customer_id)
    tz_name = payload.get("timeZone") or settings["timezone"]

    cal_ids = payload.get("calendarIds")
    calendar_ids = cal_ids if isinstance(cal_ids, list) and cal_ids else selected_calendar_ids(provider, customer_id, provider_default_calendar_id(provider))
    calendar_ids = [c for c in calendar_ids if c]
    if not calendar_ids:
        raise HTTPException(status_code=400, detail=f"No {provider} calendars selected/found")

    email = (payload.get("email") or "").strip().lower()
    phone_digits = digits_only((payload.get("phone") or "").strip())
    if not email and not phone_digits:
        raise HTTPException(status_code=400, detail="Provide email and/or phone")

    search_attendees = payload.get("searchAttendees")
    if not isinstance(search_attendees, bool):
        search_attendees = True

    now_utc = datetime.now(timezone.utc)
    default_min = now_utc - timedelta(days=365)
    default_max = now_utc + timedelta(days=365)

    try:
        time_min = parse_iso_to_utc(payload.get("timeMinUtc")) if payload.get("timeMinUtc") else default_min
        time_max = parse_iso_to_utc(payload.get("timeMaxUtc")) if payload.get("timeMaxUtc") else default_max
    except Exception:
        raise HTTPException(status_code=400, detail="timeMinUtc/timeMaxUtc must be ISO with Z or offset")

    time_min -= timedelta(minutes=1)
    time_max += timedelta(minutes=1)

    rt = load_refresh_token(provider, customer_id)
    access_token = refresh_access_token(provider, rt)

    matches = provider_search_events(
        provider=provider,
        access_token=access_token,
        calendar_ids=calendar_ids,
        tz_name=tz_name,
        email=email,
        phone_digits=phone_digits,
        search_attendees=search_attendees,
        time_min=time_min,
        time_max=time_max,
    )
    return {"ok": True, "provider": provider, "count": len(matches), "matches": matches}


def cancel_events_handler(provider: str, request: Request, payload: Dict[str, Any]):
    require_api_key(request)
    provider = validate_provider(provider)

    customer_id = (payload.get("customerId") or "").strip()
    if not customer_id:
        raise HTTPException(status_code=400, detail="customerId required")

    items = payload.get("items")
    if not isinstance(items, list):
        raise HTTPException(status_code=400, detail="items must be a list")

    rt = load_refresh_token(provider, customer_id)
    access_token = refresh_access_token(provider, rt)

    results = []
    for it in items:
        if not isinstance(it, dict):
            continue
        cal_id = (it.get("calendarId") or "").strip()
        ev_id = (it.get("eventId") or "").strip()
        if not ev_id:
            continue
        if provider == PROVIDER_GOOGLE and not cal_id:
            continue

        r = provider_delete_event(provider, access_token, cal_id, ev_id)
        ok = r["statusCode"] in (200, 202, 204)
        row = {"eventId": ev_id, "cancelled": ok, "statusCode": r["statusCode"], "providerText": r["text"]}
        if cal_id:
            row["calendarId"] = cal_id
        results.append(row)

    return {"ok": True, "provider": provider, "requested": len(items), "processed": len(results), "results": results}


def reschedule_events_handler(provider: str, request: Request, payload: Dict[str, Any]):
    require_api_key(request)
    provider = validate_provider(provider)

    customer_id = (payload.get("customerId") or "").strip()
    if not customer_id:
        raise HTTPException(status_code=400, detail="customerId required")

    settings = ensure_customer_settings(customer_id)
    tz_name = payload.get("timeZone") or settings["timezone"]

    items = payload.get("items")
    if not isinstance(items, list):
        raise HTTPException(status_code=400, detail="items must be a list")

    rt = load_refresh_token(provider, customer_id)
    access_token = refresh_access_token(provider, rt)

    calendars_to_check = selected_calendar_ids(provider, customer_id, provider_default_calendar_id(provider))
    calendars_to_check = [c for c in calendars_to_check if c]
    if not calendars_to_check:
        raise HTTPException(status_code=400, detail=f"No {provider} calendars selected/found")

    results = []
    for it in items:
        if not isinstance(it, dict):
            continue

        cal_id = (it.get("calendarId") or "").strip()
        ev_id = (it.get("eventId") or "").strip()
        if not ev_id:
            continue
        if provider == PROVIDER_GOOGLE and not cal_id:
            continue

        start_obj = it.get("start") or {}
        end_obj = it.get("end") or {}
        raw_start = (start_obj.get("dateTime") or "").strip()
        raw_end = (end_obj.get("dateTime") or "").strip()
        if not raw_start or not raw_end:
            continue

        try:
            new_start = parse_any_datetime_to_utc(raw_start, tz_name)
            new_end = parse_any_datetime_to_utc(raw_end, tz_name)
        except Exception as e:
            results.append({"eventId": ev_id, "rescheduled": False, "reason": "invalid_datetime", "error": repr(e)})
            continue

        if new_end <= new_start:
            results.append({"eventId": ev_id, "rescheduled": False, "reason": "invalid_range"})
            continue

        time_min = new_start - timedelta(minutes=1)
        time_max = new_end + timedelta(minutes=1)

        busy_merged = collect_busy_utc_excluding_event(
            provider=provider,
            access_token=access_token,
            calendar_ids=([cal_id] + calendars_to_check) if (provider == PROVIDER_GOOGLE and cal_id and cal_id not in calendars_to_check) else calendars_to_check,
            time_min_utc=time_min,
            time_max_utc=time_max,
            exclude_calendar_id=cal_id if provider == PROVIDER_GOOGLE else None,
            exclude_event_id=ev_id,
        )

        taken = any(overlaps(new_start, new_end, bs, be) for bs, be in busy_merged)
        if taken:
            results.append({"eventId": ev_id, "rescheduled": False, "reason": "slot_taken", "message": "That time is already booked. Please pick another slot."})
            continue

        patched = provider_patch_event_time(
            provider=provider,
            access_token=access_token,
            calendar_id=cal_id,
            event_id=ev_id,
            start_utc=new_start,
            end_utc=new_end,
            tz_name=tz_name,
        )
        ok = patched["statusCode"] in (200,)
        row = {
            "eventId": ev_id,
            "rescheduled": ok,
            "statusCode": patched["statusCode"],
            "event": patched.get("json"),
            "providerText": patched.get("text"),
        }
        if cal_id:
            row["calendarId"] = cal_id
        results.append(row)

    return {"ok": True, "provider": provider, "requested": len(items), "processed": len(results), "results": results}


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/debug/db")
def debug_db(request: Request):
    require_debug_key(request)
    with engine.begin() as conn:
        v = conn.execute(text("SELECT 1")).scalar_one()
    return {"db_ok": True, "select_1": v}


@app.get("/debug/schema")
def debug_schema(request: Request):
    require_debug_key(request)
    out: Dict[str, Any] = {"tables": []}
    with engine.begin() as conn:
        tables = conn.execute(
            text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='public'
                ORDER BY table_name
            """)
        ).fetchall()
        out["tables"] = [t[0] for t in tables]

        for tname in out["tables"]:
            cols = conn.execute(
                text("""
                    SELECT column_name AS name, data_type AS type
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=:t
                    ORDER BY ordinal_position
                """),
                {"t": tname},
            ).fetchall()
            out[tname] = [{"name": c[0], "type": c[1]} for c in cols]
    return out


@app.get("/oauth/{provider}/start")
def oauth_start(provider: str, customerId: str = Query(...)):
    return oauth_start_handler(provider, customerId)


@app.get("/oauth/{provider}/callback")
def oauth_callback(provider: str, code: str = "", state: str = "", error: str = ""):
    validate_provider(provider)
    return oauth_callback_handler(code, state, error)


@app.get("/{provider}/calendars")
def calendars(provider: str, request: Request, customerId: str):
    return calendars_handler(provider, request, customerId)


@app.post("/{provider}/calendars/select")
async def calendars_select(provider: str, request: Request, payload: Dict[str, Any]):
    return calendars_select_handler(provider, request, payload)


@app.post("/{provider}/settings")
async def provider_settings(provider: str, request: Request, payload: Dict[str, Any]):
    validate_provider(provider)
    return settings_handler(request, payload)


@app.post("/{provider}/freebusy")
async def freebusy(provider: str, request: Request, payload: Dict[str, Any]):
    return freebusy_handler(provider, request, payload)


@app.post("/{provider}/check_availability")
async def check_availability(provider: str, request: Request, payload: Dict[str, Any]):
    return check_availability_handler(provider, request, payload)


@app.post("/{provider}/availability")
async def availability(provider: str, request: Request, payload: Dict[str, Any]):
    return availability_handler(provider, request, payload)


@app.post("/{provider}/create_event")
async def create_event(provider: str, request: Request, payload: Dict[str, Any]):
    return create_event_handler(provider, request, payload)


@app.post("/{provider}/search_events")
async def search_events(provider: str, request: Request, payload: Dict[str, Any]):
    return search_events_handler(provider, request, payload)


@app.post("/{provider}/cancel_events")
async def cancel_events(provider: str, request: Request, payload: Dict[str, Any]):
    return cancel_events_handler(provider, request, payload)


@app.post("/{provider}/reschedule_events")
async def reschedule_events(provider: str, request: Request, payload: Dict[str, Any]):
    return reschedule_events_handler(provider, request, payload)


@app.get("/oauth/google/start")
def oauth_google_start(customerId: str = Query(...)):
    return oauth_start_handler(PROVIDER_GOOGLE, customerId)


@app.get("/oauth/google/callback")
def oauth_google_callback(code: str = "", state: str = "", error: str = ""):
    return oauth_callback_handler(code, state, error)


@app.get("/oauth/microsoft/start")
def oauth_microsoft_start(customerId: str = Query(...)):
    return oauth_start_handler(PROVIDER_MICROSOFT, customerId)


@app.get("/oauth/microsoft/callback")
def oauth_microsoft_callback(code: str = "", state: str = "", error: str = ""):
    return oauth_callback_handler(code, state, error)


@app.get("/google/calendars")
def google_calendars(request: Request, customerId: str):
    return calendars_handler(PROVIDER_GOOGLE, request, customerId)


@app.post("/google/calendars/select")
async def google_calendars_select(request: Request, payload: Dict[str, Any]):
    return calendars_select_handler(PROVIDER_GOOGLE, request, payload)


@app.post("/google/settings")
async def google_settings(request: Request, payload: Dict[str, Any]):
    return settings_handler(request, payload)


@app.post("/google/freebusy")
async def google_freebusy(request: Request, payload: Dict[str, Any]):
    return freebusy_handler(PROVIDER_GOOGLE, request, payload)


@app.post("/google/check_availability")
async def google_check_availability(request: Request, payload: Dict[str, Any]):
    return check_availability_handler(PROVIDER_GOOGLE, request, payload)


@app.post("/google/availability")
async def google_availability(request: Request, payload: Dict[str, Any]):
    return availability_handler(PROVIDER_GOOGLE, request, payload)


@app.post("/google/create_event")
async def google_create_event(request: Request, payload: Dict[str, Any]):
    return create_event_handler(PROVIDER_GOOGLE, request, payload)


@app.post("/google/search_events")
async def google_search_events(request: Request, payload: Dict[str, Any]):
    return search_events_handler(PROVIDER_GOOGLE, request, payload)


@app.post("/google/cancel_events")
async def google_cancel_events(request: Request, payload: Dict[str, Any]):
    return cancel_events_handler(PROVIDER_GOOGLE, request, payload)


@app.post("/google/reschedule_events")
async def google_reschedule_events(request: Request, payload: Dict[str, Any]):
    return reschedule_events_handler(PROVIDER_GOOGLE, request, payload)


@app.get("/microsoft/calendars")
def microsoft_calendars(request: Request, customerId: str):
    return calendars_handler(PROVIDER_MICROSOFT, request, customerId)


@app.post("/microsoft/calendars/select")
async def microsoft_calendars_select(request: Request, payload: Dict[str, Any]):
    return calendars_select_handler(PROVIDER_MICROSOFT, request, payload)


@app.post("/microsoft/settings")
async def microsoft_settings(request: Request, payload: Dict[str, Any]):
    return settings_handler(request, payload)


@app.post("/microsoft/freebusy")
async def microsoft_freebusy(request: Request, payload: Dict[str, Any]):
    return freebusy_handler(PROVIDER_MICROSOFT, request, payload)


@app.post("/microsoft/check_availability")
async def microsoft_check_availability(request: Request, payload: Dict[str, Any]):
    return check_availability_handler(PROVIDER_MICROSOFT, request, payload)


@app.post("/microsoft/availability")
async def microsoft_availability(request: Request, payload: Dict[str, Any]):
    return availability_handler(PROVIDER_MICROSOFT, request, payload)


@app.post("/microsoft/create_event")
async def microsoft_create_event(request: Request, payload: Dict[str, Any]):
    return create_event_handler(PROVIDER_MICROSOFT, request, payload)


@app.post("/microsoft/search_events")
async def microsoft_search_events(request: Request, payload: Dict[str, Any]):
    return search_events_handler(PROVIDER_MICROSOFT, request, payload)


@app.post("/microsoft/cancel_events")
async def microsoft_cancel_events(request: Request, payload: Dict[str, Any]):
    return cancel_events_handler(PROVIDER_MICROSOFT, request, payload)


@app.post("/microsoft/reschedule_events")
async def microsoft_reschedule_events(request: Request, payload: Dict[str, Any]):
    return reschedule_events_handler(PROVIDER_MICROSOFT, request, payload)
