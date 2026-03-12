
from __future__ import annotations

import calendar as pycalendar
import json
import os
import re
import secrets
import urllib.parse
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


app = FastAPI(title="Calendar Backend", version="4.1.0")


# =============================================================================
# Env
# =============================================================================
APP_BASE_URL = (os.environ.get("APP_BASE_URL") or "").strip()

API_KEY = (os.environ.get("API_KEY") or "").strip()
DEBUG_API_KEY = (os.environ.get("DEBUG_API_KEY") or "").strip()

# Google
GOOGLE_CLIENT_ID = (os.environ.get("GOOGLE_CLIENT_ID") or "").strip()
GOOGLE_CLIENT_SECRET = (os.environ.get("GOOGLE_CLIENT_SECRET") or "").strip()
GOOGLE_REDIRECT_URI = (os.environ.get("GOOGLE_REDIRECT_URI") or "").strip()

# Microsoft
MS_CLIENT_ID = (os.environ.get("MS_CLIENT_ID") or "").strip()
MS_CLIENT_SECRET = (os.environ.get("MS_CLIENT_SECRET") or "").strip()
MS_REDIRECT_URI = (os.environ.get("MS_REDIRECT_URI") or "").strip()
MS_TENANT = (os.environ.get("MS_TENANT") or "common").strip()


# =============================================================================
# Provider constants
# =============================================================================
PROVIDER_GOOGLE = "google"
PROVIDER_MICROSOFT = "microsoft"
SUPPORTED_PROVIDERS = {PROVIDER_GOOGLE, PROVIDER_MICROSOFT}


# =============================================================================
# Google constants
# =============================================================================
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


# =============================================================================
# Microsoft constants
# =============================================================================
MS_AUTHORIZE_URL = f"https://login.microsoftonline.com/{MS_TENANT}/oauth2/v2.0/authorize"
MS_TOKEN_URL = f"https://login.microsoftonline.com/{MS_TENANT}/oauth2/v2.0/token"

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
MS_ME_URL = f"{GRAPH_BASE}/me"
MS_LIST_CALENDARS_URL = f"{GRAPH_BASE}/me/calendars"
MS_CALENDARVIEW_URL = f"{GRAPH_BASE}/me/calendars/{{calendarId}}/calendarView"
MS_CREATE_EVENT_URL = f"{GRAPH_BASE}/me/calendars/{{calendarId}}/events"
MS_EVENT_URL = f"{GRAPH_BASE}/me/events/{{eventId}}"

MS_SCOPES = [
    "offline_access",
    "openid",
    "profile",
    "email",
    "https://graph.microsoft.com/User.Read",
    "https://graph.microsoft.com/Calendars.ReadWrite",
]


# =============================================================================
# Windows -> IANA mapping
# =============================================================================
WINDOWS_TZ_TO_IANA: Dict[str, str] = {
    "UTC": "UTC",
    "Mountain Standard Time": "America/Denver",
    "US Mountain Standard Time": "America/Phoenix",
    "Central Standard Time": "America/Chicago",
    "Eastern Standard Time": "America/New_York",
    "Pacific Standard Time": "America/Los_Angeles",
    "Alaskan Standard Time": "America/Anchorage",
    "Hawaiian Standard Time": "Pacific/Honolulu",
    "W. Europe Standard Time": "Europe/Berlin",
    "Romance Standard Time": "Europe/Paris",
    "GMT Standard Time": "Europe/London",
    "Greenwich Standard Time": "Atlantic/Reykjavik",
    "Central Europe Standard Time": "Europe/Budapest",
    "Central European Standard Time": "Europe/Warsaw",
    "E. Europe Standard Time": "Europe/Chisinau",
    "FLE Standard Time": "Europe/Kyiv",
    "GTB Standard Time": "Europe/Bucharest",
    "Turkey Standard Time": "Europe/Istanbul",
    "Russian Standard Time": "Europe/Moscow",
    "Arabian Standard Time": "Asia/Dubai",
    "Arab Standard Time": "Asia/Riyadh",
    "India Standard Time": "Asia/Kolkata",
    "Pakistan Standard Time": "Asia/Karachi",
    "Bangladesh Standard Time": "Asia/Dhaka",
    "SE Asia Standard Time": "Asia/Bangkok",
    "China Standard Time": "Asia/Shanghai",
    "Singapore Standard Time": "Asia/Singapore",
    "Taipei Standard Time": "Asia/Taipei",
    "Tokyo Standard Time": "Asia/Tokyo",
    "Korea Standard Time": "Asia/Seoul",
    "AUS Eastern Standard Time": "Australia/Sydney",
    "E. Australia Standard Time": "Australia/Brisbane",
    "Tasmania Standard Time": "Australia/Hobart",
    "New Zealand Standard Time": "Pacific/Auckland",
}


# =============================================================================
# API key helpers
# =============================================================================
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


# =============================================================================
# Timezone helpers
# =============================================================================
def normalize_tz_name_for_zoneinfo(tz_name: str, fallback: str = "UTC") -> str:
    raw = (tz_name or "").strip()
    if not raw:
        return fallback
    return WINDOWS_TZ_TO_IANA.get(raw, raw)


def zoneinfo_from_any_tz(tz_name: str, fallback: str = "UTC") -> ZoneInfo:
    norm = normalize_tz_name_for_zoneinfo(tz_name, fallback=fallback)
    try:
        return ZoneInfo(norm)
    except Exception:
        return ZoneInfo(fallback)


# =============================================================================
# DB
# =============================================================================
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

    CREATE TABLE IF NOT EXISTS customer_blackout_dates (
      customer_id TEXT NOT NULL,
      date_value  DATE NOT NULL,
      label       TEXT,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (customer_id, date_value)
    );

    CREATE TABLE IF NOT EXISTS customer_holiday_calendars (
      customer_id   TEXT NOT NULL,
      calendar_name TEXT NOT NULL,
      config_json   TEXT NOT NULL,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (customer_id, calendar_name)
    );

    CREATE TABLE IF NOT EXISTS slot_holds (
      hold_token  TEXT PRIMARY KEY,
      provider    TEXT NOT NULL,
      customer_id TEXT NOT NULL,
      calendar_id TEXT,
      start_utc   TIMESTAMPTZ NOT NULL,
      end_utc     TIMESTAMPTZ NOT NULL,
      expires_at  TIMESTAMPTZ NOT NULL,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
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


# =============================================================================
# General helpers
# =============================================================================
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
    tz = zoneinfo_from_any_tz(tz_name, fallback="UTC")
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


# =============================================================================
# Holiday helpers
# =============================================================================
WEEKDAY_NAME_TO_INT = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}


def nth_weekday_of_month(year: int, month: int, weekday: int, nth: int) -> Optional[date]:
    if nth < 1:
        return None
    first_weekday, days_in_month = pycalendar.monthrange(year, month)
    offset = (weekday - first_weekday) % 7
    day_num = 1 + offset + (nth - 1) * 7
    if day_num > days_in_month:
        return None
    return date(year, month, day_num)


def validate_holiday_rule(rule: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(rule, dict):
        raise HTTPException(status_code=400, detail="Each holiday rule must be an object")

    name = (rule.get("name") or "").strip()
    rule_type = (rule.get("type") or "").strip().lower()
    if not name:
        raise HTTPException(status_code=400, detail="Holiday rule name is required")
    if rule_type not in ("fixed", "nth_weekday"):
        raise HTTPException(status_code=400, detail="Holiday rule type must be fixed or nth_weekday")

    month = int(rule.get("month"))
    if not (1 <= month <= 12):
        raise HTTPException(status_code=400, detail="Holiday month must be 1-12")

    out: Dict[str, Any] = {"name": name, "type": rule_type, "month": month}

    if rule_type == "fixed":
        day = int(rule.get("day"))
        if not (1 <= day <= 31):
            raise HTTPException(status_code=400, detail="Holiday day must be 1-31")
        out["day"] = day

    if rule_type == "nth_weekday":
        weekday_name = (rule.get("weekday") or "").strip().lower()
        if weekday_name not in WEEKDAY_NAME_TO_INT:
            raise HTTPException(status_code=400, detail="Holiday weekday is invalid")
        nth = int(rule.get("nth"))
        if not (1 <= nth <= 5):
            raise HTTPException(status_code=400, detail="Holiday nth must be 1-5")
        out["weekday"] = weekday_name
        out["nth"] = nth

    return out


def validate_holiday_calendar_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    calendars = payload.get("calendars")
    if not isinstance(calendars, list):
        raise HTTPException(status_code=400, detail="calendars must be a list")

    normalized = {"calendars": []}
    for cal in calendars:
        if not isinstance(cal, dict):
            continue
        calendar_name = (cal.get("calendar") or "").strip()
        holidays = cal.get("holidays") or []
        if not calendar_name:
            raise HTTPException(status_code=400, detail="calendar name is required")
        if not isinstance(holidays, list):
            raise HTTPException(status_code=400, detail="holidays must be a list")

        normalized_rules = [validate_holiday_rule(rule) for rule in holidays]
        normalized["calendars"].append({
            "calendar": calendar_name,
            "holidays": normalized_rules,
        })

    return normalized


def nth_years_to_cover(*years: int) -> List[int]:
    s = {y for y in years if isinstance(y, int)}
    if not s:
        now_y = datetime.now().year
        s = {now_y, now_y + 1}
    return sorted(s)


# =============================================================================
# DB accessors
# =============================================================================
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
    zoneinfo_from_any_tz(tz_name, fallback="UTC")

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


def replace_customer_calendars(provider: str, customer_id: str, calendars: List[Dict[str, Any]]) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""
                DELETE FROM customer_calendars
                WHERE provider=:p AND customer_id=:cid
            """),
            {"p": provider, "cid": customer_id},
        )

        q = text("""
            INSERT INTO customer_calendars(provider, customer_id, calendar_id, summary, primary_cal, selected)
            VALUES (:p, :cid, :calid, :summary, :primary, :selected)
        """)

        inserted_primary = False
        first_id: Optional[str] = None

        for c in calendars:
            calid = c.get("id")
            if not calid:
                continue

            if first_id is None:
                first_id = calid

            primary = bool(c.get("primary", False))
            selected = primary

            if primary:
                inserted_primary = True

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

        if calendars and not inserted_primary and first_id:
            conn.execute(
                text("""
                    UPDATE customer_calendars
                    SET selected=true
                    WHERE provider=:p AND customer_id=:cid AND calendar_id=:calid
                """),
                {"p": provider, "cid": customer_id, "calid": first_id},
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

    return [
        {
            "calendarId": r[0],
            "summary": r[1],
            "primary": bool(r[2]),
            "selected": bool(r[3]),
        }
        for r in rows
    ]


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
    if ids:
        return ids
    return [default_fallback] if default_fallback else []


# =============================================================================
# Blackout date helpers
# =============================================================================
def normalize_date_str(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        raise HTTPException(status_code=400, detail="date is required")
    try:
        d = datetime.strptime(s, "%Y-%m-%d").date()
        return d.isoformat()
    except Exception:
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")


def list_blackout_dates(customer_id: str) -> List[Dict[str, Any]]:
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT date_value::text, label
                FROM customer_blackout_dates
                WHERE customer_id=:cid
                ORDER BY date_value
            """),
            {"cid": customer_id},
        ).fetchall()

    return [{"date": r[0], "label": r[1] or ""} for r in rows]


def blackout_date_set(customer_id: str) -> set[str]:
    return {x["date"] for x in list_blackout_dates(customer_id)}


def upsert_blackout_dates(customer_id: str, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(items, list):
        raise HTTPException(status_code=400, detail="items must be a list")

    with engine.begin() as conn:
        for it in items:
            if not isinstance(it, dict):
                continue
            date_value = normalize_date_str(it.get("date") or "")
            label = (it.get("label") or "").strip()
            conn.execute(
                text("""
                    INSERT INTO customer_blackout_dates(customer_id, date_value, label)
                    VALUES (:cid, :dv, :label)
                    ON CONFLICT (customer_id, date_value) DO UPDATE SET
                      label = EXCLUDED.label
                """),
                {"cid": customer_id, "dv": date_value, "label": label},
            )

    return list_blackout_dates(customer_id)


def delete_blackout_dates(customer_id: str, dates: List[str]) -> List[Dict[str, Any]]:
    if not isinstance(dates, list):
        raise HTTPException(status_code=400, detail="dates must be a list")

    cleaned = [normalize_date_str(x) for x in dates if (x or "").strip()]
    if not cleaned:
        return list_blackout_dates(customer_id)

    with engine.begin() as conn:
        conn.execute(
            text("""
                DELETE FROM customer_blackout_dates
                WHERE customer_id=:cid AND date_value = ANY(:dates)
            """),
            {"cid": customer_id, "dates": cleaned},
        )

    return list_blackout_dates(customer_id)


# =============================================================================
# Holiday calendar helpers
# =============================================================================
def save_holiday_calendars(customer_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = validate_holiday_calendar_payload(payload)
    calendars = normalized["calendars"]

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM customer_holiday_calendars WHERE customer_id=:cid"),
            {"cid": customer_id},
        )
        for cal in calendars:
            conn.execute(
                text("""
                    INSERT INTO customer_holiday_calendars(customer_id, calendar_name, config_json)
                    VALUES (:cid, :cname, :cfg)
                """),
                {
                    "cid": customer_id,
                    "cname": cal["calendar"],
                    "cfg": json.dumps(cal),
                },
            )

    return load_holiday_calendars(customer_id)


def load_holiday_calendars(customer_id: str) -> Dict[str, Any]:
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT config_json
                FROM customer_holiday_calendars
                WHERE customer_id=:cid
                ORDER BY calendar_name
            """),
            {"cid": customer_id},
        ).fetchall()

    calendars = []
    for row in rows:
        try:
            obj = json.loads(row[0])
            if isinstance(obj, dict):
                calendars.append(obj)
        except Exception:
            continue

    return {"calendars": calendars}


def expand_holiday_rules_for_year(rule: Dict[str, Any], year: int) -> List[str]:
    out: List[str] = []
    rule_type = rule["type"]
    month = int(rule["month"])

    if rule_type == "fixed":
        day = int(rule["day"])
        try:
            d = date(year, month, day)
            out.append(d.isoformat())
        except Exception:
            pass

    elif rule_type == "nth_weekday":
        weekday = WEEKDAY_NAME_TO_INT[rule["weekday"]]
        nth = int(rule["nth"])
        d = nth_weekday_of_month(year, month, weekday, nth)
        if d:
            out.append(d.isoformat())

    return out


def holiday_date_set(customer_id: str, years: List[int]) -> set[str]:
    cfg = load_holiday_calendars(customer_id)
    out: set[str] = set()

    for cal in cfg.get("calendars", []):
        for rule in cal.get("holidays", []):
            for yr in years:
                out.update(expand_holiday_rules_for_year(rule, yr))

    return out


def combined_closed_date_set(customer_id: str, years: List[int]) -> set[str]:
    return blackout_date_set(customer_id) | holiday_date_set(customer_id, years)


# =============================================================================
# Slot hold helpers
# =============================================================================
def cleanup_expired_holds() -> None:
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM slot_holds WHERE expires_at <= NOW()"))


def list_active_holds(
    provider: str,
    customer_id: str,
    time_min_utc: datetime,
    time_max_utc: datetime,
    exclude_hold_token: Optional[str] = None,
) -> List[Tuple[datetime, datetime]]:
    cleanup_expired_holds()

    q = """
        SELECT start_utc, end_utc
        FROM slot_holds
        WHERE provider=:p
          AND customer_id=:cid
          AND expires_at > NOW()
          AND end_utc > :tmin
          AND start_utc < :tmax
    """
    params: Dict[str, Any] = {
        "p": provider,
        "cid": customer_id,
        "tmin": time_min_utc,
        "tmax": time_max_utc,
    }

    if exclude_hold_token:
        q += " AND hold_token <> :ht"
        params["ht"] = exclude_hold_token

    with engine.begin() as conn:
        rows = conn.execute(text(q), params).fetchall()

    return [(r[0].astimezone(timezone.utc), r[1].astimezone(timezone.utc)) for r in rows]


def create_slot_hold(
    provider: str,
    customer_id: str,
    calendar_id: str,
    start_utc: datetime,
    end_utc: datetime,
    ttl_seconds: int = 300,
) -> Dict[str, Any]:
    cleanup_expired_holds()

    hold_token = str(uuid.uuid4())
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=max(30, int(ttl_seconds)))

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO slot_holds(
                    hold_token, provider, customer_id, calendar_id,
                    start_utc, end_utc, expires_at
                )
                VALUES (
                    :ht, :p, :cid, :calid,
                    :s, :e, :exp
                )
            """),
            {
                "ht": hold_token,
                "p": provider,
                "cid": customer_id,
                "calid": calendar_id,
                "s": start_utc,
                "e": end_utc,
                "exp": expires_at,
            },
        )

    return {
        "holdToken": hold_token,
        "provider": provider,
        "customerId": customer_id,
        "calendarId": calendar_id,
        "startUtc": iso_z(start_utc),
        "endUtc": iso_z(end_utc),
        "expiresAtUtc": iso_z(expires_at),
    }


def release_slot_hold(hold_token: str) -> bool:
    cleanup_expired_holds()
    with engine.begin() as conn:
        result = conn.execute(
            text("DELETE FROM slot_holds WHERE hold_token=:ht"),
            {"ht": hold_token},
        )
    return bool(result.rowcount)


# =============================================================================
# Google provider implementation
# =============================================================================
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


def google_list_events_api(
    access_token: str,
    calendar_id: str,
    time_min_utc: datetime,
    time_max_utc: datetime,
) -> Dict[str, Any]:
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
    tz = zoneinfo_from_any_tz(tz_name, fallback="UTC")
    body: Dict[str, Any] = {
        "summary": summary,
        "description": description,
        "start": {"dateTime": start_utc.astimezone(tz).isoformat(), "timeZone": normalize_tz_name_for_zoneinfo(tz_name, "UTC")},
        "end": {"dateTime": end_utc.astimezone(tz).isoformat(), "timeZone": normalize_tz_name_for_zoneinfo(tz_name, "UTC")},
    }
    if attendees and isinstance(attendees, list):
        body["attendees"] = [
            {"email": (a.get("email") or "").strip()}
            for a in attendees
            if isinstance(a, dict) and (a.get("email") or "").strip()
        ]

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
    r = requests.delete(
        url,
        params={"sendUpdates": "none"},
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    return {"statusCode": r.status_code, "text": r.text}


def google_patch_event_time_api(
    access_token: str,
    calendar_id: str,
    event_id: str,
    start_utc: datetime,
    end_utc: datetime,
    tz_name: str,
) -> Dict[str, Any]:
    tz = zoneinfo_from_any_tz(tz_name, fallback="UTC")
    body = {
        "start": {"dateTime": start_utc.astimezone(tz).isoformat(), "timeZone": normalize_tz_name_for_zoneinfo(tz_name, "UTC")},
        "end": {"dateTime": end_utc.astimezone(tz).isoformat(), "timeZone": normalize_tz_name_for_zoneinfo(tz_name, "UTC")},
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


# =============================================================================
# Microsoft provider implementation
# =============================================================================
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


def microsoft_calendar_view_api(
    access_token: str,
    calendar_id: str,
    time_min_utc: datetime,
    time_max_utc: datetime,
    next_link: Optional[str] = None,
) -> Dict[str, Any]:
    if next_link:
        r = requests.get(next_link, headers=_graph_headers(access_token), timeout=30)
        return {"statusCode": r.status_code, "json": safe_json(r), "text": r.text}

    url = MS_CALENDARVIEW_URL.format(calendarId=safe_cal_id(calendar_id))
    r = requests.get(
        url,
        headers=_graph_headers(access_token),
        params={
            "startDateTime": iso_z(time_min_utc),
            "endDateTime": iso_z(time_max_utc),
            "$top": "1000",
        },
        timeout=30,
    )
    return {"statusCode": r.status_code, "json": safe_json(r), "text": r.text}


def microsoft_calendar_view_all_pages(
    access_token: str,
    calendar_id: str,
    time_min_utc: datetime,
    time_max_utc: datetime,
) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []

    r = microsoft_calendar_view_api(access_token, calendar_id, time_min_utc, time_max_utc)
    if r["statusCode"] != 200 or not r["json"]:
        return items

    j = r["json"] or {}
    items.extend((j.get("value") or []))

    next_link = j.get("@odata.nextLink")
    safety = 0
    while next_link and safety < 20:
        safety += 1
        rr = microsoft_calendar_view_api(access_token, calendar_id, time_min_utc, time_max_utc, next_link=next_link)
        if rr["statusCode"] != 200 or not rr["json"]:
            break
        jj = rr["json"] or {}
        items.extend((jj.get("value") or []))
        next_link = jj.get("@odata.nextLink")

    return items


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
        "showAs": "busy",
        "start": {
            "dateTime": start_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
            "timeZone": "UTC",
        },
        "end": {
            "dateTime": end_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
            "timeZone": "UTC",
        },
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


def microsoft_patch_event_time_api(
    access_token: str,
    event_id: str,
    start_utc: datetime,
    end_utc: datetime,
) -> Dict[str, Any]:
    body = {
        "showAs": "busy",
        "start": {
            "dateTime": start_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
            "timeZone": "UTC",
        },
        "end": {
            "dateTime": end_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
            "timeZone": "UTC",
        },
    }
    url = MS_EVENT_URL.format(eventId=safe_event_id(event_id))
    r = requests.patch(
        url,
        headers={**_graph_headers(access_token), "Content-Type": "application/json"},
        data=json.dumps(body),
        timeout=30,
    )
    return {"statusCode": r.status_code, "json": safe_json(r), "text": r.text, "requestBody": body}


def microsoft_event_time_to_utc(dt_obj: Dict[str, Any], fallback_tz_name: str = "UTC") -> Optional[datetime]:
    if not isinstance(dt_obj, dict):
        return None

    raw = (dt_obj.get("dateTime") or "").strip()
    tz_name = (dt_obj.get("timeZone") or "").strip() or fallback_tz_name
    if not raw:
        return None

    tz = zoneinfo_from_any_tz(tz_name, fallback=normalize_tz_name_for_zoneinfo(fallback_tz_name, "UTC"))

    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    try:
        raw2 = raw.split(".")[0]
        dt = datetime.fromisoformat(raw2)
        dt = dt.replace(tzinfo=tz)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    return None


def microsoft_collect_busy_utc(
    access_token: str,
    tz_name: str,
    calendar_ids: List[str],
    time_min_utc: datetime,
    time_max_utc: datetime,
) -> Dict[str, Any]:
    busy: List[Tuple[datetime, datetime]] = []
    calendar_view_event_count = 0
    calendar_view_busy_count = 0

    for cid in calendar_ids:
        items = microsoft_calendar_view_all_pages(access_token, cid, time_min_utc, time_max_utc)

        for ev in items:
            calendar_view_event_count += 1

            if ev.get("isCancelled") is True:
                continue

            show_as = str(ev.get("showAs") or "").strip().lower()
            if show_as == "free":
                continue

            s_utc = microsoft_event_time_to_utc(ev.get("start") or {}, fallback_tz_name=tz_name)
            e_utc = microsoft_event_time_to_utc(ev.get("end") or {}, fallback_tz_name=tz_name)
            if not s_utc or not e_utc or e_utc <= s_utc:
                continue

            busy.append((s_utc, e_utc))
            calendar_view_busy_count += 1

    merged = merge_intervals_dt(busy)

    return {
        "ok": True,
        "checkedCalendars": list(calendar_ids),
        "timeMinUtc": iso_z(time_min_utc),
        "timeMaxUtc": iso_z(time_max_utc),
        "busyMerged": [{"startUtc": iso_z(s), "endUtc": iso_z(e)} for s, e in merged],
        "debug": {
            "calendarViewEventCount": calendar_view_event_count,
            "calendarViewBusyCount": calendar_view_busy_count,
        },
    }


def microsoft_collect_busy_utc_excluding_event(
    access_token: str,
    calendar_ids: List[str],
    time_min_utc: datetime,
    time_max_utc: datetime,
    exclude_event_id: str,
    fallback_tz_name: str = "UTC",
) -> List[Tuple[datetime, datetime]]:
    busy: List[Tuple[datetime, datetime]] = []

    for cid in calendar_ids:
        items = microsoft_calendar_view_all_pages(access_token, cid, time_min_utc, time_max_utc)
        for ev in items:
            if (ev.get("id") or "") == exclude_event_id:
                continue
            if ev.get("isCancelled") is True:
                continue

            show_as = str(ev.get("showAs") or "").strip().lower()
            if show_as == "free":
                continue

            s_utc = microsoft_event_time_to_utc(ev.get("start") or {}, fallback_tz_name=fallback_tz_name)
            e_utc = microsoft_event_time_to_utc(ev.get("end") or {}, fallback_tz_name=fallback_tz_name)
            if not s_utc or not e_utc or e_utc <= s_utc:
                continue

            busy.append((s_utc, e_utc))

    return merge_intervals_dt(busy)


# =============================================================================
# Provider abstraction
# =============================================================================
def provider_default_calendar_id(provider: str) -> Optional[str]:
    if provider == PROVIDER_GOOGLE:
        return "primary"
    return None


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
            keep = [
                {
                    "id": it.get("id"),
                    "summary": it.get("summary"),
                    "primary": bool(it.get("primary", False)),
                }
                for it in items
                if it.get("id")
            ]

    elif provider == PROVIDER_MICROSOFT:
        cal_list = microsoft_list_calendars_api(access_token)
        if cal_list["statusCode"] == 200 and cal_list["json"]:
            items = cal_list["json"].get("value") or []
            keep = [
                {
                    "id": it.get("id"),
                    "summary": it.get("name"),
                    "primary": bool(it.get("isDefaultCalendar", False)),
                }
                for it in items
                if it.get("id")
            ]
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider}")

    replace_customer_calendars(provider, customer_id, keep)
    return keep


def collect_busy_utc(
    provider: str,
    access_token: str,
    tz_name: str,
    calendar_ids: List[str],
    time_min_utc: datetime,
    time_max_utc: datetime,
    customer_id: Optional[str] = None,
    exclude_hold_token: Optional[str] = None,
) -> Dict[str, Any]:
    if provider == PROVIDER_GOOGLE:
        pack = google_collect_busy_utc(access_token, tz_name, calendar_ids, time_min_utc, time_max_utc)
    elif provider == PROVIDER_MICROSOFT:
        pack = microsoft_collect_busy_utc(access_token, tz_name, calendar_ids, time_min_utc, time_max_utc)
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider}")

    merged_busy = [
        (parse_iso_to_utc(x["startUtc"]), parse_iso_to_utc(x["endUtc"]))
        for x in pack.get("busyMerged", [])
    ]

    if customer_id:
        hold_busy = list_active_holds(
            provider=provider,
            customer_id=customer_id,
            time_min_utc=time_min_utc,
            time_max_utc=time_max_utc,
            exclude_hold_token=exclude_hold_token,
        )
        merged_busy.extend(hold_busy)

    merged_busy = merge_intervals_dt(merged_busy)
    pack["busyMerged"] = [{"startUtc": iso_z(s), "endUtc": iso_z(e)} for s, e in merged_busy]
    return pack


def collect_busy_utc_excluding_event(
    provider: str,
    access_token: str,
    calendar_ids: List[str],
    time_min_utc: datetime,
    time_max_utc: datetime,
    exclude_calendar_id: Optional[str],
    exclude_event_id: str,
    tz_name: str,
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
            fallback_tz_name=tz_name,
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
    blackout_dates: Optional[set[str]] = None,
) -> Dict[str, Any]:
    tz = zoneinfo_from_any_tz(tz_name, fallback="UTC")
    now_local = datetime.now(tz).replace(second=0, microsecond=0)

    start_day = now_local.date()
    end_day = start_day + timedelta(days=max(1, int(days)))
    horizon_end_local = datetime(end_day.year, end_day.month, end_day.day, work_end_hour, 0, tzinfo=tz)

    time_min_utc = now_local.astimezone(timezone.utc)
    time_max_utc = horizon_end_local.astimezone(timezone.utc)

    dur = timedelta(minutes=int(duration_minutes))
    step = timedelta(minutes=int(step_minutes))
    closed_dates = blackout_dates or set()

    available: List[Dict[str, str]] = []
    busy_ptr = 0

    for day_offset in range(int(days)):
        d = start_day + timedelta(days=day_offset)
        if d.isoformat() in closed_dates:
            continue

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
    tz = zoneinfo_from_any_tz(tz_name, fallback="UTC")
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
            items = microsoft_calendar_view_all_pages(access_token, cid, time_min, time_max)
            for ev in items:
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

                s_utc = microsoft_event_time_to_utc(ev.get("start") or {}, fallback_tz_name=tz_name)
                e_utc = microsoft_event_time_to_utc(ev.get("end") or {}, fallback_tz_name=tz_name)
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


# =============================================================================
# Shared route handlers
# =============================================================================
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

    return {
        "customerId": customer_id,
        "provider": provider,
        "calendars": list_calendars_db(provider, customer_id),
    }


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

    pack = collect_busy_utc(
        provider=provider,
        access_token=access_token,
        tz_name=tz_name,
        calendar_ids=calendar_ids,
        time_min_utc=time_min,
        time_max_utc=time_max,
        customer_id=customer_id,
    )
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
    work_start = int(payload.get("workStartHour", settings["work_start_hour"]))
    work_end = int(payload.get("workEndHour", settings["work_end_hour"]))
    work_days = normalize_work_days(payload.get("workDays", settings["work_days"]))

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

    duration_minutes = int(payload.get("durationMinutes", max(1, int((end_utc - start_utc).total_seconds() // 60))))
    step_minutes = int(payload.get("stepMinutes", 30))
    days = int(payload.get("days", 7))

    cal_ids = payload.get("calendarIds")
    calendar_ids = cal_ids if isinstance(cal_ids, list) and cal_ids else selected_calendar_ids(
        provider,
        customer_id,
        provider_default_calendar_id(provider),
    )
    calendar_ids = [c for c in calendar_ids if c]
    if not calendar_ids:
        raise HTTPException(status_code=400, detail=f"No {provider} calendars selected/found")

    rt = load_refresh_token(provider, customer_id)
    access_token = refresh_access_token(provider, rt)

    exact_min = start_utc - timedelta(minutes=1)
    exact_max = end_utc + timedelta(minutes=1)

    busy_pack = collect_busy_utc(
        provider=provider,
        access_token=access_token,
        tz_name=tz_name,
        calendar_ids=calendar_ids,
        time_min_utc=exact_min,
        time_max_utc=exact_max,
        customer_id=customer_id,
    )
    merged_busy = [
        (parse_iso_to_utc(x["startUtc"]), parse_iso_to_utc(x["endUtc"]))
        for x in busy_pack.get("busyMerged", [])
    ]
    merged_busy = merge_intervals_dt(merged_busy)

    overlaps_found = [
        {"startUtc": iso_z(bs), "endUtc": iso_z(be)}
        for bs, be in merged_busy
        if overlaps(start_utc, end_utc, bs, be)
    ]

    is_free = len(overlaps_found) == 0
    tz = zoneinfo_from_any_tz(tz_name, fallback="UTC")

    preference = payload.get("preference")
    if preference is not None and not isinstance(preference, dict):
        preference = None

    preferred_utc = start_utc

    now_local = datetime.now(tz).replace(second=0, microsecond=0)
    start_day = now_local.date()
    end_day = start_day + timedelta(days=max(1, days))
    horizon_end_local = datetime(end_day.year, end_day.month, end_day.day, work_end, 0, tzinfo=tz)
    horizon_min_utc = now_local.astimezone(timezone.utc)
    horizon_max_utc = horizon_end_local.astimezone(timezone.utc)

    years = nth_years_to_cover(datetime.now().year, datetime.now().year + 1, start_utc.year, end_utc.year)
    closed_dates = combined_closed_date_set(customer_id, years=years)

    horizon_busy_pack = collect_busy_utc(
        provider=provider,
        access_token=access_token,
        tz_name=tz_name,
        calendar_ids=calendar_ids,
        time_min_utc=horizon_min_utc,
        time_max_utc=horizon_max_utc,
        customer_id=customer_id,
    )
    horizon_merged_busy = [
        (parse_iso_to_utc(x["startUtc"]), parse_iso_to_utc(x["endUtc"]))
        for x in horizon_busy_pack.get("busyMerged", [])
    ]
    horizon_merged_busy = merge_intervals_dt(horizon_merged_busy)

    availability_out = compute_availability_from_busy(
        tz_name=tz_name,
        work_start_hour=work_start,
        work_end_hour=work_end,
        work_days=work_days,
        merged_busy=horizon_merged_busy,
        duration_minutes=duration_minutes,
        step_minutes=step_minutes,
        days=days,
        preferred_utc=preferred_utc,
        preference=preference,
        blackout_dates=closed_dates,
    )

    if is_free:
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
            "isFree": True,
            "status": "free",
            "reason": "slot_free",
            "message": "That time is free.",
            "busyMerged": horizon_busy_pack.get("busyMerged", []),
            "overlaps": [],
            "suggestions": availability_out.get("suggestions", []),
            "availableCount": availability_out.get("availableCount", 0),
            "available": availability_out.get("available", []),
            "debug": horizon_busy_pack.get("debug"),
        }

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
        "isFree": False,
        "status": "busy",
        "reason": "slot_taken",
        "message": "Sorry, someone just booked that slot. Please pick another time.",
        "busyMerged": horizon_busy_pack.get("busyMerged", []),
        "overlaps": overlaps_found,
        "suggestions": availability_out.get("suggestions", []),
        "availableCount": availability_out.get("availableCount", 0),
        "available": availability_out.get("available", []),
        "debug": horizon_busy_pack.get("debug"),
    }


def availability_handler(provider: str, request: Request, payload: Dict[str, Any]):
    require_api_key(request)
    provider = validate_provider(provider)

    has_exact_interval = bool(
        (payload.get("startUtc") and payload.get("endUtc"))
        or (
            payload.get("timeMinUtc")
            and payload.get("timeMaxUtc")
            and "durationMinutes" not in payload
            and "days" not in payload
        )
    )
    if has_exact_interval:
        return check_availability_handler(provider, request, payload)

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
            return {
                "ok": False,
                "reason": "invalid_preferredDateTimeUtc",
                "message": "preferredDateTimeUtc must be ISO with Z or offset",
            }

    preference = payload.get("preference")
    if preference is not None and not isinstance(preference, dict):
        preference = None

    rt = load_refresh_token(provider, customer_id)
    access_token = refresh_access_token(provider, rt)

    tz = zoneinfo_from_any_tz(tz_name, fallback="UTC")
    now_local = datetime.now(tz).replace(second=0, microsecond=0)
    start_day = now_local.date()
    end_day = start_day + timedelta(days=max(1, days))
    horizon_end_local = datetime(end_day.year, end_day.month, end_day.day, work_end, 0, tzinfo=tz)
    time_min_utc = now_local.astimezone(timezone.utc)
    time_max_utc = horizon_end_local.astimezone(timezone.utc)

    current_year = datetime.now().year
    closed_dates = combined_closed_date_set(customer_id, years=[current_year, current_year + 1])

    busy_pack = collect_busy_utc(
        provider=provider,
        access_token=access_token,
        tz_name=tz_name,
        calendar_ids=calendar_ids,
        time_min_utc=time_min_utc,
        time_max_utc=time_max_utc,
        customer_id=customer_id,
    )
    merged_busy = [
        (parse_iso_to_utc(x["startUtc"]), parse_iso_to_utc(x["endUtc"]))
        for x in busy_pack.get("busyMerged", [])
    ]
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
        blackout_dates=closed_dates,
    )
    out["provider"] = provider
    out["calendarIdsUsed"] = calendar_ids
    out["busyMerged"] = busy_pack.get("busyMerged", [])
    out["debug"] = busy_pack.get("debug")
    out["message"] = "Available times found." if out.get("availableCount", 0) > 0 else "No available times found in the requested window."
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

    local_tz = zoneinfo_from_any_tz(tz_name, fallback="UTC")
    start_local_date = start_utc.astimezone(local_tz).date().isoformat()
    closed_dates = combined_closed_date_set(customer_id, years=[start_utc.year, end_utc.year])

    if start_local_date in closed_dates:
        return {
            "booked": False,
            "reason": "closed_date",
            "message": "That business is closed on the requested date. Please pick another day.",
        }

    rt = load_refresh_token(provider, customer_id)
    access_token = refresh_access_token(provider, rt)

    calendars_to_check = selected_calendar_ids(provider, customer_id, provider_default_calendar_id(provider))
    calendars_to_check = [c for c in calendars_to_check if c]
    if calendar_id not in calendars_to_check:
        calendars_to_check = [calendar_id] + calendars_to_check

    time_min = start_utc - timedelta(minutes=1)
    time_max = end_utc + timedelta(minutes=1)

    busy_pack = collect_busy_utc(
        provider=provider,
        access_token=access_token,
        tz_name=tz_name,
        calendar_ids=calendars_to_check,
        time_min_utc=time_min,
        time_max_utc=time_max,
        customer_id=customer_id,
    )
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

        local_tz = zoneinfo_from_any_tz(tz_name, fallback="UTC")
        new_local_date = new_start.astimezone(local_tz).date().isoformat()
        closed_dates = combined_closed_date_set(customer_id, years=[new_start.year, new_end.year])
        if new_local_date in closed_dates:
            results.append({
                "eventId": ev_id,
                "rescheduled": False,
                "reason": "closed_date",
                "message": "That business is closed on the requested date. Please pick another day.",
            })
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
            tz_name=tz_name,
        )

        hold_busy = list_active_holds(
            provider=provider,
            customer_id=customer_id,
            time_min_utc=time_min,
            time_max_utc=time_max,
        )
        busy_merged = merge_intervals_dt(busy_merged + hold_busy)

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


# =============================================================================
# Unified schedule endpoint
# =============================================================================
@app.post("/schedule")
async def schedule(request: Request, payload: Dict[str, Any]):
    require_api_key(request)

    provider = validate_provider((payload.get("provider") or "").strip().lower())
    intent = (payload.get("intent") or "").strip().lower()
    customer_id = (payload.get("customerId") or "").strip()

    if not customer_id:
        raise HTTPException(status_code=400, detail="customerId required")

    if intent == "search":
        search_payload = {
            "customerId": customer_id,
            "timeZone": payload.get("timeZone") or "America/Denver",
            "email": ((payload.get("search") or {}).get("email") if isinstance(payload.get("search"), dict) else "") or payload.get("email") or "",
            "phone": ((payload.get("search") or {}).get("phone") if isinstance(payload.get("search"), dict) else "") or payload.get("phone") or "",
            "timeMinUtc": payload.get("timeMinUtc") or "",
            "timeMaxUtc": payload.get("timeMaxUtc") or "",
            "calendarIds": payload.get("calendarIds") or [],
            "searchAttendees": True,
        }
        out = search_events_handler(provider, request, search_payload)
        return {
            "ok": True,
            "intent": "search",
            "provider": provider,
            "customerId": customer_id,
            "actionTaken": "searched",
            "message": f"Found {out.get('count', 0)} matching appointment(s)." if out.get("count", 0) else "I could not find any matching appointments.",
            "needsUserChoice": out.get("count", 0) > 1,
            "needsMoreInfo": out.get("count", 0) == 0,
            "booked": False,
            "cancelled": False,
            "rescheduled": False,
            "matches": out.get("matches", []),
            "results": [],
            "suggestions": [],
            "available": [],
            "event": None,
        }

    if intent == "cancel":
        out = cancel_events_handler(provider, request, {
            "customerId": customer_id,
            "items": payload.get("items") or [],
        })
        success = any(x.get("cancelled") for x in out.get("results", []))
        return {
            "ok": True,
            "intent": "cancel",
            "provider": provider,
            "customerId": customer_id,
            "actionTaken": "cancelled" if success else "none",
            "message": "Appointment cancelled successfully." if success else "I could not cancel that appointment.",
            "needsUserChoice": False,
            "needsMoreInfo": False,
            "booked": False,
            "cancelled": success,
            "rescheduled": False,
            "matches": [],
            "results": out.get("results", []),
            "suggestions": [],
            "available": [],
            "event": None,
        }

    if intent == "reschedule":
        out = reschedule_events_handler(provider, request, {
            "customerId": customer_id,
            "timeZone": payload.get("timeZone") or "America/Denver",
            "items": payload.get("items") or [],
        })
        success = any(x.get("rescheduled") for x in out.get("results", []))
        slot_taken = any(x.get("reason") == "slot_taken" for x in out.get("results", []))
        closed_date = any(x.get("reason") == "closed_date" for x in out.get("results", []))

        msg = "Appointment rescheduled successfully."
        if slot_taken:
            msg = "Sorry, that time is taken. Please pick a different time."
        elif closed_date:
            msg = "That business is closed on the requested date. Please pick another day."
        elif not success:
            msg = "I could not reschedule that appointment."

        return {
            "ok": True,
            "intent": "reschedule",
            "provider": provider,
            "customerId": customer_id,
            "actionTaken": "rescheduled" if success else "none",
            "message": msg,
            "needsUserChoice": False,
            "needsMoreInfo": False,
            "booked": False,
            "cancelled": False,
            "rescheduled": success,
            "matches": [],
            "results": out.get("results", []),
            "suggestions": [],
            "available": [],
            "event": None,
        }

    if intent == "schedule":
        has_exact = bool((payload.get("startUtc") or "").strip() and (payload.get("endUtc") or "").strip())

        if has_exact:
            check_payload = {
                "customerId": customer_id,
                "timeZone": payload.get("timeZone") or "America/Denver",
                "startUtc": payload.get("startUtc"),
                "endUtc": payload.get("endUtc"),
                "durationMinutes": payload.get("durationMinutes", 60),
                "stepMinutes": payload.get("stepMinutes", 30),
                "days": payload.get("days", 7),
                "calendarIds": payload.get("calendarIds") or [],
                "preference": payload.get("preference") or {},
            }
            check_out = check_availability_handler(provider, request, check_payload)

            if not check_out.get("isFree"):
                return {
                    "ok": True,
                    "intent": "schedule",
                    "provider": provider,
                    "customerId": customer_id,
                    "actionTaken": "suggested",
                    "message": "Sorry, that time is taken. Here are some other available times.",
                    "needsUserChoice": False,
                    "needsMoreInfo": False,
                    "booked": False,
                    "cancelled": False,
                    "rescheduled": False,
                    "matches": [],
                    "results": [],
                    "suggestions": check_out.get("suggestions", []),
                    "available": check_out.get("available", []),
                    "event": None,
                }

            hold = create_slot_hold(
                provider=provider,
                customer_id=customer_id,
                calendar_id=(payload.get("calendarId") or ""),
                start_utc=parse_iso_to_utc(payload.get("startUtc")),
                end_utc=parse_iso_to_utc(payload.get("endUtc")),
                ttl_seconds=int(payload.get("ttlSeconds", 300)),
            )

            create_payload = {
                "customerId": customer_id,
                "timeZone": payload.get("timeZone") or "America/Denver",
                "calendarId": payload.get("calendarId") or "",
                "summary": payload.get("summary") or "Appointment",
                "description": payload.get("description") or "",
                "attendees": payload.get("attendees") or [],
                "start": {"dateTime": payload.get("startUtc")},
                "end": {"dateTime": payload.get("endUtc")},
            }

            create_out = create_event_handler(provider, request, create_payload)
            release_slot_hold(hold["holdToken"])

            if create_out.get("booked"):
                return {
                    "ok": True,
                    "intent": "schedule",
                    "provider": provider,
                    "customerId": customer_id,
                    "actionTaken": "booked",
                    "message": "Appointment booked successfully.",
                    "needsUserChoice": False,
                    "needsMoreInfo": False,
                    "booked": True,
                    "cancelled": False,
                    "rescheduled": False,
                    "hold": hold,
                    "matches": [],
                    "results": [],
                    "suggestions": [],
                    "available": [],
                    "event": create_out.get("event"),
                }

            return {
                "ok": True,
                "intent": "schedule",
                "provider": provider,
                "customerId": customer_id,
                "actionTaken": "none",
                "message": create_out.get("message") or "I could not book that appointment.",
                "needsUserChoice": False,
                "needsMoreInfo": False,
                "booked": False,
                "cancelled": False,
                "rescheduled": False,
                "hold": hold,
                "matches": [],
                "results": [],
                "suggestions": [],
                "available": [],
                "event": None,
            }

        avail_payload = {
            "customerId": customer_id,
            "timeZone": payload.get("timeZone") or "America/Denver",
            "durationMinutes": payload.get("durationMinutes", 60),
            "stepMinutes": payload.get("stepMinutes", 30),
            "days": payload.get("days", 14),
            "calendarIds": payload.get("calendarIds") or [],
            "preferredDateTimeUtc": payload.get("preferredDateTimeUtc") or "",
            "preference": payload.get("preference") or {},
        }
        avail_out = availability_handler(provider, request, avail_payload)

        return {
            "ok": True,
            "intent": "schedule",
            "provider": provider,
            "customerId": customer_id,
            "actionTaken": "suggested" if avail_out.get("availableCount", 0) > 0 else "none",
            "message": "Here are the best available times." if avail_out.get("availableCount", 0) > 0 else "I could not find any available times.",
            "needsUserChoice": avail_out.get("availableCount", 0) > 0,
            "needsMoreInfo": avail_out.get("availableCount", 0) == 0,
            "booked": False,
            "cancelled": False,
            "rescheduled": False,
            "matches": [],
            "results": [],
            "suggestions": avail_out.get("suggestions", []),
            "available": avail_out.get("available", []),
            "event": None,
        }

    raise HTTPException(status_code=400, detail="Unsupported intent")


# =============================================================================
# Hold endpoints
# =============================================================================
@app.post("/holds/create")
async def create_hold_endpoint(request: Request, payload: Dict[str, Any]):
    require_api_key(request)

    provider = validate_provider((payload.get("provider") or "").strip().lower())
    customer_id = (payload.get("customerId") or "").strip()
    calendar_id = (payload.get("calendarId") or "").strip()
    tz_name = (payload.get("timeZone") or "America/Denver").strip()
    ttl_seconds = int(payload.get("ttlSeconds", 300))

    if not customer_id:
        raise HTTPException(status_code=400, detail="customerId required")

    try:
        start_utc = parse_any_datetime_to_utc((payload.get("startUtc") or "").strip(), tz_name)
        end_utc = parse_any_datetime_to_utc((payload.get("endUtc") or "").strip(), tz_name)
    except Exception:
        raise HTTPException(status_code=400, detail="startUtc/endUtc required")

    if end_utc <= start_utc:
        raise HTTPException(status_code=400, detail="endUtc must be after startUtc")

    rt = load_refresh_token(provider, customer_id)
    access_token = refresh_access_token(provider, rt)

    cal_ids = payload.get("calendarIds")
    calendar_ids = cal_ids if isinstance(cal_ids, list) and cal_ids else selected_calendar_ids(provider, customer_id, provider_default_calendar_id(provider))
    calendar_ids = [c for c in calendar_ids if c]
    if calendar_id and calendar_id not in calendar_ids:
        calendar_ids = [calendar_id] + calendar_ids

    busy_pack = collect_busy_utc(
        provider=provider,
        access_token=access_token,
        tz_name=tz_name,
        calendar_ids=calendar_ids,
        time_min_utc=start_utc - timedelta(minutes=1),
        time_max_utc=end_utc + timedelta(minutes=1),
        customer_id=customer_id,
    )

    merged_busy = [
        (parse_iso_to_utc(x["startUtc"]), parse_iso_to_utc(x["endUtc"]))
        for x in busy_pack.get("busyMerged", [])
    ]

    if any(overlaps(start_utc, end_utc, bs, be) for bs, be in merged_busy):
        return {
            "ok": True,
            "held": False,
            "reason": "slot_taken",
            "message": "Sorry, that slot is not available.",
            "busyMerged": busy_pack.get("busyMerged", []),
        }

    hold = create_slot_hold(
        provider=provider,
        customer_id=customer_id,
        calendar_id=calendar_id or "",
        start_utc=start_utc,
        end_utc=end_utc,
        ttl_seconds=ttl_seconds,
    )

    return {
        "ok": True,
        "held": True,
        "message": "Slot hold created.",
        "hold": hold,
    }


@app.post("/holds/release")
async def release_hold_endpoint(request: Request, payload: Dict[str, Any]):
    require_api_key(request)
    hold_token = (payload.get("holdToken") or "").strip()
    if not hold_token:
        raise HTTPException(status_code=400, detail="holdToken required")

    ok = release_slot_hold(hold_token)
    return {
        "ok": True,
        "released": ok,
        "message": "Hold released." if ok else "Hold was not found or already expired.",
    }


# =============================================================================
# Holiday / blackout endpoints
# =============================================================================
@app.get("/customers/{customer_id}/blackout_dates")
def get_blackout_dates(request: Request, customer_id: str):
    require_api_key(request)
    return {
        "ok": True,
        "customerId": customer_id,
        "blackoutDates": list_blackout_dates(customer_id),
    }


@app.post("/customers/{customer_id}/blackout_dates")
async def add_blackout_dates(request: Request, customer_id: str, payload: Dict[str, Any]):
    require_api_key(request)
    items = payload.get("items") or []
    out = upsert_blackout_dates(customer_id, items)
    return {
        "ok": True,
        "customerId": customer_id,
        "blackoutDates": out,
    }


@app.delete("/customers/{customer_id}/blackout_dates")
async def remove_blackout_dates(request: Request, customer_id: str, payload: Dict[str, Any]):
    require_api_key(request)
    dates = payload.get("dates") or []
    out = delete_blackout_dates(customer_id, dates)
    return {
        "ok": True,
        "customerId": customer_id,
        "blackoutDates": out,
    }


@app.get("/customers/{customer_id}/holiday_calendars")
def get_holiday_calendars(request: Request, customer_id: str):
    require_api_key(request)
    return {
        "ok": True,
        "customerId": customer_id,
        "holidayCalendars": load_holiday_calendars(customer_id),
    }


@app.post("/customers/{customer_id}/holiday_calendars")
async def set_holiday_calendars(request: Request, customer_id: str, payload: Dict[str, Any]):
    require_api_key(request)
    out = save_holiday_calendars(customer_id, payload)
    return {
        "ok": True,
        "customerId": customer_id,
        "holidayCalendars": out,
    }


# =============================================================================
# Health / debug
# =============================================================================
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


# =============================================================================
# Generic provider routes
# =============================================================================
@app.get("/oauth/{provider}/start")
def oauth_start(provider: str, customerId: str = Query(...)):
    return oauth_start_handler(provider, customerId)


@app.get("/oauth/{provider}/callback")
def oauth_callback(provider: str, code: str = "", state: str = "", error: str = ""):
    provider = validate_provider(provider)
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


# =============================================================================
# Wrapper routes
# =============================================================================
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
