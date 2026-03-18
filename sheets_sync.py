"""
Google Sheets sync module.

Syncs calendar events (Google + Microsoft) to a reminder table in Google Sheets.
Make.com reads that sheet to trigger SMS reminders.

Sheet columns A–P (0-indexed 0–15):
  A  customer_id
  B  provider
  C  calendar_id
  D  event_id
  E  title
  F  name
  G  phone
  H  email
  I  start_time  (UTC ISO)
  J  end_time    (UTC ISO)
  K  status      (scheduled | cancelled)
  L  reminder_3d
  M  reminder_1d
  N  reminder_2h
  O  timezone
  P  last_updated

RULES:
  - NEVER overwrite reminder columns L/M/N.
  - EXCEPTION: if event start_time changes, reset L/M/N to blank.
  - Composite key = customer_id + provider + calendar_id + event_id
  - Rolling window: 30 days past → 180 days future
  - Batch operations only (no row-by-row updates).
  - Mark status=cancelled when event disappears from provider;
    hard-delete rows only when end_time < now - 1 day.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SHEETS_SPREADSHEET_ID: str = os.environ.get("SHEETS_SPREADSHEET_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON: str = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
SHEET_TAB: str = os.environ.get("SHEETS_TAB_NAME", "Appointments").strip()

SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"
SHEETS_BASE = "https://sheets.googleapis.com/v4/spreadsheets"

# Rolling window
SYNC_DAYS_PAST = 30
SYNC_DAYS_FUTURE = 180

# Column indices (0-based)
C_CUSTOMER_ID = 0
C_PROVIDER = 1
C_CALENDAR_ID = 2
C_EVENT_ID = 3
C_TITLE = 4
C_NAME = 5
C_PHONE = 6
C_EMAIL = 7
C_START_TIME = 8
C_END_TIME = 9
C_STATUS = 10
C_REMINDER_3D = 11
C_REMINDER_1D = 12
C_REMINDER_2H = 13
C_TIMEZONE = 14
C_LAST_UPDATED = 15
NUM_COLS = 16  # A–P

# ---------------------------------------------------------------------------
# Regex helpers for contact extraction
# ---------------------------------------------------------------------------
_EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")
_PHONE_RE = re.compile(
    r"\b(?:\+?1[\s.\-]?)?\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}\b"
)
_NAME_RE = re.compile(
    r"(?:name|patient|client|customer|contact)\s*[:\-]\s*([A-Za-z][A-Za-z '\-]{1,40})",
    re.IGNORECASE,
)


def _extract_email(text: str) -> str:
    m = _EMAIL_RE.search(text or "")
    return m.group(0).lower() if m else ""


def _extract_phone(text: str) -> str:
    m = _PHONE_RE.search(text or "")
    return re.sub(r"\D", "", m.group(0)) if m else ""


def _extract_name(text: str) -> str:
    m = _NAME_RE.search(text or "")
    return m.group(1).strip() if m else ""


# ---------------------------------------------------------------------------
# Service-account token (cached, auto-refresh)
# ---------------------------------------------------------------------------
_sa_token: Optional[str] = None
_sa_token_expiry: Optional[datetime] = None
_sa_lock = asyncio.Lock()


def _build_jwt(sa_info: Dict[str, Any]) -> str:
    """Build a signed JWT for Google service account auth."""
    import base64
    import hashlib
    import hmac
    import struct
    import time

    # Minimal JWT signing — uses RS256 via Python's cryptography library if available,
    # falls back to google-auth library approach.
    try:
        from google.oauth2 import service_account
        from google.auth.transport.requests import Request as GRequest
        import google.auth

        creds = service_account.Credentials.from_service_account_info(
            sa_info, scopes=[SHEETS_SCOPE]
        )
        creds.refresh(GRequest())
        return creds.token
    except ImportError:
        raise RuntimeError(
            "google-auth package is required for Sheets sync. "
            "Add google-auth to requirements.txt."
        )


async def _get_sheets_token() -> str:
    """Return a valid Google Sheets access token, refreshing if needed."""
    global _sa_token, _sa_token_expiry

    async with _sa_lock:
        now = datetime.now(timezone.utc)
        if _sa_token and _sa_token_expiry and now < _sa_token_expiry - timedelta(minutes=5):
            return _sa_token

        if not GOOGLE_SERVICE_ACCOUNT_JSON:
            raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON env var is missing")

        sa_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        # Run the synchronous google-auth refresh in a thread to avoid blocking
        token = await asyncio.to_thread(_build_jwt, sa_info)
        _sa_token = token
        # Google SA tokens last 1 hour
        _sa_token_expiry = now + timedelta(hours=1)
        return token


# ---------------------------------------------------------------------------
# Sheets API helpers
# ---------------------------------------------------------------------------
_sheets_semaphore = asyncio.Semaphore(3)
_sheets_http: Optional[httpx.AsyncClient] = None


def _get_sheets_http() -> httpx.AsyncClient:
    global _sheets_http
    if _sheets_http is None:
        _sheets_http = httpx.AsyncClient(timeout=httpx.Timeout(30.0))
    return _sheets_http


async def _sheets_get(path: str, token: str) -> Dict[str, Any]:
    url = f"{SHEETS_BASE}/{SHEETS_SPREADSHEET_ID}{path}"
    async with _sheets_semaphore:
        r = await _get_sheets_http().get(url, headers={"Authorization": f"Bearer {token}"})
    r.raise_for_status()
    return r.json()


async def _sheets_post(path: str, token: str, body: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{SHEETS_BASE}/{SHEETS_SPREADSHEET_ID}{path}"
    async with _sheets_semaphore:
        r = await _get_sheets_http().post(
            url,
            headers={"Authorization": f"Bearer {token}"},
            json=body,
        )
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Sheet read
# ---------------------------------------------------------------------------
async def _read_sheet_rows(token: str) -> List[List[str]]:
    """Return all rows (including header) from the sheet."""
    data = await _sheets_get(
        f"/values/{SHEET_TAB}!A:{chr(ord('A') + NUM_COLS - 1)}",
        token,
    )
    return data.get("values", [])


def _build_index(rows: List[List[str]]) -> Dict[str, Tuple[int, List[str]]]:
    """
    Build a dict: composite_key → (1-based row number, row_data).
    Skips header row (row 1 = index 0).
    Pads short rows to NUM_COLS.
    """
    index: Dict[str, Tuple[int, List[str]]] = {}
    for i, row in enumerate(rows):
        if i == 0:  # header
            continue
        padded = list(row) + [""] * (NUM_COLS - len(row))
        key = _composite_key(
            padded[C_CUSTOMER_ID],
            padded[C_PROVIDER],
            padded[C_CALENDAR_ID],
            padded[C_EVENT_ID],
        )
        if key:
            index[key] = (i + 1, padded)  # 1-based row number
    return index


def _composite_key(customer_id: str, provider: str, calendar_id: str, event_id: str) -> str:
    return f"{customer_id}|{provider}|{calendar_id}|{event_id}"


# ---------------------------------------------------------------------------
# Event normalization
# ---------------------------------------------------------------------------
def normalize_google_event_for_sheet(
    event: Dict[str, Any],
    customer_id: str,
    calendar_id: str,
    customer_tz: str,
) -> Optional[Dict[str, Any]]:
    """Normalize a Google Calendar event dict to the standard schema."""
    ev_id = (event.get("id") or "").strip()
    if not ev_id:
        return None

    start_obj = event.get("start") or {}
    end_obj = event.get("end") or {}
    start_raw = start_obj.get("dateTime") or start_obj.get("date") or ""
    end_raw = end_obj.get("dateTime") or end_obj.get("date") or ""

    try:
        start_utc = _parse_to_utc(start_raw, customer_tz)
        end_utc = _parse_to_utc(end_raw, customer_tz)
    except Exception:
        return None

    description = event.get("description") or ""
    title = (event.get("summary") or "").strip()

    # Contact extraction: description first, then attendees
    name = _extract_name(description)
    phone = _extract_phone(description)
    email = _extract_email(description)

    for att in event.get("attendees") or []:
        if not isinstance(att, dict):
            continue
        att_email = (att.get("email") or "").strip().lower()
        att_name = (att.get("displayName") or "").strip()
        if att_email and not email:
            email = att_email
        if att_name and not name:
            name = att_name

    tz = (start_obj.get("timeZone") or customer_tz).strip()

    return {
        "customer_id": customer_id,
        "provider": "google",
        "calendar_id": calendar_id,
        "event_id": ev_id,
        "title": title,
        "name": name,
        "phone": phone,
        "email": email,
        "start_time": _iso_z(start_utc),
        "end_time": _iso_z(end_utc),
        "status": "scheduled",
        "timezone": tz,
        "last_updated": _iso_z(datetime.now(timezone.utc)),
    }


def normalize_microsoft_event_for_sheet(
    event: Dict[str, Any],
    customer_id: str,
    calendar_id: str,
    customer_tz: str,
) -> Optional[Dict[str, Any]]:
    """Normalize a Microsoft Graph calendar event to the standard schema."""
    ev_id = (event.get("id") or "").strip()
    if not ev_id:
        return None

    start_obj = event.get("start") or {}
    end_obj = event.get("end") or {}

    try:
        start_utc = _ms_dt_to_utc(start_obj, customer_tz)
        end_utc = _ms_dt_to_utc(end_obj, customer_tz)
        if not start_utc or not end_utc:
            return None
    except Exception:
        return None

    title = (event.get("subject") or "").strip()
    body_preview = event.get("bodyPreview") or ""
    body_content = (event.get("body") or {}).get("content") or ""
    searchable_text = body_preview + "\n" + body_content

    name = _extract_name(searchable_text)
    phone = _extract_phone(searchable_text)
    email = _extract_email(searchable_text)

    for att in event.get("attendees") or []:
        if not isinstance(att, dict):
            continue
        addr = ((att.get("emailAddress") or {}).get("address") or "").strip().lower()
        disp = ((att.get("emailAddress") or {}).get("name") or "").strip()
        if addr and not email:
            email = addr
        if disp and not name:
            name = disp

    tz = (start_obj.get("timeZone") or customer_tz).strip()

    return {
        "customer_id": customer_id,
        "provider": "microsoft",
        "calendar_id": calendar_id,
        "event_id": ev_id,
        "title": title,
        "name": name,
        "phone": phone,
        "email": email,
        "start_time": _iso_z(start_utc),
        "end_time": _iso_z(end_utc),
        "status": "scheduled",
        "timezone": tz,
        "last_updated": _iso_z(datetime.now(timezone.utc)),
    }


# ---------------------------------------------------------------------------
# Calendar event fetchers (uses main.py HTTP functions indirectly via params)
# ---------------------------------------------------------------------------
async def fetch_google_events_for_sync(
    access_token: str,
    calendar_id: str,
    http_client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    time_min: datetime,
    time_max: datetime,
) -> List[Dict[str, Any]]:
    """Fetch all Google Calendar events in the rolling window."""
    from urllib.parse import quote

    url = (
        f"https://www.googleapis.com/calendar/v3/calendars/"
        f"{quote(calendar_id, safe='')}/events"
    )
    params = {
        "timeMin": _iso_z(time_min),
        "timeMax": _iso_z(time_max),
        "singleEvents": "true",
        "orderBy": "startTime",
        "maxResults": "2500",
    }
    async with semaphore:
        r = await http_client.get(
            url,
            headers={"Authorization": f"Bearer {access_token}"},
            params=params,
        )
    if r.status_code != 200:
        return []
    data = r.json()
    return [ev for ev in (data.get("items") or []) if ev.get("status") != "cancelled"]


async def fetch_microsoft_events_for_sync(
    access_token: str,
    calendar_id: str,
    http_client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    time_min: datetime,
    time_max: datetime,
) -> List[Dict[str, Any]]:
    """Fetch all Microsoft Graph calendar events in the rolling window (handles pagination)."""
    from urllib.parse import quote

    url = (
        f"https://graph.microsoft.com/v1.0/me/calendars/"
        f"{quote(calendar_id, safe='')}/calendarView"
    )
    params = {
        "startDateTime": _iso_z(time_min),
        "endDateTime": _iso_z(time_max),
        "$top": "1000",
    }
    items: List[Dict[str, Any]] = []
    safety = 0
    next_link: Optional[str] = None

    while True:
        async with semaphore:
            if next_link:
                r = await http_client.get(
                    next_link, headers={"Authorization": f"Bearer {access_token}"}
                )
            else:
                r = await http_client.get(
                    url,
                    headers={"Authorization": f"Bearer {access_token}"},
                    params=params,
                )
        if r.status_code != 200:
            break
        data = r.json()
        items.extend(
            ev for ev in (data.get("value") or []) if not ev.get("isCancelled")
        )
        next_link = data.get("@odata.nextLink")
        safety += 1
        if not next_link or safety >= 20:
            break

    return items


# ---------------------------------------------------------------------------
# Main sync function
# ---------------------------------------------------------------------------
async def sync_customer_provider_to_sheet(
    customer_id: str,
    provider: str,
    access_token: str,
    calendar_ids: List[str],
    customer_tz: str,
    http_client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> None:
    """
    Sync one customer+provider's calendar events to Google Sheets.
    Called after create/cancel/reschedule and via /internal/sync endpoint.
    """
    if not SHEETS_SPREADSHEET_ID or not GOOGLE_SERVICE_ACCOUNT_JSON:
        return  # Sheets not configured — skip silently

    now_utc = datetime.now(timezone.utc)
    time_min = now_utc - timedelta(days=SYNC_DAYS_PAST)
    time_max = now_utc + timedelta(days=SYNC_DAYS_FUTURE)

    token = await _get_sheets_token()

    # 1. Read current sheet state
    rows = await _read_sheet_rows(token)
    index = _build_index(rows)

    # 2. Fetch calendar events in parallel across all calendar_ids
    fetch_tasks = []
    for cal_id in calendar_ids:
        if provider == "google":
            fetch_tasks.append(
                fetch_google_events_for_sync(
                    access_token, cal_id, http_client, semaphore, time_min, time_max
                )
            )
        elif provider == "microsoft":
            fetch_tasks.append(
                fetch_microsoft_events_for_sync(
                    access_token, cal_id, http_client, semaphore, time_min, time_max
                )
            )

    all_fetched: List[List[Dict[str, Any]]] = await asyncio.gather(
        *fetch_tasks, return_exceptions=True
    )

    # 3. Normalize all events and track which keys we saw
    normalized_events: List[Dict[str, Any]] = []
    seen_keys: set[str] = set()

    for cal_id, result in zip(calendar_ids, all_fetched):
        if isinstance(result, Exception):
            continue
        for raw_ev in result:
            if provider == "google":
                norm = normalize_google_event_for_sheet(raw_ev, customer_id, cal_id, customer_tz)
            else:
                norm = normalize_microsoft_event_for_sheet(raw_ev, customer_id, cal_id, customer_tz)
            if norm is None:
                continue
            key = _composite_key(
                norm["customer_id"], norm["provider"], norm["calendar_id"], norm["event_id"]
            )
            seen_keys.add(key)
            normalized_events.append(norm)

    # 4. Build batch update / append lists
    update_data: List[Dict[str, Any]] = []  # for batchUpdate
    append_rows: List[List[str]] = []        # for append

    for norm in normalized_events:
        key = _composite_key(
            norm["customer_id"], norm["provider"], norm["calendar_id"], norm["event_id"]
        )
        if key in index:
            row_num, existing = index[key]
            time_changed = existing[C_START_TIME] != norm["start_time"]

            new_row = list(existing)  # start from existing to preserve reminder flags
            new_row[C_TITLE] = norm["title"]
            new_row[C_NAME] = norm["name"] or existing[C_NAME]
            new_row[C_PHONE] = norm["phone"] or existing[C_PHONE]
            new_row[C_EMAIL] = norm["email"] or existing[C_EMAIL]
            new_row[C_START_TIME] = norm["start_time"]
            new_row[C_END_TIME] = norm["end_time"]
            new_row[C_STATUS] = norm["status"]
            new_row[C_TIMEZONE] = norm["timezone"]
            new_row[C_LAST_UPDATED] = norm["last_updated"]

            if time_changed:
                # Reset reminder flags — time changed, reminders need to be re-sent
                new_row[C_REMINDER_3D] = ""
                new_row[C_REMINDER_1D] = ""
                new_row[C_REMINDER_2H] = ""

            # Pad to NUM_COLS
            while len(new_row) < NUM_COLS:
                new_row.append("")

            col_end = chr(ord("A") + NUM_COLS - 1)
            update_data.append(
                {
                    "range": f"{SHEET_TAB}!A{row_num}:{col_end}{row_num}",
                    "values": [new_row],
                }
            )
        else:
            # New event — insert
            new_row = [
                norm["customer_id"],
                norm["provider"],
                norm["calendar_id"],
                norm["event_id"],
                norm["title"],
                norm["name"],
                norm["phone"],
                norm["email"],
                norm["start_time"],
                norm["end_time"],
                norm["status"],
                "",  # reminder_3d
                "",  # reminder_1d
                "",  # reminder_2h
                norm["timezone"],
                norm["last_updated"],
            ]
            append_rows.append(new_row)

    # 5. Mark cancelled: events in sheet (for this customer+provider) not in fetched set
    cancel_updates: List[Dict[str, Any]] = []
    col_end = chr(ord("A") + NUM_COLS - 1)
    for key, (row_num, existing) in index.items():
        if existing[C_CUSTOMER_ID] != customer_id or existing[C_PROVIDER] != provider:
            continue
        if key in seen_keys:
            continue
        if existing[C_STATUS] == "cancelled":
            continue
        new_row = list(existing)
        new_row[C_STATUS] = "cancelled"
        new_row[C_LAST_UPDATED] = _iso_z(now_utc)
        while len(new_row) < NUM_COLS:
            new_row.append("")
        cancel_updates.append(
            {
                "range": f"{SHEET_TAB}!A{row_num}:{col_end}{row_num}",
                "values": [new_row],
            }
        )

    # 6. Execute batch update (existing rows + cancellations)
    all_updates = update_data + cancel_updates
    if all_updates:
        await _sheets_post(
            "/values:batchUpdate",
            token,
            {
                "valueInputOption": "USER_ENTERED",
                "data": all_updates,
            },
        )

    # 7. Append new rows
    if append_rows:
        await _sheets_post(
            f"/values/{SHEET_TAB}!A:P:append"
            "?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS",
            token,
            {"values": append_rows},
        )

    # 8. Hard-delete rows where end_time < now - 1 day
    await _cleanup_old_rows(token, rows, now_utc)


async def _cleanup_old_rows(
    token: str,
    rows: List[List[str]],
    now_utc: datetime,
) -> None:
    """Delete rows where end_time is more than 1 day in the past."""
    cutoff = now_utc - timedelta(days=1)
    delete_requests = []

    # Collect rows to delete (in reverse order so indices stay valid)
    rows_to_delete = []
    for i, row in enumerate(rows):
        if i == 0:  # header
            continue
        padded = list(row) + [""] * (NUM_COLS - len(row))
        end_raw = padded[C_END_TIME]
        if not end_raw:
            continue
        try:
            end_utc = _parse_to_utc(end_raw, "UTC")
            if end_utc < cutoff:
                rows_to_delete.append(i)  # 0-based sheet index
        except Exception:
            continue

    if not rows_to_delete:
        return

    # Build deleteDimension requests in reverse order (high index first)
    for row_idx in sorted(rows_to_delete, reverse=True):
        delete_requests.append(
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": 0,  # first sheet; override via SHEETS_SHEET_ID env if needed
                        "dimension": "ROWS",
                        "startIndex": row_idx,
                        "endIndex": row_idx + 1,
                    }
                }
            }
        )

    if delete_requests:
        await _sheets_post(
            ":batchUpdate",
            token,
            {"requests": delete_requests},
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_to_utc(raw: str, fallback_tz: str = "UTC") -> datetime:
    from zoneinfo import ZoneInfo

    s = (raw or "").strip()
    if not s:
        raise ValueError("empty datetime")

    if s.endswith("Z") or "+" in s[10:] or (len(s) > 10 and "-" in s[10:]):
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    # All-day date like "2025-03-17"
    if len(s) == 10:
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)

    dt = datetime.fromisoformat(s)
    try:
        tz = ZoneInfo(fallback_tz)
    except Exception:
        tz = timezone.utc
    return dt.replace(tzinfo=tz).astimezone(timezone.utc)


def _ms_dt_to_utc(dt_obj: Dict[str, Any], fallback_tz: str = "UTC") -> Optional[datetime]:
    raw = (dt_obj.get("dateTime") or "").strip()
    tz_name = (dt_obj.get("timeZone") or fallback_tz).strip()
    if not raw:
        return None
    try:
        return _parse_to_utc(raw if ("+" in raw[10:] or raw.endswith("Z") or "-" in raw[10:]) else raw, tz_name)
    except Exception:
        return None
