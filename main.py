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

    # ---- Time window we will scan ----
    now_local = datetime.now(tz).replace(second=0, microsecond=0)
    start_day_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_day_local = (start_day_local + timedelta(days=days)).replace(hour=23, minute=59, second=59, microsecond=0)

    time_min_utc = start_day_local.astimezone(timezone.utc)
    time_max_utc = end_day_local.astimezone(timezone.utc)

    # ---- 1) FREEBUSY (primary source) ----
    fb = freebusy_raw(access_token, calendar_ids, time_min_utc, time_max_utc, tz_name)
    if fb["statusCode"] != 200 or not fb["json"]:
        return {
            "ok": False,
            "reason": "freebusy_failed",
            "statusCode": fb["statusCode"],
            "googleResponseText": fb["text"],
            "requestBody": fb["requestBody"],
        }

    calendars_obj = fb["json"].get("calendars", {})
    busy_intervals: List[Dict[str, datetime]] = []

    # Busy from freebusy
    for _, info in calendars_obj.items():
        for b in (info.get("busy") or []):
            s, e = b.get("start"), b.get("end")
            if not s or not e:
                continue
            s_utc = parse_iso_to_utc(s)
            e_utc = parse_iso_to_utc(e)
            if e_utc > s_utc:
                busy_intervals.append({"start": s_utc, "end": e_utc})

    # ---- 2) EVENTS LIST (fallback — catches cases freebusy misses) ----
    # This adds 1 API call per calendar, but for 3 calendars it’s totally fine.
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
            # Don't fail availability just because one calendar list call failed
            continue

        items = (r.json() or {}).get("items", []) or []
        for ev in items:
            if ev.get("status") == "cancelled":
                continue

            # Treat all non-cancelled events as busy (you said everything is marked Busy)
            start = (ev.get("start") or {}).get("dateTime")
            end = (ev.get("end") or {}).get("dateTime")

            # skip all-day events (date only) for now
            if not start or not end:
                continue

            try:
                s_utc = parse_iso_to_utc(start)
                e_utc = parse_iso_to_utc(end)
                if e_utc > s_utc:
                    busy_intervals.append({"start": s_utc, "end": e_utc})
            except Exception:
                continue

    # ---- Merge busy across ALL calendars ----
    busy_merged = merge_intervals(busy_intervals)

    # ---- Generate available slots ----
    dur = timedelta(minutes=duration_minutes)
    available: List[Dict[str, str]] = []

    cur_day = start_day_local.date()
    last_day = (start_day_local + timedelta(days=days)).date()

    while cur_day <= last_day:
        day0_local = datetime(cur_day.year, cur_day.month, cur_day.day, 0, 0, tzinfo=tz)
        weekday = day0_local.weekday()

        if weekday in work_days:
            win_start_local = day0_local.replace(hour=work_start_hour, minute=0)
            win_end_local = day0_local.replace(hour=work_end_hour, minute=0)

            # IMPORTANT: clamp today's start to now so we don't suggest past times
            if day0_local.date() == now_local.date() and now_local > win_start_local:
                win_start_local = now_local

            # ignore days where the window is invalid
            if win_end_local <= win_start_local:
                cur_day = (day0_local + timedelta(days=1)).date()
                continue

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
        "window": {"timeMinUtc": iso_z(time_min_utc), "timeMaxUtc": iso_z(time_max_utc)},
        "busyMergedCount": len(busy_merged),
        "availableCount": len(available),
        "suggestions": suggestions,
        "available": available[:500],
    }
def slot_overlaps_busy(slot_start: datetime, slot_end: datetime, busy: List[Dict[str, datetime]]) -> bool:
    for b in busy:
        if slot_start < b["end"] and slot_end > b["start"]:
            return True
    return False
