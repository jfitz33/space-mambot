"""Shared helpers for daily rollover timing (default midnight ET).

Environment variables:
- DAILY_ROLLOVER_TIME: "HH:MM" (24h). Defaults to "00:00".
- DAILY_ROLLOVER_TZ: IANA timezone name. Defaults to "America/New_York".
"""
from __future__ import annotations

import os
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo


def _safe_int(val: str, *, minimum: int, maximum: int, default: int) -> int:
    try:
        num = int(val)
    except Exception:
        return default
    return max(minimum, min(maximum, num))


def _parse_rollover_time(raw: str, tz: ZoneInfo) -> time:
    parts = (raw or "").split(":", maxsplit=1)
    hour = _safe_int(parts[0] if parts else "0", minimum=0, maximum=23, default=0)
    minute = _safe_int(parts[1] if len(parts) > 1 else "0", minimum=0, maximum=59, default=0)
    return time(hour=hour, minute=minute, tzinfo=tz)


_ROLLOVER_TZ_NAME = os.getenv("DAILY_ROLLOVER_TZ", "America/New_York")
ROLLOVER_TZ = ZoneInfo(_ROLLOVER_TZ_NAME)
_ROLLOVER_TIME_STR = os.getenv("DAILY_ROLLOVER_TIME", "00:00")
_ROLLOVER_TIME = _parse_rollover_time(_ROLLOVER_TIME_STR, ROLLOVER_TZ)


def rollover_timezone() -> ZoneInfo:
    """Return the configured rollover timezone (default America/New_York)."""
    return ROLLOVER_TZ


def rollover_time() -> time:
    """Return the configured rollover time of day (default 00:00)."""
    return _ROLLOVER_TIME


def rollover_label() -> str:
    return f"{_ROLLOVER_TIME.strftime('%H:%M')} {_ROLLOVER_TZ_NAME}"


def rollover_day_key(dt: datetime | None = None) -> str:
    """Return the YYYYMMDD day key using the configured rollover timezone."""
    current = (dt or datetime.now(ROLLOVER_TZ)).astimezone(ROLLOVER_TZ)
    return current.strftime("%Y%m%d")


def next_rollover_datetime(from_dt: datetime | None = None) -> datetime:
    now = (from_dt or datetime.now(ROLLOVER_TZ)).astimezone(ROLLOVER_TZ)
    today_rollover = datetime.combine(now.date(), _ROLLOVER_TIME)
    if today_rollover <= now:
        today_rollover += timedelta(days=1)
    return today_rollover


def seconds_until_next_rollover(from_dt: datetime | None = None) -> float:
    target = next_rollover_datetime(from_dt)
    now = (from_dt or datetime.now(ROLLOVER_TZ)).astimezone(ROLLOVER_TZ)
    return max(1.0, (target - now).total_seconds())