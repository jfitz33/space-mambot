"""Shared helpers for daily rollover timing (default midnight ET).

Environment variables:
- DAILY_ROLLOVER_TIME: "HH:MM" (24h). Defaults to "00:00".
- DAILY_ROLLOVER_TZ: IANA timezone name. Defaults to "America/New_York".
"""
from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv, find_dotenv

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


_DOTENV_LOADED = False


def _ensure_dotenv_loaded() -> None:
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return
    dotenv_path = find_dotenv(usecwd=True)
    if not dotenv_path:
        dotenv_path = Path(__file__).resolve().parent.parent / ".env"
    if dotenv_path:
        load_dotenv(dotenv_path=dotenv_path)
    _DOTENV_LOADED = True


def _read_env(name: str, default: str) -> str:
    if name in os.environ:
        return os.environ.get(name, "") or ""
    return default


def _rollover_config() -> tuple[ZoneInfo, time, str]:
    _ensure_dotenv_loaded()
    tz_name = _read_env("DAILY_ROLLOVER_TZ", "America/New_York")
    tz = ZoneInfo(tz_name)
    time_raw = _read_env("DAILY_ROLLOVER_TIME", "00:00")
    rollover_time = _parse_rollover_time(time_raw, tz)
    return tz, rollover_time, tz_name


def rollover_timezone() -> ZoneInfo:
    """Return the configured rollover timezone (default America/New_York)."""
    tz, _, _ = _rollover_config()
    return tz


def rollover_time() -> time:
    """Return the configured rollover time of day (default 00:00)."""
    _, rollover_time, _ = _rollover_config()
    return rollover_time


def rollover_label() -> str:
    tz, rollover_time, tz_name = _rollover_config()
    return f"{rollover_time.strftime('%H:%M')} {tz_name}"


def rollover_day(dt: datetime | None = None) -> date:
    tz, rollover_time, _ = _rollover_config()
    current = (dt or datetime.now(tz)).astimezone(tz)
    today_rollover = datetime.combine(current.date(), rollover_time)
    if current < today_rollover:
        return current.date() - timedelta(days=1)
    return current.date()


def rollover_day_key(dt: datetime | None = None) -> str:
    """Return the YYYYMMDD day key using the configured rollover timezone."""
    return rollover_day(dt).strftime("%Y%m%d")



def next_rollover_datetime(from_dt: datetime | None = None) -> datetime:
    tz, rollover_time, _ = _rollover_config()
    now = (from_dt or datetime.now(tz)).astimezone(tz)
    today_rollover = datetime.combine(now.date(), rollover_time)
    if today_rollover <= now:
        today_rollover += timedelta(days=1)
    return today_rollover


def seconds_until_next_rollover(from_dt: datetime | None = None) -> float:
    target = next_rollover_datetime(from_dt)
    tz = rollover_timezone()
    now = (from_dt or datetime.now(tz)).astimezone(tz)
    return max(1.0, (target - now).total_seconds())