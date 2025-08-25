from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None

def get_et_tz():
    """
    Preferred: IANA 'America/New_York' via ZoneInfo (+ tzdata on Windows).
    Fallback: fixed UTC-5 (no DST) so the bot still runs, but boundaries will be off during DST.
    """
    if ZoneInfo:
        try:
            return ZoneInfo("America/New_York")
        except Exception:
            pass
    return timezone(timedelta(hours=-5))

ET = get_et_tz()

def now_et() -> datetime:
    return datetime.now(ET)

def daily_key(dt: datetime | None = None) -> str:
    d = (dt or now_et()).date()
    return f"D:{d.isoformat()}"

def weekly_key(dt: datetime | None = None) -> str:
    # Weekly boundary: Sunday night midnight → Monday 00:00 ET
    d = (dt or now_et()).date()
    monday = d - timedelta(days=d.weekday())  # Monday date (Mon=0)
    iso_year, iso_week, _ = monday.isocalendar()
    return f"W:{iso_year}-{iso_week:02d}"

def period_key_for_category(cat: str, dt: datetime | None = None) -> str:
    cat = (cat or "").lower()
    if cat == "daily":
        return daily_key(dt)
    if cat == "weekly":
        return weekly_key(dt)
    return "P"  # permanent
