from datetime import datetime, date, timedelta

from core.daily_rollover import rollover_day, rollover_timezone

def now_et() -> datetime:
    return datetime.now(rollover_timezone())

def rollover_date(dt: datetime | None = None) -> date:
    return rollover_day(dt)


def daily_key(dt: datetime | date | None = None) -> str:
    if isinstance(dt, date) and not isinstance(dt, datetime):
        d = dt
    else:
        d = rollover_day(dt)
    return f"D:{d.isoformat()}"

def weekly_key(dt: datetime | None = None) -> str:
    # Weekly boundary: Sunday night midnight → Monday 00:00 ET
    d = rollover_day(dt)
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
