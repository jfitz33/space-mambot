"""Central feature flags for limited-time events and rollouts."""
import os

def is_set1_week1_locked() -> bool:
    """Return True when week 1 launch restrictions are active.

    Defaults to the DAILY_DUEL_WEEK1_ENABLE toggle for backwards compatibility
    but allows an explicit SET1_WEEK1_LOCKED override. Set either env var to "0"
    after week 1 to re-enable normal shop features.
    """

    override = os.getenv("SET1_WEEK1_LOCKED")
    if override is not None:
        return override == "1"
    return os.getenv("DAILY_DUEL_WEEK1_ENABLE", "1") == "1"