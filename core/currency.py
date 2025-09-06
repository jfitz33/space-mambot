import os
from typing import Optional
from core.constants import PACKS_BY_SET

# Configure shard display names (you can add emoji, short names, etc.)
SHARD_SET_NAMES = {
    1: "Elemental Shards",
    2: "Astral Shards",
    # 3: "Techno Shards",
}

def shard_set_name(set_id: int, short: bool = False) -> str:
    # In case you later want short names like "Elementals"
    return SHARD_SET_NAMES.get(int(set_id), f"Shards (Set {int(set_id)})")

def shards_label(amount: int, set_id: int) -> str:
    name = shard_set_name(set_id)
    # tweak pluralization if you want: e.g., "1 Shard" vs "2 Shards"
    return f"{amount} {name}"

def mambucks_label(amount: int) -> str:
    return f"{amount} Mambucks"

# Resolve a card/set name → set_id (uses your constants)
def set_id_for_pack(pack_name: Optional[str]) -> Optional[int]:
    if not pack_name:
        return None
    p = (pack_name or "").strip().upper()
    for sid, names in (PACKS_BY_SET or {}).items():
        if p in names:
            return sid
    return None

def get_shard_exchange_rate() -> tuple[int, int]:
    """
    Read SHARD_EXCHANGE_RATE from env as 'A:B' meaning A source -> B target.
    Defaults to '1:2'.
    """
    raw = os.getenv("SHARD_EXCHANGE_RATE", "2:1")
    try:
        a, b = raw.split(":")
        num = max(1, int(a))
        den = max(1, int(b))
    except Exception:
        num, den = 1, 2
    return num, den
