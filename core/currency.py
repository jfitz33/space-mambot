# Configure shard display names (you can add emoji, short names, etc.)
SHARD_SET_NAMES = {
    1: "Frostfire Shards",
    2: "Sandstorm Shards",
    3: "Temporal Shards",
}

def shard_set_name(set_id: int, short: bool = False) -> str:
    # In case you later want short names like "Frostfire"
    return SHARD_SET_NAMES.get(int(set_id), f"Shards (Set {int(set_id)})")

def shards_label(amount: int, set_id: int) -> str:
    name = shard_set_name(set_id)
    # tweak pluralization if you want: e.g., "1 Shard" vs "2 Shards"
    return f"{amount} {name}"

def mambucks_label(amount: int) -> str:
    return f"{amount} Mambucks"

