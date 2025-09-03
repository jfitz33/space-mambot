PACK_COST=10
BOX_COST=200
PACKS_IN_BOX=24
SALE_DISCOUNT_PCT=10

RARITY_ORDER = ["common", "rare", "super", "ultra", "secret", "starlight"]
RARITY_ALIASES = {
    "c": "common", "comm": "common",
    "r": "rare",
    "sr": "super", "super rare": "super",
    "ur": "ultra", "ultra rare": "ultra",
    "secr": "secret", "secret rare": "secret",
    "starlight rare": "starlight", "slr": "starlight",
}
FRAGMENTABLE_RARITIES = ["common", "rare", "super", "ultra", "secret"]

# Which packs belong to which Set (uppercase pack names)
PACKS_BY_SET = {
    1: {"FIRE", "WATER"},  # Set 1 → Elemental Shards
    # 2: {"...","..."},    # Add future sets here
}

def set_id_for_pack(pack_name: str) -> int | None:
    if not pack_name:
        return None
    p = (pack_name or "").strip().upper()
    for sid, names in PACKS_BY_SET.items():
        if p in names:
            return sid
    return None

# Shard economy
CRAFT_COST_BY_RARITY = {
    "common": 5,
    "rare": 60,
    "super": 90,
    "ultra": 300,
    "secret": 1500
}

SHARD_YIELD_BY_RARITY = {
    "common": 1,
    "rare": 20,
    "super": 30,
    "ultra": 100,
    "secret": 500
}