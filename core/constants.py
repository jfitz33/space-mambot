import os

PACK_COST=int(os.getenv("PACK_MAMBUCK_COST","10"))
PACK_SHARD_COST=int(os.getenv("PACK_SHARD_COST", "100"))
BOX_COST=int(os.getenv("BOX_MAMBUCK_COST","200"))
BOX_SHARD_COST=int(os.getenv("BOX_SHARD_COST", "2000"))
PACKS_IN_BOX=24
BUNDLE_BOX_COST=int(os.getenv("BUNDLE_MAMBUCK_COST","350"))
BUNDLE_BOX_SHARD_COST=int(os.getenv("BUNDLE_SHARD_COST", "3500"))
TIN_COST=int(os.getenv("TIN_MAMBUCK_COST","80"))
TIN_SHARD_COST=int(os.getenv("TIN_SHARD_COST", "800"))
FROSTFIRE_BUNDLE_NAME="Frostfire Bundle"
SANDSTORM_BUNDLE_NAME="Sandstorm Bundle"
TEMPORAL_BUNDLE_NAME="Temporal Bundle"
SALE_DISCOUNT_PCT=10

# Daily sale layout: (rarity, number of entries)
SALE_LAYOUT = [
    ("super", 3),
    ("ultra", 1),
    ("secret", 1),
]

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

# Slots (gamba) configuration
GAMBA_DEFAULT_SHARD_SET_ID = 1
GAMBA_PRIZES = [
    {
        "key": "card_super_rare",
        "weight": 0.40,
        "prize_type": "card",
        "rarity": "SUPER RARE",
        "description": "Random Super Rare Card",
    },
    {
        "key": "card_ultra_rare",
        "weight": 0.15,
        "prize_type": "card",
        "rarity": "ULTRA RARE",
        "description": "Random Ultra Rare Card",
    },
    {
        "key": "card_secret_rare",
        "weight": 0.05,
        "prize_type": "card",
        "rarity": "SECRET RARE",
        "description": "Random Secret Rare Card",
    },
    {
        "key": "shards_100",
        "weight": 0.20,
        "prize_type": "shards",
        "amount": 100,
        "description": "100 Shards",
    },
    {
        "key": "mambucks_10",
        "weight": 0.20,
        "prize_type": "mambucks",
        "amount": 10,
        "description": "10 Mambucks",
    },
]

# Starter deck card sets are excluded from crafting/fragmenting.
STARTER_DECK_SET_NAMES = {
    "Cult of the Mambo",
    "Hellfire Heretics",
}

# Which packs belong to which Set (uppercase pack names)
PACKS_BY_SET = {
    1: {"Blazing Genesis", "Storm of the Abyss"},  # Set 1 → Frostfire Shards
    2: {"Obsidian Empire", "Evolving Maelstrom"},   # Set 2 -> Sandstorm Shards
    3: {"Power of the Primordial", "Cyberstorm Crisis"}
}

BUNDLES = (
    {
        "id": "frostfire",
        "name": FROSTFIRE_BUNDLE_NAME,
        "cost": BUNDLE_BOX_COST,
        "shard_cost": BUNDLE_BOX_SHARD_COST,
        "set_id": 1,
    },
    {
        "id": "sandstorm",
        "name": SANDSTORM_BUNDLE_NAME,
        "cost": BUNDLE_BOX_COST,
        "shard_cost": BUNDLE_BOX_SHARD_COST,
        "set_id": 2,
    },
    {
        "id": "temporal",
        "name": TEMPORAL_BUNDLE_NAME,
        "cost": BUNDLE_BOX_COST,
        "shard_cost": BUNDLE_BOX_SHARD_COST,
        "set_id": 3,
    },
)

# Bundles that should be grouped with a set (uppercase bundle names)
BUNDLES_BY_SET = {bundle["name"].upper(): bundle["set_id"] for bundle in BUNDLES}
BUNDLE_NAME_INDEX = {bundle["name"].casefold(): bundle for bundle in BUNDLES}

def _normalize_pack_name(pack_name: str) -> str:
    import re

    # Uppercase, strip whitespace, collapse runs, remove punctuation variations.
    cleaned = re.sub(r"[^A-Z0-9 ]+", " ", (pack_name or "").upper())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


_PACKS_BY_SET_NORMALIZED = {
    sid: {_normalize_pack_name(name) for name in names}
    for sid, names in PACKS_BY_SET.items()
}

# Team roles
TEAM_ROLE_MAPPING = {
    "Cult of the Mambo": "Water",
    "Hellfire Heretics": "Fire",
}
TEAM_ROLE_NAMES = frozenset(TEAM_ROLE_MAPPING.values())

def set_id_for_pack(pack_name: str) -> int | None:
    if not pack_name:
        return None
    p = _normalize_pack_name(pack_name)
    if not p:
        return None
    for sid, names in _PACKS_BY_SET_NORMALIZED.items():
        if p in names:
            return sid
        # Allow pack names that append qualifiers like "(1st Edition)" or suffixes.
        for base in names:
            if base and (p.startswith(base + " ") or p.endswith(" " + base) or f" {base} " in p):
                return sid
    bundle_sid = BUNDLES_BY_SET.get(p)
    if bundle_sid is not None:
        return bundle_sid
    return None

def pack_names_for_set(state, set_id: int) -> list[str]:
    packs_index = getattr(state, "packs_index", None) or {}
    names = [name for name in packs_index.keys() if set_id_for_pack(name) == set_id]
    return sorted(names, key=str.casefold)

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