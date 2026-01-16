import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

PACK_COST=int(os.getenv("PACK_MAMBUCK_COST","10"))
PACK_SHARD_COST=int(os.getenv("PACK_SHARD_COST", "100"))
BOX_COST=int(os.getenv("BOX_MAMBUCK_COST","200"))
BOX_SHARD_COST=int(os.getenv("BOX_SHARD_COST", "2000"))
PACKS_IN_BOX=24
BUNDLE_BOX_COST=int(os.getenv("BUNDLE_MAMBUCK_COST","350"))
BUNDLE_BOX_SHARD_COST=int(os.getenv("BUNDLE_SHARD_COST", "3500"))
TIN_COST=int(os.getenv("TIN_MAMBUCK_COST","70"))
TIN_SHARD_COST=int(os.getenv("TIN_SHARD_COST", "800"))
FROSTFIRE_BUNDLE_NAME="Frostfire Bundle"
SANDSTORM_BUNDLE_NAME="Sandstorm Bundle"
TEMPORAL_BUNDLE_NAME="Temporal Bundle"
SALE_DISCOUNT_PCT=10
def _parse_active_set_env() -> int:
    # Ensure we load the .env file even if this module is imported before bot.py
    # (or another entry point) has a chance to call load_dotenv. This keeps
    # CURRENT_ACTIVE_SET in sync with the developer's .env configuration when
    # running scripts or shells that import this module directly.
    dotenv_path = find_dotenv(usecwd=True)
    if not dotenv_path:
        # Fallback to a .env next to bot.py when the working directory is
        # elsewhere (e.g., service managers).
        dotenv_path = Path(__file__).resolve().parent.parent / ".env"

    if dotenv_path:
        load_dotenv(dotenv_path=dotenv_path)

    try:
        return int(os.getenv("CURRENT_ACTIVE_SET", "1"))
    except ValueError:
        return 1


CURRENT_ACTIVE_SET = _parse_active_set_env()

# Battleground-style duel team points configuration.
TEAM_BATTLEGROUND_START_POINTS = 500
TEAM_BATTLEGROUND_MIDPOINT = 500
TEAM_BATTLEGROUND_SEGMENT_SIZE = 200
DUEL_TEAM_TRANSFER_BASE = 20
DUEL_TEAM_TRANSFER_MIN = 5
DUEL_TEAM_TRANSFER_MAX = 50
DUEL_TEAM_SAME_TEAM_MULTIPLIER = 0.4
DUEL_TEAM_WIN_PCT_MULTIPLIER_MIN = 0.85
DUEL_TEAM_WIN_PCT_MULTIPLIER_MAX = 1.15
DUEL_TEAM_ACTIVITY_MULTIPLIER_MIN = 0.9
DUEL_TEAM_ACTIVITY_MULTIPLIER_MAX = 1.1
DUEL_TEAM_ACTIVITY_IMPACT = 0.2
DUEL_TEAM_ACTIVITY_MATCH_THRESHOLD = 5

# Daily sale layout: (rarity, number of entries)
SALE_LAYOUT = [
    ("ultra", 4),
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
        "key": "cards_super_rare_bundle",
        "weight": 0.25,
        "prize_type": "cards",
        "rarity": "SUPER RARE",
        "amount": 2,
        "description": ":rar_super: Super x2",
    },
    {
        "key": "card_ultra_rare",
        "weight": 0.15,
        "prize_type": "card",
        "rarity": "ULTRA RARE",
        "description": ":rar_ultra: Ultra",
    },
    {
        "key": "cards_ultra_rare_bundle",
        "weight": 0.10,
        "prize_type": "cards",
        "rarity": "ULTRA RARE",
        "amount": 2,
        "description": ":rar_ultra: Ultra x2",
    },
    {
        "key": "card_secret_rare",
        "weight": 0.05,
        "prize_type": "card",
        "rarity": "SECRET RARE",
        "description": ":rar_secret: Secret",
    },
    {
        "key": "shards_100_frostfire",
        "weight": 0.25,
        "prize_type": "shards",
        "amount": 100,
        "shard_type": "frostfire",
        "description": ":rar_frostfire:Shards x100",
    },
    {
        "key": "shards_500_frostfire",
        "weight": 0.10,
        "prize_type": "shards",
        "amount": 500,
        "shard_type": "frostfire",
        "description": ":rar_frostfire:Shards x500",
    },
    {
        "key": "mambucks_10",
        "weight": 0.10,
        "prize_type": "mambucks",
        "amount": 10,
        "description": "Mambucks x10",
    },
]

# Starter deck card sets are excluded from crafting/fragmenting.
STARTER_DECK_SET_NAMES = {
    "Starter Deck Water",
    "Starter Deck Fire",
}

# Which packs belong to which Set (uppercase pack names)
PACKS_BY_SET = {
    1: {"Blazing Genesis", "Storm of the Abyss"},  # Set 1 → Frostfire Shards
    2: {"Obsidian Empire", "Evolving Maelstrom"},   # Set 2 -> Sandstorm Shards
    3: {"Power of the Primordial", "Cyberstorm Crisis"}
}

BUNDLES = (
    #{
    #    "id": "frostfire",
    #    "name": FROSTFIRE_BUNDLE_NAME,
    #    "cost": BUNDLE_BOX_COST,
    #    "shard_cost": BUNDLE_BOX_SHARD_COST,
    #    "set_id": 1,
    #},
    #{
    #    "id": "sandstorm",
    #    "name": SANDSTORM_BUNDLE_NAME,
    #    "cost": BUNDLE_BOX_COST,
    #    "shard_cost": BUNDLE_BOX_SHARD_COST,
    #    "set_id": 2,
    #},
    #{
    #    "id": "temporal",
    #    "name": TEMPORAL_BUNDLE_NAME,
    #    "cost": BUNDLE_BOX_COST,
    #    "shard_cost": BUNDLE_BOX_SHARD_COST,
    #    "set_id": 3,
    #},
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

# Team roles / packs by set
TEAM_SETS = {
    1: {
        "order": ("Fire", "Water"),
        "teams": {
            "Fire": {"display": "Team Fire", "emoji": "🔥"},
            "Water": {"display": "Team Water", "emoji": "💧"},
        },
    },
    2: {
        "order": ("Wind", "Earth"),
        "teams": {
            "Wind": {
                "display": "Team Wind",
                "emoji": "🌪️",
                "pack": "Evolving Maelstrom",
            },
            "Earth": {
                "display": "Team Earth",
                "emoji": "⛰️",
                "pack": "Obsidian Empire",
            },
        },
    },
    3: {
        "order": ("Past", "Future"),
        "teams": {
            "Past": {
                "display": "Team Past",
                "emoji": "⌛",
                "pack": "Power of the Primordial",
            },
            "Future": {
                "display": "Team Future",
                "emoji": "🤖",
                "pack": "Cyberstorm Crisis",
            },
        },
    },
}

TEAM_ROLE_MAPPING = {
    "Starter Deck Water": "Water",
    "Starter Deck Fire": "Fire",
}
TEAM_ROLE_NAMES = frozenset({name for cfg in TEAM_SETS.values() for name in cfg.get("teams", {})})


def latest_team_set_id() -> int | None:
    team_sets = set(TEAM_SETS.keys())
    pack_sets = set(PACKS_BY_SET.keys())
    eligible = sorted(team_sets & pack_sets)
    if not eligible:
        return None

    if CURRENT_ACTIVE_SET in eligible:
        return CURRENT_ACTIVE_SET
    return eligible[-1]

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
    "common": 4,
    "rare": 40,
    "super": 100,
    "ultra": 300,
    "secret": 1500
}

SHARD_YIELD_BY_RARITY = {
    "common": 1,
    "rare": 10,
    "super": 25,
    "ultra": 75,
    "secret": 375
}