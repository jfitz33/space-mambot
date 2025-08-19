# core/util_norm.py (new or wherever you prefer)

def normalize_rarity(s: str | None) -> str:
    s = (s or "").strip().lower()
    aliases = {
        "common": "common",
        "c": "common",
        "uncommon": "uncommon",
        "u": "uncommon",
        "rare": "rare",
        "r": "rare",
        "super": "super",
        "sr": "super",
        "ultra": "ultra",
        "ur": "ultra",
        "secret": "secret",
        "secr": "secret",
        "starlight": "starlight",
        "sl": "starlight",
    }
    return aliases.get(s, s)

def normalize_set_name(s: str | None) -> str:
    s = (s or "").strip()
    if s.lower().startswith("set:"):
        s = s[4:].strip()
    return s

def blank_to_none(s):
    return None if s is None or str(s).strip() == "" else str(s).strip()
