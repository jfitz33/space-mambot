# core/cards_shop.py
import csv, glob, hashlib
from typing import Dict, Optional, Iterable, Tuple

# Canonicalize rarities (keep starlight distinct)
_CANON_RARITY_MAP = {
    "c": "common", "common": "common",
    "u": "uncommon", "uncommon": "uncommon",
    "r": "rare", "rare": "rare",
    "sr": "super", "super": "super", "super rare": "super",
    "ur": "ultra", "ultra": "ultra", "ultra rare": "ultra",
    "sec": "secret", "secret": "secret", "secret rare": "secret",
    "starlight": "starlight", "starlight rare": "starlight",
}

def canonicalize_rarity(raw: str) -> str:
    key = (raw or "").strip().lower()
    return _CANON_RARITY_MAP.get(key, key)

def _normalize_row(row: dict) -> dict:
    """Normalize a CSV row or packs_index card dict to a common shape."""
    return {
        "name":       row.get("cardname") or row.get("name") or "",
        "rarity":     canonicalize_rarity(row.get("cardrarity") or row.get("rarity") or ""),
        "set":        row.get("cardset") or row.get("set") or "",
        "code":       row.get("cardcode") or row.get("code") or None,
        "id":         row.get("cardid") or row.get("id") or None,
        # Preserve raw fields (your db_add_cards may expect these)
        "cardname":   row.get("cardname"),
        "cardrarity": row.get("cardrarity"),
        "cardset":    row.get("cardset"),
        "cardcode":   row.get("cardcode"),
        "cardid":     row.get("cardid"),
    }

def _print_key_from_fields(name: str, rarity: str, set_: str, code: Optional[str], cid: Optional[str]) -> str:
    base = f"{name.lower()}|{rarity.lower()}|{set_.lower()}|{(code or '').lower()}|{(cid or '').lower()}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]

def _sig_for_resolution(name: str, rarity: str, code: Optional[str], cid: Optional[str]) -> Tuple[str, str, str, str]:
    """Signature that ignores SET to help resolve it later."""
    return (
        (name or "").strip().lower(),
        (rarity or "").strip().lower(),
        (code or "").strip().lower(),
        (cid  or "").strip().lower(),
    )

def ensure_shop_index(state) -> None:
    """
    Build in-memory indexes:
      - _shop_print_by_key: exact 'printing' entries (key includes set)
      - _shop_sig_to_set: (name, rarity, code, id) -> non-empty set
    Sources:
      - state.packs_index
      - state.starters_index
    Handles meta that are dicts (with by_rarity/cards/rows) or plain lists.
    """
    if getattr(state, "_shop_print_by_key", None) is not None:
        return

    state._shop_print_by_key = {}
    state._shop_sig_to_set = {}

    def _ingest_row(raw_row: dict, default_set: str | None):
        # make a copy so we don't mutate shared structures
        row = dict(raw_row) if isinstance(raw_row, dict) else {}
        set_present = (row.get("cardset") or row.get("set") or "").strip()
        if not set_present and default_set:
            row["cardset"] = default_set  # inject pack/starter name as the set

        card = _normalize_row(row)
        name, rarity, set_, code, cid = card["name"], card["rarity"], card["set"], card["code"], card["id"]

        if set_:
            sig = _sig_for_resolution(name, rarity, code, cid)
            state._shop_sig_to_set[sig] = set_

        k = _print_key_from_fields(name, rarity, set_, code, cid)
        state._shop_print_by_key.setdefault(k, card)

    def _ingest_container(container, container_label: str):
        if not container:
            return
        # container is expected to be a dict (name -> meta)
        for parent_name, meta in container.items():
            parent_name_str = (str(parent_name) or "").strip()
            default_set = parent_name_str  # use the key as the default set name

            # meta can be a dict or a list
            if isinstance(meta, list):
                for row in meta:
                    _ingest_row(row, default_set)
                continue

            if isinstance(meta, dict):
                # try by_rarity first
                by_rarity = meta.get("by_rarity")
                meta_name = (meta.get("name") or parent_name_str).strip()
                eff_default_set = meta_name or default_set

                if isinstance(by_rarity, dict) and by_rarity:
                    for pool in by_rarity.values():
                        for row in (pool or []):
                            _ingest_row(row, eff_default_set)

                # flat lists commonly stored as 'cards' or 'rows'
                cards_list = meta.get("cards")
                if isinstance(cards_list, list):
                    for row in cards_list:
                        _ingest_row(row, eff_default_set)

                rows_list = meta.get("rows")
                if isinstance(rows_list, list):
                    for row in rows_list:
                        _ingest_row(row, eff_default_set)

    # Dedupe: keep the best printing per (name, rarity, code, id) to purge any stale data or faulty imports
    tmp = {}
    to_delete = []
    for k, card in state._shop_print_by_key.items():
        name = (card.get("name") or card.get("cardname") or "").strip()
        rarity = (card.get("rarity") or card.get("cardrarity") or "").strip()
        set_ = (card.get("set") or card.get("cardset") or "").strip()
        code = (card.get("code") or card.get("cardcode") or "").strip()
        cid  = (card.get("id") or card.get("cardid") or "").strip()
        sig = _sig_for_resolution(name, rarity, code, cid)
        score = (1 if set_ else 0, 1 if code else 0, 1 if cid else 0)
        prev = tmp.get(sig)
        if prev is None or score > prev[0]:
            # mark previous (if any) for deletion
            if prev is not None:
                to_delete.append(prev[1])
            tmp[sig] = (score, k)
        else:
            to_delete.append(k)

    # drop the losers
    for k in to_delete:
        state._shop_print_by_key.pop(k, None)

    # rebuild resolver map to align with survivors
    state._shop_sig_to_set.clear()
    for card in state._shop_print_by_key.values():
        set_ = (card.get("set") or card.get("cardset") or "").strip()
        if not set_:
            continue
        name = (card.get("name") or card.get("cardname") or "").strip()
        rarity = (card.get("rarity") or card.get("cardrarity") or "").strip()
        code = (card.get("code") or card.get("cardcode") or "").strip()
        cid  = (card.get("id") or card.get("cardid") or "").strip()
        sig = _sig_for_resolution(name, rarity, code, cid)
        state._shop_sig_to_set[sig] = set_


    # Harvest packs and starters (whatever you maintain on state)
    packs_index = getattr(state, "packs_index", None)
    if isinstance(packs_index, dict):
        _ingest_container(packs_index, "packs_index")

    starters_index = getattr(state, "starters_index", None)
    if isinstance(starters_index, dict):
        _ingest_container(starters_index, "starters_index")

def shop_load_csvs_into_index(state, glob_pattern: str) -> int:
    """
    Optional: load CSV files (starters, extra sets) into the shop indexes.
    Ensures we have 'set' info to resolve printings.
    """
    if getattr(state, "_shop_print_by_key", None) is None:
        # initialize if not yet built
        state._shop_print_by_key = {}
        state._shop_sig_to_set = {}

    count = 0
    for path in glob.glob(glob_pattern):
        with open(path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                card = _normalize_row(row)
                name, rarity, set_, code, cid = card["name"], card["rarity"], card["set"], card["code"], card["id"]

                # resolution signature
                sig = _sig_for_resolution(name, rarity, code, cid)
                if set_:
                    state._shop_sig_to_set[sig] = set_

                # store printing entry
                k = _print_key_from_fields(name, rarity, set_, code, cid)
                state._shop_print_by_key.setdefault(k, card)
        count += 1
    return count

def find_card_by_print_key(state, key: str) -> Optional[dict]:
    ensure_shop_index(state)
    return getattr(state, "_shop_print_by_key", {}).get(key)

def resolve_card_set(state, card: dict) -> Optional[str]:
    """
    Resolve the set for a card printing without using a fallback.
    Strategy:
      1) If card already has a non-empty set, return it.
      2) Otherwise, look up by (name, rarity, code, id) in _shop_sig_to_set.
      3) As a last resort, scan known printings for same signature with non-empty set.
      4) If still unknown, return None (block buy/sell).
    """
    set_ = card.get("set") or card.get("cardset") or ""
    if set_:
        return set_

    ensure_shop_index(state)
    name = card.get("name") or card.get("cardname") or ""
    rarity = canonicalize_rarity(card.get("rarity") or card.get("cardrarity") or "")
    code = card.get("code") or card.get("cardcode") or ""
    cid  = card.get("id") or card.get("cardid") or ""
    sig = _sig_for_resolution(name, rarity, code, cid)

    # Try prebuilt signature map
    known = getattr(state, "_shop_sig_to_set", {}).get(sig)
    if known:
        return known

    # Fallback scan (just in case)
    for other in getattr(state, "_shop_print_by_key", {}).values():
        if (
            (other.get("name") or other.get("cardname") or "").strip().lower() == name.strip().lower()
            and canonicalize_rarity(other.get("rarity") or other.get("cardrarity") or "") == rarity
            and (other.get("code") or other.get("cardcode") or "").strip().lower() == code.strip().lower()
            and (other.get("id") or other.get("cardid") or "").strip().lower() == cid.strip().lower()
        ):
            if other.get("set") or other.get("cardset"):
                return other.get("set") or other.get("cardset")

    return None

def get_card_rarity(card: dict) -> str:
    return canonicalize_rarity(card.get("rarity") or card.get("cardrarity") or "")

def card_label(card: dict) -> str:
    """Human-friendly label for UI."""
    name = card.get("name") or card.get("cardname") or "Unknown"
    bits = []
    if card.get("set") or card.get("cardset"): bits.append(card.get("set") or card.get("cardset"))
    if card.get("rarity") or card.get("cardrarity"): bits.append(get_card_rarity(card))
    if card.get("code") or card.get("cardcode"): bits.append(card.get("code") or card.get("cardcode"))
    suffix = " · ".join(bits)
    return (f"{name} — {suffix}" if suffix else name)[:100]

def print_key_for_fields(name: str, rarity: str, set_name: str, code: str | None, cid: str | None) -> str:
    base = f"{(name or '').lower()}|{(rarity or '').lower()}|{(set_name or '').lower()}|{(code or '').lower()}|{(cid or '').lower()}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]

def register_print_if_missing(state, card: dict) -> str:
    """
    Ensure an exact printing exists in the shop index; return its print_key.
    Accepts a dict with keys matching your CSV fields: cardname, cardrarity, cardset, cardcode, cardid.
    """
    ensure_shop_index(state)
    norm = _normalize_row(card)  # preserves/normalizes set/rarity/code/id
    k = print_key_for_fields(norm["name"], norm["rarity"], norm["set"], norm["code"], norm["id"])
    if k not in state._shop_print_by_key:
        state._shop_print_by_key[k] = norm
        # improve resolver map for future lookups
        if norm["set"]:
            sig = _sig_for_resolution(norm["name"], norm["rarity"], norm["code"], norm["id"])
            state._shop_sig_to_set[sig] = norm["set"]
    return k