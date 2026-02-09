import os, csv, random
from collections import defaultdict
from core.state import AppState
from core.db import db_add_cards
from core.constants import PACKS_IN_BOX
from typing import List, Dict

RARITY_MAP = {
    "common":"common","uncommon":"uncommon","rare":"rare",
    "super":"super","super rare":"super","ultra":"ultra",
    "ultra rare":"ultra","secret":"secret","secret rare":"secret",
}
RARITY_ORDER = ["secret","ultra","super","rare","uncommon","common"]

# Build a rank map from your existing highest->lowest ordering
RARITY_INDEX = {r: i for i, r in enumerate(RARITY_ORDER)}  # lower index = higher rarity

def _cap_prefs_to_top(top_rarity: str, prefs: List[str]) -> List[str]:
    """
    Keep only rarities that are NOT higher than top_rarity.
    With highest->lowest order, that means index >= index(top_rarity).
    """
    top_idx = RARITY_INDEX[top_rarity]
    return [r for r in prefs if RARITY_INDEX.get(r, 999) >= top_idx]

def open_pack_with_guaranteed_top_from_csv(state, pack_name: str, top_rarity: str) -> list[dict]:
    """
    Build ONE pack (same 7+1+1 structure as open_pack_from_csv) whose highest rarity is exactly `top_rarity`.
      - 7 base pulls: from 'common' pool or capped fallback (never above top_rarity)
      - 1 'rare' slot: from 'rare' pool or capped fallback (never above top_rarity)
      - 1 top slot: forced from `top_rarity` pool
    Returns: list[dict] for a single pack (9 cards), same shape as open_pack_from_csv.
    """
    if pack_name not in state.packs_index:
        raise ValueError(f"Unknown pack '{pack_name}'.")
    by_rarity: Dict[str, list[dict]] = state.packs_index[pack_name]["by_rarity"]

    if not by_rarity.get(top_rarity):
        raise ValueError(f"No cards at rarity '{top_rarity}' for pack '{pack_name}'.")

    pulls: list[dict] = []

    # ---- 7 "common" slots (fallback upward, but cap at top_rarity) ----------
    # Your original upward fallback order for this block was:
    # ["uncommon","rare","super","ultra","secret"]
    base_upward = ["uncommon", "rare", "super", "ultra", "secret"]
    capped_base_upward = _cap_prefs_to_top(top_rarity, base_upward)
    # Try true 'common' first; if missing, fallback within the cap
    pool = by_rarity.get("common") or _fallback_pool(by_rarity, capped_base_upward) or by_rarity[top_rarity]
    for _ in range(7):
        pulls.append(_weighted_pick(pool))

    # ---- 1 "rare" slot (fallback upward, but cap at top_rarity) -------------
    # Your original order for this block was:
    # ["super","ultra","secret","uncommon","common"]  (tries higher first, then lower)
    rare_prefs = ["super", "ultra", "secret", "uncommon", "common"]
    capped_rare_prefs = _cap_prefs_to_top(top_rarity, rare_prefs)
    pool = by_rarity.get("rare") or _fallback_pool(by_rarity, capped_rare_prefs) or by_rarity[top_rarity]
    pulls.append(_weighted_pick(pool))

    # ---- 1 guaranteed top slot ----------------------------------------------
    pulls.append(random.choice(by_rarity[top_rarity]))

    return pulls

def open_box_from_csv(state, pack_name: str) -> list[list[dict]]:
    """Open a full box (PACKS_IN_BOX packs) for the given pack name."""
    per_pack: list[list[dict]] = []
    for i in range(1, PACKS_IN_BOX + 1):
        top = "super" if i <= 18 else ("ultra" if i <= 23 else "secret")
        per_pack.append(
            open_pack_with_guaranteed_top_from_csv(state, pack_name, top_rarity=top)
        )
    return per_pack

def normalize_rarity(s: str) -> str:
    return RARITY_MAP.get((s or "").strip().lower(), "rare")

def load_packs_from_csv(state: AppState):
    required = ["cardname","cardq","cardrarity","card_edition","cardset","cardcode","cardid","print_id"]
    packs: dict[str, dict] = {}

    for fname in os.listdir(state.packs_dir):
        if not fname.lower().endswith(".csv"):
            continue
        path = os.path.join(state.packs_dir, fname)
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            if not r.fieldnames:
                raise ValueError(f"{fname}: missing header")
            hm = {(h or "").strip().lower(): h for h in r.fieldnames}
            missing = [h for h in required if h not in hm]
            if missing:
                raise ValueError(f"{fname}: missing columns {missing}. Found: {r.fieldnames}")

            for row in r:
                get = lambda k: (row.get(hm[k]) or "").strip()

                pack_name = get("cardset") or os.path.splitext(fname)[0]
                name      = get("cardname")
                rarity_raw= get("cardrarity")
                rarity    = normalize_rarity(rarity_raw)  # your existing helper
                code      = get("cardcode")
                cid       = get("cardid")
                try:
                    weight = max(1, int(get("cardq")))
                except Exception:
                    weight = 1

                pack = packs.setdefault(pack_name, {"name": pack_name, "by_rarity": defaultdict(list)})

                # IMPORTANT: write canonical keys that the index understands:
                card_dict = {
                    # canonical/csv-style keys
                    "cardname":   name,
                    "cardrarity": rarity,
                    "cardset":    pack_name,
                    "cardcode":   code or None,
                    "cardid":     cid or None,

                    # short aliases (shop index also reads these)
                    "name":    name,
                    "rarity":  rarity,
                    "set":     pack_name,
                    "code":    code or None,
                    "id":      cid or None,

                    # any other fields you want to keep
                    "print_id": (row.get(hm["print_id"]) or "").strip() or None,
                    "weight":   weight,
                }

                pack["by_rarity"][rarity].append(card_dict)

    # freeze defaultdicts
    for p in packs.values():
        p["by_rarity"] = {k: v for k, v in p["by_rarity"].items()}

    state.packs_index = packs
    return packs

def _weighted_pick(items: list[dict]) -> dict:
    weights = [max(1, it["weight"]) for it in items]
    return random.choices(items, weights=weights, k=1)[0]

def _fallback_pool(by_rarity: dict, preferred: list[str]) -> list[dict]:
    for r in preferred:
        pool = by_rarity.get(r, [])
        if pool: return pool
    out = []
    for v in by_rarity.values(): out.extend(v)
    return out

def open_pack_from_csv(state: AppState, pack_name: str, amount: int = 1) -> list[dict]:
    if pack_name not in state.packs_index:
        raise ValueError(f"Unknown pack '{pack_name}'.")
    by_rarity = state.packs_index[pack_name]["by_rarity"]
    pulls: list[dict] = []
    for _ in range(amount):
        pool = by_rarity.get("common") or _fallback_pool(by_rarity, ["uncommon","rare","super","ultra","secret"])
        for _i in range(7): pulls.append(_weighted_pick(pool))
        pool = by_rarity.get("rare") or _fallback_pool(by_rarity, ["super","ultra","secret","uncommon","common"])
        pulls.append(_weighted_pick(pool))
        roll = random.random()
        target = "secret" if roll < 0.04 else ("ultra" if roll < 0.25 else "super")
        prefs = {"secret":["secret","ultra","super","rare","uncommon","common"],
                 "ultra":["ultra","super","rare","uncommon","common","secret"],
                 "super":["super","rare","uncommon","common","ultra","secret"]}[target]
        pool = _fallback_pool(by_rarity, prefs)
        pulls.append(_weighted_pick(pool))
    return pulls

def open_mini_pack_from_csv(state: AppState, pack_names: str | list[str]) -> list[dict]:
    """Open a mini pack: 4 commons and 1 rare (with fallback rarities if needed)."""
    if isinstance(pack_names, str):
        pack_names = [pack_names]

    by_rarity = defaultdict(list)
    for pack_name in pack_names:
        if pack_name not in state.packs_index:
            continue
        for rarity, cards in state.packs_index[pack_name]["by_rarity"].items():
            by_rarity[rarity].extend(cards)

    if not by_rarity:
        raise ValueError("No eligible packs found for mini pack reward.")

    pulls: list[dict] = []

    common_pool = by_rarity.get("common") or _fallback_pool(by_rarity, ["uncommon", "rare", "super", "ultra", "secret"])
    for _ in range(4):
        pulls.append(_weighted_pick(common_pool))

    rare_pool = by_rarity.get("rare") or _fallback_pool(by_rarity, ["super", "ultra", "secret", "uncommon", "common"])
    pulls.append(_weighted_pick(rare_pool))

    return pulls

def resolve_card_in_pack(state: AppState, card_set: str, card_name: str, card_code: str="", card_id: str="") -> dict:
    if not state.packs_index or card_set not in state.packs_index:
        raise ValueError(f"Set '{card_set}' not found.")
    candidates = []
    for items in state.packs_index[card_set]["by_rarity"].values():
        for it in items:
            if it["name"] == card_name:
                candidates.append(it)
    if not candidates:
        raise ValueError(f"Card '{card_name}' not in set '{card_set}'.")
    if card_code:
        candidates = [it for it in candidates if (it.get("card_code") or "") == card_code]
        if not candidates: raise ValueError(f"No '{card_name}' with code '{card_code}' in '{card_set}'.")
    if card_id:
        candidates = [it for it in candidates if (it.get("card_id") or "") == card_id]
        if not candidates: raise ValueError(f"No '{card_name}' with id '{card_id}' in '{card_set}'.")
    sigs = {(it.get("rarity",""), it.get("card_code",""), it.get("card_id","")) for it in candidates}
    if len(sigs) > 1:
        raise ValueError("Multiple prints match; specify card_code or card_id.")
    return candidates[0]

def persist_pulls_to_db(state, user_id: int, pack_name: str, pulls: list[dict]) -> int:
    """
    Aggregate pulls (qty=1 entries) and upsert them to the user's collection via db_add_cards.
    Each pull dict should have: name, rarity, set (optional; defaults to pack_name), card_code, card_id, qty (optional; defaults to 1).
    Returns the total quantity added.
    """
    agg = defaultdict(int)
    for it in pulls or []:
        name = (it.get("name") or "").strip()
        if not name:
            continue
        rarity = (it.get("rarity") or "").strip().lower()
        cset   = (it.get("set") or pack_name) or pack_name
        code   = (it.get("card_code") or "").strip()
        cid    = (it.get("card_id") or "").strip()
        qty    = int(it.get("qty", 1) or 1)
        if qty <= 0:
            continue
        key = (name, rarity, cset, code, cid)
        agg[key] += qty

    if not agg:
        return 0

    items = [{"name": k[0], "qty": q, "rarity": k[1], "set": k[2], "card_code": k[3], "card_id": k[4]}
             for k, q in agg.items()]

    db_add_cards(state, user_id, items, pack_name)
    return sum(q for q in agg.values())