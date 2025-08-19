# core/starters.py
import os, csv
from typing import Dict, List, Optional
from core.state import AppState
from core.db import db_add_cards  # uses your existing bulk insert/upsert
from core.util_norm import normalize_set_name

REQUIRED_HEADER = ["cardname","cardq","cardrarity","card_edition","cardset","cardcode","cardid","print_id"]
RARITY_MAP = {
    "common":"common","uncommon":"uncommon","rare":"rare",
    "super":"super","super rare":"super","ultra":"ultra",
    "ultra rare":"ultra","secret":"secret","secret rare":"secret",
}
RARITY_ORDER = ["secret","ultra","super","rare","uncommon","common"]

def normalize_rarity(s: str) -> str:
    return RARITY_MAP.get((s or "").strip().lower(), "rare")

def load_starters_from_csv(state: AppState, starters_dir: Optional[str] = None) -> Dict[str, List[dict]]:
    """
    Load starter decks from CSVs into state.starters_index:
      {
        deck_name: [
          {
            cardname, cardrarity, cardset, cardcode, cardid, cardq,
            name, rarity, set, code, id, qty,
            print_id (optional)
          }, ...
        ]
      }
    Every row includes both canonical CSV-style keys and short aliases so downstream
    code (indexing, labels, DB) can rely on consistent field names.
    """
    starters_dir = starters_dir or getattr(state, "starters_dir", "starters_csv")
    state.starters_dir = starters_dir
    starters: Dict[str, List[dict]] = {}

    if not os.path.isdir(starters_dir):
        state.starters_index = {}
        return {}

    # Required columns (case-insensitive). print_id optional.
    required = ["cardname", "cardq", "cardrarity", "cardset", "cardcode", "cardid"]

    for fname in os.listdir(starters_dir):
        if not fname.lower().endswith(".csv"):
            continue

        path = os.path.join(starters_dir, fname)
        deck = os.path.splitext(fname)[0]

        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            if not r.fieldnames:
                raise ValueError(f"{fname}: missing header")

            # Build a case-insensitive header map like your pack loader
            hm = {(h or "").strip().lower(): h for h in r.fieldnames}
            missing = [h for h in required if h not in hm]
            if missing:
                raise ValueError(f"{fname}: missing columns {missing}. Found: {r.fieldnames}")

            rows: List[dict] = []
            for row in r:
                get = lambda k: (row.get(hm[k]) or "").strip()

                # Prefer CSV's cardset if present, else default to the deck name
                set_name = get("cardset") or deck

                name       = get("cardname")
                qty_raw    = get("cardq")
                rarity_raw = get("cardrarity")
                code       = get("cardcode") or None
                cid        = get("cardid") or None
                print_id   = (row.get(hm.get("print_id", ""), "") or "").strip() if "print_id" in hm else None

                if not name:
                    continue
                try:
                    qty = max(1, int(qty_raw or "0"))
                except Exception:
                    qty = 1

                rarity = normalize_rarity(rarity_raw)  # keep consistent with pack loader

                card = {
                    # Canonical CSV-style keys (what the shop/index code reads)
                    "cardname":   name,
                    "cardrarity": rarity,
                    "cardset":    set_name,
                    "cardcode":   code,
                    "cardid":     cid,
                    "cardq":      qty,

                    # Short aliases (used by labels/other helpers)
                    "name":   name,
                    "rarity": rarity,
                    "set":    set_name,
                    "code":   code,
                    "id":     cid,
                    "qty":    qty,

                    # Optional, if present in CSV
                    "print_id": print_id or None,
                }

                rows.append(card)

            if rows:
                starters[deck] = rows

    state.starters_index = starters
    return starters

def grant_starter_to_user(state, user_id: int, deck_name: str) -> int:
    """Grant a starter deck to a user; ensures the set is the deck name."""
    cards = (state.starters_index or {}).get(deck_name, [])
    if not cards:
        return 0

    # Use the deck name as the default set (only applied if a row lacks cardset)
    default_set = normalize_set_name(deck_name)
    total_added = db_add_cards(state, user_id, cards, default_set=default_set)
    return total_added
