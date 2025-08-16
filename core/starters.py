# core/starters.py
import os, csv
from typing import Dict, List
from core.state import AppState
from core.db import db_add_cards  # uses your existing bulk insert/upsert

REQUIRED_HEADER = ["cardname","cardq","cardrarity","card_edition","cardset","cardcode","cardid","print_id"]

def load_starters_from_csv(state: AppState, starters_dir: str | None = None) -> Dict[str, List[dict]]:
    """Load starter decks from CSVs into state.starters_index:
       { deck_name: [ {name,qty,rarity,set,card_code,card_id}, ... ] }"""
    starters_dir = starters_dir or getattr(state, "starters_dir", "starters_csv")
    state.starters_dir = starters_dir
    starters: Dict[str, List[dict]] = {}
    if not os.path.isdir(starters_dir):
        state.starters_index = {}
        return {}

    for fname in os.listdir(starters_dir):
        if not fname.lower().endswith(".csv"):
            continue
        path = os.path.join(starters_dir, fname)
        deck = os.path.splitext(fname)[0]
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            if not r.fieldnames:
                raise ValueError(f"{fname}: missing header")
            # exact header match (like your pack loader expects)
            if [h.strip().lower() for h in r.fieldnames] != REQUIRED_HEADER:
                raise ValueError(f"{fname}: header must be exactly {REQUIRED_HEADER}")

            rows: List[dict] = []
            for row in r:
                name = (row["cardname"] or "").strip()
                qty  = int((row["cardq"] or "0").strip() or 0)
                rarity = (row["cardrarity"] or "").strip().lower()
                cset   = (row["cardset"] or "").strip()
                code   = (row["cardcode"] or "").strip()
                cid    = (row["cardid"] or "").strip()
                if not name or qty <= 0:
                    continue
                rows.append({
                    "name": name, "qty": qty, "rarity": rarity,
                    "set": cset, "card_code": code, "card_id": cid
                })
            if rows:
                starters[deck] = rows

    state.starters_index = starters
    return starters

def grant_starter_to_user(state, user_id: int, deck_name: str) -> int:
    """Grant a starter deck to a user via db_add_cards(..., pack_name=...)."""
    cards = (state.starters_index or {}).get(deck_name, [])
    if not cards:
        return 0

    # NEW: pass a pack name to satisfy db_add_cards signature
    pack_name_for_audit = f"Starter:{deck_name}"
    db_add_cards(state, user_id, cards, pack_name_for_audit)

    return sum(c.get("qty", 0) or 0 for c in cards)
