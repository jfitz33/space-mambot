from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List

from core.cards_shop import canonicalize_rarity


def _normalize_row(raw: dict) -> dict | None:
    name = (raw.get("cardname") or raw.get("name") or "").strip()
    set_name = (raw.get("cardset") or raw.get("set") or "").strip()
    rarity = canonicalize_rarity(raw.get("cardrarity") or raw.get("rarity") or "")
    if not name or not set_name:
        return None

    code = (raw.get("cardcode") or raw.get("code") or "").strip() or None
    cid = (raw.get("cardid") or raw.get("id") or "").strip() or None

    normalized = {
        "name": name,
        "cardname": name,
        "rarity": rarity,
        "cardrarity": rarity,
        "set": set_name,
        "cardset": set_name,
        "code": code,
        "cardcode": code,
        "id": cid,
        "cardid": cid,
    }
    for key, value in (raw or {}).items():
        normalized.setdefault(key, value)
    return normalized


def load_admin_grant_cards_from_csv(state, path: str | Path) -> Dict[str, dict]:
    """Load admin-grant-only cards and attach them to state."""

    resolved = Path(path)
    if not resolved.is_absolute():
        base = Path(__file__).resolve().parents[1]
        resolved = (base / resolved).resolve()

    by_set: Dict[str, List[dict]] = {}
    if resolved.exists():
        with resolved.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                card = _normalize_row(row)
                if not card:
                    continue
                set_name = card["set"]
                by_set.setdefault(set_name, []).append(card)

    state.admin_grant_cards_index = {
        set_name: {"name": set_name, "cards": cards}
        for set_name, cards in by_set.items()
    }
    state.admin_grant_only_sets = {set_name.strip().lower() for set_name in by_set.keys() if set_name.strip()}
    return state.admin_grant_cards_index


def is_admin_grant_only_set(state, set_name: str | None) -> bool:
    lowered = (set_name or "").strip().lower()
    if not lowered:
        return False
    return lowered in (getattr(state, "admin_grant_only_sets", None) or set())