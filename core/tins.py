import json
from pathlib import Path
from typing import Dict, Iterable

from core.cards_shop import canonicalize_rarity


def _normalize_promo_card(tin_name: str, raw: dict) -> dict:
    """Return a promo card dict with consistent keys and a default set name."""
    card = dict(raw or {})
    name = (card.get("name") or card.get("cardname") or "").strip()
    rarity = canonicalize_rarity(card.get("rarity") or card.get("cardrarity") or "")
    set_name = (card.get("set") or card.get("cardset") or tin_name).strip()
    code = (card.get("code") or card.get("cardcode") or "").strip() or None
    cid = (card.get("id") or card.get("cardid") or "").strip() or None

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
    # Preserve any extra fields
    for k, v in card.items():
        normalized.setdefault(k, v)
    return normalized


def _normalize_pack_list(raw: Iterable[str]) -> list[str]:
    packs: list[str] = []
    for entry in raw or []:
        text = (str(entry) or "").strip()
        if text:
            packs.append(text)
    return packs


def load_tins_from_json(state, path: str | Path) -> Dict[str, dict]:
    """
    Load tin definitions from JSON and attach them to ``state.tins_index``.

    Expected schema per tin entry:
        {
            "name": "Tin Name",
            "promo_cards": [ { name/cardname, rarity, set?, code?, id? }, ... ],
            "packs": ["Pack A", "Pack B"]
        }
    """

    resolved = Path(path)
    if not resolved.is_absolute():
        base = Path(__file__).resolve().parents[1]
        resolved = (base / resolved).resolve()

    tins: Dict[str, dict] = {}
    if not resolved.exists():
        state.tins_index = tins
        return tins

    with resolved.open("r", encoding="utf-8") as f:
        data = json.load(f)

    entries = data.get("tins") if isinstance(data, dict) else data
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        name = (entry.get("name") or "").strip()
        if not name:
            continue
        promo_cards = [_normalize_promo_card(name, pc) for pc in (entry.get("promo_cards") or []) if pc]
        packs = _normalize_pack_list(entry.get("packs") or [])
        tins[name] = {"name": name, "promo_cards": promo_cards, "packs": packs}

    state.tins_index = tins
    return tins