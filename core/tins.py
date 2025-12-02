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


def is_tin_promo_print(state, card: dict, set_name: str | None = None) -> bool:
    """
    Return True if the given printing matches a promo card bundled with a tin.

    Matching is performed against ``state.tins_index`` using name, rarity, set,
    and (when present) code/id for disambiguation. The default set name comes
    from ``card`` unless explicitly provided.
    """

    def _norm(text: str | None) -> str:
        return (text or "").strip().lower()

    tins = getattr(state, "tins_index", None) or {}
    set_norm = _norm(set_name or card.get("set") or card.get("cardset"))
    name_norm = _norm(card.get("name") or card.get("cardname"))
    rarity_norm = canonicalize_rarity(card.get("rarity") or card.get("cardrarity") or "")
    code_norm = _norm(card.get("code") or card.get("cardcode"))
    cid_norm = _norm(card.get("id") or card.get("cardid"))

    if not set_norm or not name_norm:
        return False

    for _, meta in tins.items():
        for promo in meta.get("promo_cards") or []:
            if set_norm != _norm(promo.get("set") or promo.get("cardset")):
                continue
            if name_norm != _norm(promo.get("name") or promo.get("cardname")):
                continue

            promo_rarity = canonicalize_rarity(promo.get("rarity") or promo.get("cardrarity") or "")
            if rarity_norm and promo_rarity and rarity_norm != promo_rarity:
                continue

            promo_code = _norm(promo.get("code") or promo.get("cardcode"))
            promo_cid = _norm(promo.get("id") or promo.get("cardid"))
            if promo_code and promo_code != code_norm:
                continue
            if promo_cid and promo_cid != cid_norm:
                continue

            return True

    return False