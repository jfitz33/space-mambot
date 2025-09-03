# core/pricing.py
from __future__ import annotations
import math
from typing import Optional, Tuple
from zoneinfo import ZoneInfo
from datetime import datetime
from core.db import db_sales_get_for_day
from core.constants import CRAFT_COST_BY_RARITY, SALE_DISCOUNT_PCT

ET = ZoneInfo("America/New_York")

def day_key_et(dt: Optional[datetime] = None) -> str:
    dt = (dt or datetime.now(tz=ET)).astimezone(ET)
    return dt.strftime("%Y%m%d")

def _norm(s: Optional[str]) -> str:
    return (s or "").strip()

def craft_cost_for_card(state, card: dict, set_name: str, *, on_day: Optional[str] = None) -> Tuple[int, Optional[dict]]:
    """Return (cost_each_shards, sale_row_or_None) honoring today's sale if this card matches."""
    rarity = _norm(card.get("rarity") or card.get("cardrarity")).lower()
    base = int(CRAFT_COST_BY_RARITY.get(rarity, 0))
    if base <= 0:
        return (base, None)

    # check daily sale match
    dk = on_day or day_key_et()
    sales = db_sales_get_for_day(state, dk)
    sale = sales.get(rarity)
    if not sale:
        return (base, None)

    # match this exact printing: name + set (+code/id if present)
    name = _norm(card.get("name") or card.get("cardname"))
    code = _norm(card.get("code") or card.get("cardcode"))
    cid  = _norm(card.get("id")   or card.get("cardid"))

    if name.lower() != _norm(sale["card_name"]).lower():  # name mismatch
        return (base, None)
    if set_name.lower() != _norm(sale["card_set"]).lower():
        return (base, None)

    # If sale row has code/id, require exact match; if sale row leaves them blank, ignore those fields.
    scode = _norm(sale.get("card_code"))
    scid  = _norm(sale.get("card_id"))
    if scode and scode != code:
        return (base, None)
    if scid and scid != cid:
        return (base, None)

    # Apply the stored price (already rounded)
    return (int(sale["price_shards"]), sale)
