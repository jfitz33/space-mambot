# core/pricing.py
from __future__ import annotations
import math
from typing import Optional, Tuple
from datetime import datetime
from core.db import db_sales_get_for_day, db_craft_set_discount_get
from core.daily_rollover import rollover_day_key
from core.tins import is_tin_promo_print
from core.purchase_options import is_craft_blocked
from core.constants import CRAFT_COST_BY_RARITY, set_id_for_pack


def day_key_et(dt: Optional[datetime] = None) -> str:
    if dt is None:
        return rollover_day_key()
    return rollover_day_key(dt)

def _norm(s: Optional[str]) -> str:
    return (s or "").strip()

def craft_cost_for_card(state, card: dict, set_name: str, *, on_day: Optional[str] = None) -> Tuple[int, Optional[dict]]:
    """Return (cost_each_shards, sale_row_or_None) honoring today's sale if this card matches."""
    if is_tin_promo_print(state, card, set_name=set_name):
        return (0, None)

    rarity = _norm(card.get("rarity") or card.get("cardrarity")).lower()
    base = int(CRAFT_COST_BY_RARITY.get(rarity, 0))
    if base <= 0:
        return (base, None)
    
    # Apply set-wide craft discount (if configured).
    set_id = set_id_for_pack(set_name) if set_name else None
    if set_id is not None:
        if is_craft_blocked(set_id):
            return (0, None)
        set_discount_pct = db_craft_set_discount_get(state, set_id)
        if set_discount_pct and set_discount_pct > 0:
            base = int(math.ceil(base * (100 - int(set_discount_pct)) / 100.0))

    # check daily sale match
    dk = on_day or day_key_et()
    sales = db_sales_get_for_day(state, dk) or {}
    sale_rows = sales.get(rarity) or []
    if not sale_rows:
        return (base, None)

    # match this exact printing: name + set (+code/id if present)
    name = _norm(card.get("name") or card.get("cardname"))
    code = _norm(card.get("code") or card.get("cardcode"))
    cid  = _norm(card.get("id")   or card.get("cardid"))

    for sale in sale_rows:
        if name.lower() != _norm(sale["card_name"]).lower():
            continue
        if set_name.lower() != _norm(sale["card_set"]).lower():
            continue
        # If sale row has code/id, require exact match; if sale row leaves them blank, ignore those fields.
        scode = _norm(sale.get("card_code"))
        scid  = _norm(sale.get("card_id"))
        if scode and scode != code:
            continue
        if scid and scid != cid:
            continue

        return (int(sale["price_shards"]), sale)

        # Apply the stored price (already rounded)
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
