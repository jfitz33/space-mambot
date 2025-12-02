# core/wallet_api.py
from typing import Optional
from core.db import (
    db_wallet_get, db_wallet_add, db_wallet_try_spend_mambucks,
    db_shards_get, db_shards_add, db_shards_try_spend,
)
from core.currency import mambucks_label, shards_label

# Mambucks (pack currency)
def get_mambucks(state, user_id: int) -> int:
    return int(db_wallet_get(state, user_id).get("mambucks", 0))

def credit_mambucks(state, user_id: int, amount: int) -> int:
    db_wallet_add(state, user_id, d_mambucks=int(amount))
    return get_mambucks(state, user_id)

def try_spend_mambucks(state, user_id: int, amount: int) -> Optional[int]:
    new = db_wallet_try_spend_mambucks(state, user_id, int(amount))
    return None if new is None else int(new.get("mambucks", 0))

# Shards (per set)
def get_shards(state, user_id: int, set_id: int) -> int:
    return int(db_shards_get(state, user_id, int(set_id)))

def add_shards(state, user_id: int, set_id: int, delta: int) -> int:
    db_shards_add(state, user_id, int(set_id), int(delta))
    return get_shards(state, user_id, set_id)

def try_spend_shards(state, user_id: int, set_id: int, amount: int) -> Optional[int]:
    new = db_shards_try_spend(state, user_id, int(set_id), int(amount))
    return None if new is None else int(new.get("shards", 0))

# Message helpers
def fmt_mambucks(amount: int) -> str:
    return mambucks_label(int(amount))

def fmt_shards(amount: int, set_id: int) -> str:
    return shards_label(int(amount), int(set_id))
