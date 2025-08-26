from core.currency import shard_set_name

def not_enough_mambucks(need: int, have: int) -> str:
    return f"❌ Not enough Mambucks. Need **{need}**, you have **{have}**."

def not_enough_shards(need: int, have: int, set_id: int) -> str:
    title = shard_set_name(set_id)
    return f"❌ Not enough {title}. Need **{need}**, you have **{have}**."

def credit_line_mambucks(amount: int, new_total: int) -> str:
    return f"**+{amount}** Mambucks (now **{new_total}**)."

def credit_line_shards(amount: int, new_total: int, set_id: int) -> str:
    title = shard_set_name(set_id)
    return f"**+{amount}** {title} (now **{new_total}**)."

def debit_line_mambucks(amount: int, new_total: int) -> str:
    return f"**−{amount}** Mambucks (now **{new_total}**)."

def debit_line_shards(amount: int, new_total: int, set_id: int) -> str:
    title = shard_set_name(set_id)
    return f"**−{amount}** {title} (now **{new_total}**)."
