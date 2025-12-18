from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable, Literal, Sequence

from core.currency import mambucks_label, shard_set_name, shards_label

Currency = Literal["mambucks", "shards"]


def _parse_shard_enabled_sets(env_value: str | None) -> set[int]:
    enabled: set[int] = set()
    if not env_value:
        return enabled
    for token in env_value.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            enabled.add(int(token))
        except ValueError:
            continue
    return enabled


# Comma-separated set IDs (e.g., "1,2") that are allowed to use shards for packs/boxes.
PACK_SHARD_ENABLED_SETS = _parse_shard_enabled_sets(os.getenv("PACK_SHARD_ENABLED_SETS", ""))


@dataclass(frozen=True)
class PaymentOption:
    currency: Currency
    amount: int
    set_id: int | None = None

    @property
    def cost_label(self) -> str:
        if self.currency == "shards":
            return shards_label(int(self.amount), int(self.set_id or 0))
        return mambucks_label(int(self.amount))

    @property
    def button_label(self) -> str:
        if self.currency == "shards":
            name = shard_set_name(int(self.set_id or 0))
            return f"Pay with {name}"
        return "Pay with Mambucks"


def payment_options_for_set(
    set_id: int | None,
    *,
    mambuck_cost: int | None = None,
    shard_cost: int | None = None,
    allow_mambucks: bool = True,
    extra_shard_sets: Iterable[int] | None = None,
) -> list[PaymentOption]:
    """Build payment options for a pack/box/tin tied to ``set_id``.

    Mambucks are included by default. Shards are added when ``set_id`` is included in
    the ``PACK_SHARD_ENABLED_SETS`` environment variable or explicitly provided via
    ``extra_shard_sets``. Costs can differ per currency; shard costs default to the
    mambuck price when not provided.
    """
    options: list[PaymentOption] = []

    if allow_mambucks:
        if mambuck_cost is None:
            raise ValueError("mambuck_cost is required when allow_mambucks=True")
        options.append(PaymentOption(currency="mambucks", amount=int(mambuck_cost)))

    shard_enabled = set(extra_shard_sets or set()) | PACK_SHARD_ENABLED_SETS
    effective_shard_cost = shard_cost if shard_cost is not None else mambuck_cost
    if set_id is not None and int(set_id) in shard_enabled and effective_shard_cost is not None:
        options.append(
            PaymentOption(currency="shards", amount=int(effective_shard_cost), set_id=int(set_id))
        )

    if not options:
        raise ValueError("At least one payment option must be available.")
    return options


def format_payment_options(options: Sequence[PaymentOption]) -> str:
    """Format payment options as bullet lines for confirmation messages."""
    return "\n".join(f"• {opt.cost_label}" for opt in options)