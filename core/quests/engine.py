from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Optional, Iterable, List
from datetime import datetime
from zoneinfo import ZoneInfo
import asyncio
import json

from core.quests.schema import (
    db_fetch_active_quests,
    db_get_user_progress,
    db_upsert_progress,
    db_mark_claimed,
    db_mark_claimed_step,
)
from .timekeys import now_et, period_key_for_category
from core.db import db_wallet_add

ET = ZoneInfo("America/New_York")

@dataclass
class QuestDef:
    quest_id: str
    title: str
    description: str
    category: str            # daily/weekly/permanent
    target_count: int
    reward_type: str         # e.g., 'mambucks'
    reward_payload: dict     # e.g., {'amount': 50} or {'rarity': 'RARE'}
    active: bool = True

    @property
    def milestones(self) -> List[dict]:
        ms = (self.reward_payload or {}).get("milestones") or []
        # sort ascending, dedupe counts
        out, seen = [], set()
        for m in sorted(ms, key=lambda x: int(x.get("count", 0))):
            c = int(m.get("count", 0))
            if c > 0 and c not in seen:
                out.append({"count": c, "reward": m.get("reward", {})})
                seen.add(c)
        return out

class QuestManager:
    """
    Storage-agnostic manager; expects you to provide 3 tiny DB functions on state:
      - db_fetch_active_quests(state) -> list[QuestDef-like dicts]
      - db_upsert_progress(state, user_id, quest_id, period_key, delta) -> (progress, completed)
      - db_get_user_progress(state, user_id, quest_ids, period_key) -> dict[quest_id] -> row
      - db_mark_claimed(state, user_id, quest_id, period_key) -> bool
    You can map these to your existing DB layer.
    """
    def __init__(self, state: Any):
        self.state = state
        self._defs: dict[str, QuestDef] = {}

    async def load_defs(self) -> None:
        rows = await db_fetch_active_quests(self.state)
        self._defs = {
            r["quest_id"]: QuestDef(
                quest_id=r["quest_id"],
                title=r["title"],
                description=r["description"],
                category=r["category"],
                target_count=int(r["target_count"]),
                reward_type=r["reward_type"],
                reward_payload=(r.get("reward_payload") or {}),
                active=bool(r.get("active", True)),
            )
            for r in rows if r.get("active", True)
        }

    def next_milestone(self, q: QuestDef, claimed_steps: int) -> Optional[dict]:
        ms = q.milestones
        return ms[claimed_steps] if 0 <= claimed_steps < len(ms) else None

    async def increment(self, user_id: int, quest_id: str, amount: int = 1) -> Tuple[int, bool]:
        q = self._defs.get(quest_id)
        if not q or not q.active:
            return 0, False
        pkey = period_key_for_category(q.category)
        return await db_upsert_progress(self.state, user_id, q.quest_id, pkey, amount, q.target_count)

    async def get_user_view(self, user_id: int) -> List[dict]:
        view = []
        for q in self._defs.values():
            pkey = period_key_for_category(q.category)
            row = await db_get_user_progress(self.state, user_id, [q.quest_id], pkey)
            r = row.get(q.quest_id) or {}
            progress = int(r.get("progress", 0))
            claimed_steps = int(r.get("claimed_steps", 0))
            ms = self.next_milestone(q, claimed_steps)
            if ms:
                target = int(ms["count"])
                completed = progress >= target
                claimed = False  # not yet claimed this step
                display_progress = min(progress, target)
                view.append({
                    "quest": q,
                    "progress": display_progress,
                    "target": target,
                    "completed": completed,
                    "claimed": claimed,
                    "claimed_steps": claimed_steps,
                    "period_key": pkey,
                    "milestone_mode": True,
                })
            else:
                # no more milestones -> treat as fully done
                view.append({
                    "quest": q,
                    "progress": progress,
                    "target": q.target_count,
                    "completed": True,
                    "claimed": True,
                    "claimed_steps": claimed_steps,
                    "period_key": pkey,
                    "milestone_mode": True,
                })
        return view

    async def claim(self, user_id: int, quest_id: str) -> Tuple[bool, str]:
        q = self._defs.get(quest_id)
        if not q:
            return False, "Quest not found."
        pkey = period_key_for_category(q.category)
        row = await db_get_user_progress(self.state, user_id, [q.quest_id], pkey)
        r = (row or {}).get(q.quest_id) or {}
        progress = int(r.get("progress", 0))
        claimed_steps = int(r.get("claimed_steps", 0))

        # Milestone quest?
        ms = self.next_milestone(q, claimed_steps)
        if ms:
            target = int(ms["count"])
            if progress < target:
                return False, f"Not completed yet. {progress}/{target}."
            ok = await db_mark_claimed_step(self.state, user_id, q.quest_id, pkey, expect_steps=claimed_steps)
            if not ok:
                return False, "Already claimed or race condition; try again."
            reward = ms.get("reward") or {}
            try:
                ack = await give_reward(self.state, user_id, reward.get("type", q.reward_type), reward)
            except Exception as e:
                return False, f"Reward error: {e}"
            return True, f"Reward claimed for milestone {target}! {ack}"

        # --- single-step fallback ---
        if r.get("claimed_at"):
            return False, "Reward already claimed."
        if progress < q.target_count and not r.get("completed_at"):
            return False, "Quest not completed yet."
        ok = await db_mark_claimed(self.state, user_id, q.quest_id, pkey)
        if not ok:
            return False, "Unable to claim at this time."
        try:
            ack = await give_reward(self.state, user_id, q.reward_type, q.reward_payload or {})
        except Exception as e:
            return False, f"Reward error: {e}"
        return True, f"Reward claimed! {ack}"

async def maybe_await(x):
    if asyncio.iscoroutine(x): return await x
    return x

# --- Reward hook (wire to your wallet / cards) ---
async def _credit_currency(state, user_id: int, currency: str, amount: int):
    """
    Credit using core.db.db_wallet_add.

    Preferred call (keywords):
        db_wallet_add(state, user_id, d_fitzcoin=..., d_mambucks=...)

    Fallback (positional deltas), if your helper is positional-only:
        db_wallet_add(state, user_id, fitzcoin_delta, mambucks_delta)
    """
    if not db_wallet_add:
        raise RuntimeError("core.db.db_wallet_add is not available.")

    amt = int(amount or 0)
    if amt <= 0:
        return

    c = (currency or "").lower()
    if c not in ("mambucks", "fitzcoin"):
        raise RuntimeError(f"Unsupported currency: {currency}")

    # Try keywords first (most robust)
    try:
        if c == "mambucks":
            return await maybe_await(db_wallet_add(state, user_id, d_fitzcoin=0, d_mambucks=amt))
        else:
            return await maybe_await(db_wallet_add(state, user_id, d_fitzcoin=amt, d_mambucks=0))
    except TypeError:
        # Fallback: positional deltas (fitzcoin, mambucks)
        if c == "mambucks":
            return await maybe_await(db_wallet_add(state, user_id, 0, amt))
        else:
            return await maybe_await(db_wallet_add(state, user_id, amt, 0))

async def give_reward(state, user_id: int, reward_type: str, payload: dict) -> str:
    """
    Performs the reward side-effect and returns a short human message
    describing what was granted (for UI).
    """
    rt = (reward_type or "").lower()
    data = payload or {}

    if rt == "mambucks":
        amt = int(data.get("amount", 0))
        await _credit_currency(state, user_id, "mambucks", amt)
        return f"{amt} Mambucks credited to your wallet."

    if rt == "fitzcoin":
        amt = int(data.get("amount", 0))
        await _credit_currency(state, user_id, "fitzcoin", amt)
        return f"{amt} Fitzcoin credited to your wallet."

    #if rt == "card_random_rarity":
    #    rarity = str(data.get("rarity", "COMMON"))
    #    qty = int(data.get("qty", 1))
    #    from cogs.rewards_wheel import _pick_random_card_by_rarity, _award_card_to_user
    #    card = _pick_random_card_by_rarity(state, rarity)
    #    if not card:
    #        raise RuntimeError(f"No cards available for rarity {rarity}")
    #    await _award_card_to_user(state, user_id, card, qty=qty)
    #    # Friendly line; you can tweak wording
    #    return f"{qty} random {rarity.title()} card added to your collection."

    if rt == "pack":
        pack_name = data["pack"]
        qty = int(data.get("qty", 1))
        if hasattr(state, "shop") and hasattr(state.shop, "grant_pack"):
            await maybe_await(state.shop.grant_pack(user_id, pack_name, qty))
            return f"Granted {qty}× {pack_name} pack(s)."
        raise RuntimeError("Pack reward helper not wired (expected state.shop.grant_pack).")

    raise RuntimeError(f"Unknown reward_type: {reward_type}")
