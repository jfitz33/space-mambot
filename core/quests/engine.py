# core/engine.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Optional, Iterable, List, Tuple
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import asyncio

from core.quests.schema import (
    db_fetch_active_quests,
    db_get_user_progress,
    db_upsert_progress,
    db_mark_claimed,
    db_mark_claimed_step,
    db_daily_quest_add_slot,
    db_daily_quest_get_slots,
    db_daily_quest_mark_claimed,
    db_daily_quest_update_progress,
)
from .timekeys import now_et, period_key_for_category, daily_key
from core.db import db_wallet_add, db_shards_add
from core.constants import set_id_for_pack
from core.currency import shard_set_name

ET = ZoneInfo("America/New_York")

@dataclass
class QuestDef:
    quest_id: str
    title: str
    description: str
    category: str            # daily/weekly/permanent
    target_count: int
    reward_type: str         # 'mambucks' | 'fitzcoin' | 'shards' | 'pack'
    reward_payload: dict     # e.g., {'amount': 50} or {'amount': 50, 'set_id': 1} or {'pack': 'FIRE', 'qty': 3}
    max_rollover_days: int = 0
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
      - db_upsert_progress(state, user_id, quest_id, period_key, delta, target_count) -> (progress, completed)
      - db_get_user_progress(state, user_id, quest_ids, period_key) -> dict[quest_id] -> row
      - db_mark_claimed(state, user_id, quest_id, period_key) -> bool
      - db_mark_claimed_step(state, user_id, quest_id, period_key, expect_steps) -> bool
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
                max_rollover_days=int(
                    r.get("max_rollover_days")
                    or (r.get("reward_payload") or {}).get("max_rollover_days", 0)
                    or 0
                ),
                active=bool(r.get("active", True)),
            )
            for r in rows if r.get("active", True)
        }

    def next_milestone(self, q: QuestDef, claimed_steps: int) -> Optional[dict]:
        ms = q.milestones
        return ms[claimed_steps] if 0 <= claimed_steps < len(ms) else None

    # --- Daily rollover helpers -------------------------------------------------
    def _date_from_key(self, day_key: str) -> date:
        try:
            _, iso = day_key.split(":", 1)
            return datetime.fromisoformat(iso).date()
        except Exception:
            return now_et().date()

    def _day_key_from_date(self, d: date) -> str:
        return f"D:{d.isoformat()}"

    async def _auto_grant_slot(self, user_id: int, slot: dict):
        ok = await db_daily_quest_mark_claimed(
            self.state, user_id, slot["quest_id"], slot["day_key"], auto=True
        )
        if ok:
            payload = slot.get("reward_payload") or {}
            await give_reward(self.state, user_id, slot.get("reward_type"), payload)

    async def _apply_rollover_limit(self, user_id: int, q: QuestDef, slots: List[dict]) -> List[dict]:
        if q.max_rollover_days <= 0:
            return slots
        pending = [s for s in slots if not s.get("claimed_at")]
        # If at or above the cap, auto grant the oldest pending slot to keep the queue size stable
        while len(pending) >= q.max_rollover_days > 0:
            oldest = pending.pop(0)
            await self._auto_grant_slot(user_id, oldest)
            stamp = now_et().isoformat()
            oldest["claimed_at"] = oldest.get("claimed_at") or stamp
            oldest["completed_at"] = oldest.get("completed_at") or stamp
            oldest["auto_granted_at"] = oldest.get("auto_granted_at") or stamp
        return slots

    async def _ensure_daily_rollover_slots(self, user_id: int, q: QuestDef) -> List[dict]:
        today_key = daily_key()
        slots = await db_daily_quest_get_slots(self.state, user_id, q.quest_id)
        if not slots:
            slots = [await db_daily_quest_add_slot(self.state, user_id, q, today_key)]

        last_date = self._date_from_key(slots[-1]["day_key"])
        today_date = self._date_from_key(today_key)

        while last_date < today_date:
            last_date = last_date + timedelta(days=1)
            slots = await self._apply_rollover_limit(user_id, q, slots)
            slots.append(await db_daily_quest_add_slot(self.state, user_id, q, self._day_key_from_date(last_date)))

        slots = await self._apply_rollover_limit(user_id, q, slots)
        return slots

    async def _increment_rollover_daily(self, user_id: int, q: QuestDef, amount: int) -> Tuple[int, bool]:
        slots = await self._ensure_daily_rollover_slots(user_id, q)
        remaining = int(amount or 0)
        for slot in slots:
            if slot.get("claimed_at"):
                continue
            target = int(slot.get("target_count") or q.target_count)
            progress = int(slot.get("progress") or 0)
            if progress >= target:
                continue
            delta = min(remaining, target - progress)
            if delta <= 0:
                continue
            row = await db_daily_quest_update_progress(
                self.state, user_id, q.quest_id, slot["day_key"], delta, target
            )
            slot["progress"] = int(row.get("progress", progress + delta))
            slot["completed_at"] = slot.get("completed_at") or row.get("completed_at")
            remaining -= delta
            if remaining <= 0:
                break

        active_slot = next((s for s in slots if not s.get("claimed_at") and int(s.get("progress", 0)) < int(s.get("target_count") or q.target_count)), None)
        claimables = [s for s in slots if not s.get("claimed_at") and int(s.get("progress", 0)) >= int(s.get("target_count") or q.target_count)]
        if active_slot:
            prog = int(active_slot.get("progress", 0))
            target = int(active_slot.get("target_count") or q.target_count)
            return prog, prog >= target
        if claimables:
            target = int(claimables[0].get("target_count") or q.target_count)
            return target, True
        return 0, False

    async def increment(self, user_id: int, quest_id: str, amount: int = 1) -> Tuple[int, bool]:
        q = self._defs.get(quest_id)
        if not q or not q.active:
            return 0, False
        if q.category == "daily" and q.max_rollover_days > 0:
            return await self._increment_rollover_daily(user_id, q, amount)
        pkey = period_key_for_category(q.category)
        return await db_upsert_progress(self.state, user_id, q.quest_id, pkey, amount, q.target_count)

    async def get_user_view(self, user_id: int) -> List[dict]:
        view = []
        for q in self._defs.values():
            pkey = period_key_for_category(q.category)
            if q.category == "daily" and q.max_rollover_days > 0:
                slots = await self._ensure_daily_rollover_slots(user_id, q)
                pending = [s for s in slots if not s.get("claimed_at")]
                claimables = [s for s in pending if int(s.get("progress", 0)) >= int(s.get("target_count") or q.target_count)]
                active_slot = pending[0] if pending else (slots[0] if slots else {})
                target = int(active_slot.get("target_count", q.target_count))
                progress = int(active_slot.get("progress", 0))
                view.append({
                    "quest": q,
                    "progress": min(progress, target),
                    "target": target,
                    "completed": bool(claimables),
                    "claimed": False if claimables else not pending,
                    "claimed_steps": 0,
                    "period_key": daily_key(),
                    "milestone_mode": False,
                    "rollover_pending": len(pending),
                    "rollover_claimables": len(claimables),
                })
                continue

            row = await db_get_user_progress(self.state, user_id, [q.quest_id], pkey)
            r = row.get(q.quest_id) or {}

            progress = int(r.get("progress", 0))
            claimed_steps = int(r.get("claimed_steps", 0))
            has_milestones = bool(q.milestones)

            if has_milestones:
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
                    # All milestones done & claimed
                    view.append({
                        "quest": q,
                        "progress": q.target_count,
                        "target": q.target_count,
                        "completed": True,
                        "claimed": True,
                        "claimed_steps": claimed_steps,
                        "period_key": pkey,
                        "milestone_mode": True,
                    })
            else:
                # Single-step quest
                target = q.target_count
                completed = progress >= target or bool(r.get("completed_at"))
                claimed = bool(r.get("claimed_at"))
                display_progress = min(progress, target)
                view.append({
                    "quest": q,
                    "progress": display_progress,
                    "target": target,
                    "completed": completed,
                    "claimed": claimed,
                    "claimed_steps": claimed_steps,
                    "period_key": pkey,
                    "milestone_mode": False,
                })
        return view

    async def claim(self, user_id: int, quest_id: str) -> Tuple[bool, str]:
        q = self._defs.get(quest_id)
        if not q:
            return False, "Quest not found."
        if q.category == "daily" and q.max_rollover_days > 0:
            slots = await self._ensure_daily_rollover_slots(user_id, q)
            claimables = [s for s in slots if not s.get("claimed_at") and int(s.get("progress", 0)) >= int(s.get("target_count") or q.target_count)]
            if not claimables:
                return False, "Not completed yet."
            slot = claimables[0]
            ok = await db_daily_quest_mark_claimed(self.state, user_id, q.quest_id, slot["day_key"], auto=False)
            if not ok:
                return False, "Already claimed or race condition; try again."
            try:
                ack = await give_reward(self.state, user_id, slot.get("reward_type", q.reward_type), slot.get("reward_payload") or {})
            except Exception as e:
                return False, f"Reward error: {e}"
            return True, f"Reward claimed for {slot['day_key'].split(':')[-1]}! {ack}"
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
    if asyncio.iscoroutine(x):
        return await x
    return x

# --- Reward hooks ------------------------------------------------------------

async def credit_currency(state, user_id: int, currency: str, amount: int):
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
    Supported types: 'mambucks', 'fitzcoin' (legacy), 'shards', 'pack'
    """
    rt = (reward_type or "").lower()
    data = payload or {}

    if rt == "mambucks":
        amt = int(data.get("amount", 0))
        await credit_currency(state, user_id, "mambucks", amt)
        return f"{amt} Mambucks credited to your wallet."

    if rt == "fitzcoin":
        amt = int(data.get("amount", 0))
        await credit_currency(state, user_id, "fitzcoin", amt)
        return f"{amt} Fitzcoin credited to your wallet."

    if rt == "shards":
        amt = int(data.get("amount", 0))
        # resolve set id from payload variants: set_id | set (int) | set (str pack name) | pack
        set_id: Optional[int] = None
        if "set_id" in data:
            set_id = int(data["set_id"])
        elif "set" in data and isinstance(data["set"], int):
            set_id = int(data["set"])
        elif "set" in data and isinstance(data["set"], str):
            set_id = set_id_for_pack(str(data["set"]))
        elif "pack" in data:
            set_id = set_id_for_pack(str(data["pack"]))
        if not set_id:
            set_id = 1  # default to Set 1
        if amt <= 0:
            return "No shards credited."
        db_shards_add(state, user_id, set_id, amt)
        return f"{amt} {shard_set_name(set_id)} credited to your wallet."

    if rt == "pack":
        pack_name = data["pack"]
        qty = int(data.get("qty", 1))
        if hasattr(state, "shop") and hasattr(state.shop, "grant_pack"):
            await maybe_await(state.shop.grant_pack(user_id, pack_name, qty))
            return f"Granted {qty}× {pack_name} pack(s)."
        raise RuntimeError("Pack reward helper not wired (expected state.shop.grant_pack).")

    raise RuntimeError(f"Unknown reward_type: {reward_type}")
