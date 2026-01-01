# core/engine.py
from __future__ import annotations
import os
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
    db_daily_quest_snapshot_day,
    db_daily_quest_list_users,
    db_daily_quest_mark_claimed,
    db_daily_quest_update_progress,
    db_seed_quests_from_json,
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
        self._last_defs_refresh_date: date | None = None
        self.week1_enabled = os.getenv("DAILY_DUEL_WEEK1_ENABLE", "1") == "1"

    async def load_defs(self) -> None:
        rows = await db_fetch_active_quests(self.state)
        self._defs = self._build_defs(rows)
        self._last_defs_refresh_date = self._last_defs_refresh_date or now_et().date()

    def _build_defs(self, rows: Iterable[dict]) -> dict[str, QuestDef]:
        return {
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
    
    async def _load_defs_for_date(self, target_date: date, mutate: bool = True) -> dict[str, QuestDef]:
        """Load quest defs for the given date, optionally updating live state."""

        json_path = getattr(self.state, "quests_json_path", None)
        if json_path:
            try:
                await db_seed_quests_from_json(self.state, json_path, deactivate_missing=True)
            except FileNotFoundError:
                pass

        rows = await db_fetch_active_quests(self.state)
        defs = self._build_defs(rows)
        if mutate:
            self._defs = defs
            self._last_defs_refresh_date = target_date
        return defs

    async def _refresh_defs_if_needed(self, target_date: date | None = None) -> None:
        target_date = target_date or now_et().date()
        if self._last_defs_refresh_date == target_date and self._defs:
            return

        await self._load_defs_for_date(target_date, mutate=True)

    async def ensure_today_daily_snapshots(self) -> None:
        today = now_et().date()
        await self._refresh_defs_if_needed(today)
        await self._ensure_day_snapshots(self._defs, daily_key(today))

    async def _ensure_day_snapshots(self, defs: dict[str, QuestDef], day_key: str) -> None:
        daily_rollover_quests = [q for q in defs.values() if q.category == "daily" and q.max_rollover_days > 0]
        for q in daily_rollover_quests:
            await db_daily_quest_snapshot_day(
                self.state,
                {
                    "quest_id": q.quest_id,
                    "reward_payload": q.reward_payload,
                    "reward_type": q.reward_type,
                    "target_count": q.target_count,
                    "day_key": day_key,
                },
            )

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

    async def _auto_grant_slot(self, user_id: int, slot: dict, *, roles: List[str] | None = None):
        payload = self._resolve_reward_payload_for_user(
            slot.get("reward_payload") or {},
            roles=roles,
        )
        ok = await db_daily_quest_mark_claimed(
            self.state, user_id, slot["quest_id"], slot["day_key"], auto=True
        )
        if ok:
            await give_reward(self.state, user_id, slot.get("reward_type"), payload)

    async def _apply_rollover_limit(
        self, user_id: int, q: QuestDef, slots: List[dict], *, roles: List[str] | None = None
    ) -> List[dict]:
        if q.max_rollover_days <= 0:
            return slots
        # Do not auto-grant pack rewards; these are handled explicitly via
        # admin tooling so users receive the correct pack selection later.
        if any((s.get("reward_type") or "").lower() == "pack" for s in slots):
            return slots
        pending = [s for s in slots if not s.get("claimed_at")]
        # If at or above the cap, auto grant the oldest pending slot to keep the queue size stable
        while len(pending) > q.max_rollover_days > 0:
            oldest = pending.pop(0)
            await self._auto_grant_slot(user_id, oldest, roles=roles)
            stamp = now_et().isoformat()
            oldest["claimed_at"] = oldest.get("claimed_at") or stamp
            oldest["completed_at"] = oldest.get("completed_at") or stamp
            oldest["auto_granted_at"] = oldest.get("auto_granted_at") or stamp
        return slots

    def _resolve_reward_payload_for_user(
        self, payload: dict, *, roles: List[str] | None = None
    ) -> dict:
        """
        Apply per-role overrides (if present) to the reward payload without
        mutating the stored snapshot.
        """

        data = dict(payload or {})
        pack_by_role = data.get("pack_by_role") or {}
        if not pack_by_role:
            return data

        pack_name = None
        role_names = {r.casefold() for r in (roles or []) if isinstance(r, str)}
        for role_name, pack in pack_by_role.items():
            if isinstance(role_name, str) and role_name.casefold() in role_names:
                pack_name = pack
                break

        if not pack_name:
            pack_name = data.get("default_pack") or data.get("pack")

        if pack_name:
            data["pack"] = pack_name

        return data

    async def _ensure_daily_rollover_slots(
        self, user_id: int, q: QuestDef, today_date: date | None = None, *, roles: List[str] | None = None
    ) -> List[dict]:
        today_key = daily_key(today_date)
        slots = await db_daily_quest_get_slots(self.state, user_id, q.quest_id)
        if not slots:
            slots = [await db_daily_quest_add_slot(self.state, user_id, q, today_key)]

        last_date = self._date_from_key(slots[-1]["day_key"])
        today_date_obj = self._date_from_key(today_key)

        while last_date < today_date_obj:
            last_date = last_date + timedelta(days=1)
            slots = await self._apply_rollover_limit(user_id, q, slots, roles=roles)
            slots.append(await db_daily_quest_add_slot(self.state, user_id, q, self._day_key_from_date(last_date)))

        slots = await self._apply_rollover_limit(user_id, q, slots, roles=roles)

        if roles:
            slots = [
                {**slot, "reward_payload": self._resolve_reward_payload_for_user(slot.get("reward_payload") or {}, roles=roles)}
                for slot in slots
            ]
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
        await self._refresh_defs_if_needed(now_et().date())
        q = self._defs.get(quest_id)
        if not q or not q.active:
            return 0, False
        if q.category == "daily" and q.max_rollover_days > 0:
            return await self._increment_rollover_daily(user_id, q, amount)
        pkey = period_key_for_category(q.category)
        return await db_upsert_progress(self.state, user_id, q.quest_id, pkey, amount, q.target_count)

    async def fast_forward_daily_rollovers(
        self, target_date: date, *, include_user_ids: Iterable[int] | None = None
    ) -> int:
        """Ensure rollover-style daily quests have slots up to ``target_date`` for known users."""
        # First, lock in today's snapshot/slots using the already loaded defs so
        # mid-day quest.json edits don't rewrite the current day's rewards.
        today = now_et().date()
        
        # Use the defs already loaded for today (refreshing if needed) to stamp today's snapshot.
        await self._refresh_defs_if_needed(today)
        await self._ensure_day_snapshots(self._defs, daily_key(today))

        daily_rollover_quests_today = [q for q in self._defs.values() if q.category == "daily" and q.max_rollover_days > 0]
        if not daily_rollover_quests_today:
            return 0

        users = set(await db_daily_quest_list_users(self.state))
        if include_user_ids:
            for uid in include_user_ids:
                try:
                    val = int(uid)
                except Exception:
                    continue
                if val:
                    users.add(val)
        advanced = 0

        # Ensure up through today with the current snapshot (pre-refresh values)
        for uid in users:
            for q in daily_rollover_quests_today:
                await self._ensure_daily_rollover_slots(uid, q, today_date=today)
                advanced += 1

        # Load (but do not persist as "current") the defs for the target rollover day so future slots
        # use the latest quests.json without changing today's in-memory defs.
        future_defs = (
            self._defs if target_date <= today else await self._load_defs_for_date(target_date, mutate=False)
        )
        await self._ensure_day_snapshots(future_defs, daily_key(target_date))
        daily_rollover_quests_future = [q for q in future_defs.values() if q.category == "daily" and q.max_rollover_days > 0]

        for uid in users:
            for q in daily_rollover_quests_future:
                await self._ensure_daily_rollover_slots(uid, q, today_date=target_date)
                advanced += 1
        return advanced

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

    async def claim(
        self, user_id: int, quest_id: str, *, roles: List[str] | None = None
    ) -> Tuple[bool, str]:
        q = self._defs.get(quest_id)
        if not q:
            return False, "Quest not found."
        if q.category == "daily" and q.max_rollover_days > 0:
            slots = await self._ensure_daily_rollover_slots(user_id, q, roles=roles)
            pending = [s for s in slots if not s.get("claimed_at")]
            claimables = [
                s
                for s in pending
                if int(s.get("progress", 0))
                >= int(s.get("target_count") or q.target_count)
            ]
            payload_flags = q.reward_payload or {}
            catch_up_all = bool(
                payload_flags.get("catch_up_all_pending")
                or payload_flags.get("catch_up_all")
            )
            include_all_pending = catch_up_all and self.week1_enabled
            if not claimables:
                return False, "Not completed yet."
            
            # Only attempt to claim slots that have actually hit their target so
            # we don't bounce on partially-complete days when catch-up is
            # enabled—unless launch-week catchup is active, in which case we
            # claim every pending day once any day is completed.
            to_claim = pending if include_all_pending else (claimables if catch_up_all else claimables[:1])

            granted, messages, failures = 0, [], []
            for slot in to_claim:
                ok = await db_daily_quest_mark_claimed(
                    self.state, user_id, q.quest_id, slot["day_key"], auto=False
                )
                if not ok:
                    # Attempt a one-time refresh in case a reset left the slot
                    # row missing or with empty claim markers.
                    refreshed = await db_daily_quest_add_slot(
                        self.state, user_id, q, slot["day_key"]
                    )
                    if not refreshed.get("claimed_at"):
                        ok = await db_daily_quest_mark_claimed(
                            self.state, user_id, q.quest_id, slot["day_key"], auto=False
                        )
                if not ok:
                    failures.append(
                        f"{slot['day_key'].split(':')[-1]}: could not mark claimed"
                    )
                    continue
                try:
                    payload = self._resolve_reward_payload_for_user(
                        slot.get("reward_payload") or q.reward_payload or {},
                        roles=roles,
                    )
                    ack = await give_reward(
                        self.state,
                        user_id,
                        slot.get("reward_type", q.reward_type),
                        payload,
                    )
                except Exception as e:
                    failures.append(
                        f"{slot['day_key'].split(':')[-1]}: reward error: {e}"
                    )
                    continue
                granted += 1
                messages.append(
                    f"{slot['day_key'].split(':')[-1]}: {ack}"
                )

            if granted == 0:
                pending_desc = ", ".join(
                    f"{s['day_key'].split(':')[-1]}={int(s.get('progress', 0))}/"
                    f"{int(s.get('target_count') or q.target_count)}"
                    for s in pending
                )
                return (
                    False,
                    "Already claimed or race condition; try again. "
                    f"(Pending slots: {pending_desc or 'none'}; failures: {', '.join(failures) or 'none'})",
                )
            summary = "\n".join(messages)
            return True, f"Reward claimed for {granted} day(s)!\n{summary}"
        
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
        pack_name = data.get("pack")
        if not pack_name:
            raise RuntimeError("Pack reward payload missing 'pack' name.")

        qty = max(1, int(data.get("qty", 1)))

        if not hasattr(state, "shop") or not hasattr(state.shop, "grant_pack"):
            raise RuntimeError("Pack reward helper not wired (expected state.shop.grant_pack).")

        ack = await maybe_await(state.shop.grant_pack(user_id, pack_name, qty))
        return ack or f"Granted {qty}× {pack_name} pack(s)."

    raise RuntimeError(f"Unknown reward_type: {reward_type}")
