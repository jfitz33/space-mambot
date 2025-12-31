# cogs/sales.py
import os
import math
import random
import asyncio
from typing import Optional, Any, Dict, List

import discord
from discord.ext import commands
from discord import app_commands

# Guild scoping
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

# DB helpers (you provided these)
import core.db as db
from core.daily_rollover import rollover_day_key, seconds_until_next_rollover

# Craft costs + rarity helper
from core.constants import (
    CRAFT_COST_BY_RARITY,
    PACKS_BY_SET,
    SALE_DISCOUNT_PCT,
    SALE_LAYOUT,
    CURRENT_ACTIVE_SET,
    pack_names_for_set,
)
from core.cards_shop import get_card_rarity  # normalizes rarity across your data
from core.tins import is_tin_promo_print

DISCOUNT_PCT = SALE_DISCOUNT_PCT
TARGET_RARITIES = [rarity for (rarity, _count) in SALE_LAYOUT]


def _today_key() -> str:
    return rollover_day_key()


class Sales(commands.Cog):
    """
    Rolls the daily sale lineup (multiple slots per rarity) and refreshes the ShopSim banner.
    Uses your db_sales_* helpers exactly as provided.
    """
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state = bot.state
        self._task: Optional[asyncio.Task] = None
        self._last_roll_day_key: Optional[str] = None

    # ---------- lifecycle ----------

    async def cog_load(self):
        # Start the rollover loop
        self._task = asyncio.create_task(self._rollover_loop())

        # Ensure today's sales exist; if not, roll them now; always refresh banner once
        day_key = _today_key()
        try:
            today_rows = db.db_sales_get_for_day(self.state, day_key) or {}
            if not today_rows:
                print("[sales] No sales for today; rolling at startup.")
                await self._roll_and_store_for_day(day_key)
            else:
                total_rows = sum(len(v) for v in today_rows.values())
                print(f"[sales] Found {total_rows} sale rows for {day_key}.")
                self._last_roll_day_key = day_key
            await self._refresh_banner()
        except Exception as e:
            print("[sales] initial check failed:", e)

    async def cog_unload(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print("[sales] error stopping task:", e)
            self._task = None

    # ---------- internals ----------

    async def _rollover_loop(self):
        while True:
            try:
                await asyncio.sleep(seconds_until_next_rollover())
                await self.roll_for_day(_today_key())
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print("[sales] rollover loop error:", e)
                await asyncio.sleep(5)

    async def _roll_and_store_for_day(self, day_key: str):
        """
        Build a fresh set of sales rows following SALE_LAYOUT, then store with db_sales_replace_for_day.
        """
        rows = self._pick_sales_rows()
        db.db_sales_replace_for_day(self.state, day_key, rows)
        self._last_roll_day_key = day_key

    async def roll_for_day(self, day_key: str):
        """Roll sales for the given ET day and refresh the banner."""
        if self._last_roll_day_key == day_key:
            print(f"[sales] roll for {day_key} skipped (already rolled).")
            return
        await self._roll_and_store_for_day(day_key)
        await self._refresh_banner()

    def _pick_sales_rows(self) -> List[Dict[str, Any]]:
        """
        Choose random craftable printings for each slot defined in SALE_LAYOUT.
        """
        pi = getattr(self.state, "packs_index", None) or {}
        out: List[Dict[str, Any]] = []

        # Pre-index all cards by rarity
        buckets: Dict[str, List[Dict[str, Any]]] = {r: [] for r in TARGET_RARITIES}

        active_set_id = CURRENT_ACTIVE_SET if CURRENT_ACTIVE_SET in PACKS_BY_SET else None
        latest_set_id = max(PACKS_BY_SET) if PACKS_BY_SET else None
        allowed_pack_names = set(pack_names_for_set(self.state, active_set_id)) if active_set_id else set()

        # Fallback to the latest available set if the configured set is missing
        if not allowed_pack_names and latest_set_id and latest_set_id != active_set_id:
            allowed_pack_names = set(pack_names_for_set(self.state, latest_set_id))
        if not allowed_pack_names:
            return out

        for pack_name, pack in pi.items():
            if allowed_pack_names and pack_name not in allowed_pack_names:
                continue
            by_rarity = pack.get("by_rarity") or {}
            for rkey, items in by_rarity.items():
                for card in items or []:
                    rarity = (get_card_rarity(card) or "").lower()
                    if rarity in buckets:
                        # Only include craftable rarities (must have base cost)
                        base_cost = CRAFT_COST_BY_RARITY.get(rarity)
                        if base_cost:
                            if is_tin_promo_print(self.state, card, set_name=pack_name):
                                continue
                            # stash minimal info + where it came from
                            buckets[rarity].append({
                                "card": card,
                                "pack": pack_name,
                                "base_cost": int(base_cost),
                            })

        for rarity, count in SALE_LAYOUT:
            candidates = buckets.get(rarity) or []
            if not candidates or count <= 0:
                continue

            picks: List[Dict[str, Any]] = []
            # If we have two packs for the active set and multiple slots, try to split evenly.
            pack_list = sorted(list(allowed_pack_names))
            if len(pack_list) == 2 and count > 1:
                per_pack_counts = [count // 2, count // 2]
                for i in range(count - sum(per_pack_counts)):
                    per_pack_counts[i % 2] += 1

                remaining_pool = candidates[:]
                for pack_name, target_count in zip(pack_list, per_pack_counts):
                    pack_pool = [c for c in remaining_pool if c.get("pack") == pack_name]
                    if not pack_pool or target_count <= 0:
                        continue
                    take = min(target_count, len(pack_pool))
                    choices = random.sample(pack_pool, k=take)
                    picks.extend(choices)
                    for choice in choices:
                        if choice in remaining_pool:
                            remaining_pool.remove(choice)

                remaining_slots = min(count, len(candidates)) - len(picks)
                if remaining_slots > 0 and remaining_pool:
                    picks.extend(random.sample(remaining_pool, k=min(remaining_slots, len(remaining_pool))))
            else:
                picks = random.sample(candidates, k=min(count, len(candidates)))

            for choice in picks:
                card = choice["card"]
                set_name = choice["pack"]
                base_cost = choice["base_cost"]

                price_shards = math.ceil(base_cost * (100 - DISCOUNT_PCT) / 100.0)

                # Canonicalize fields (accept both csv/short keys)
                name = (card.get("name") or card.get("cardname") or "").strip()
                code = (card.get("code") or card.get("cardcode")) or None
                cid  = (card.get("id")   or card.get("cardid"))   or None

                out.append({
                    "rarity": rarity,
                    "card_name": name,
                    "card_set": set_name,
                    "card_code": code,
                    "card_id": cid,
                    "discount_pct": DISCOUNT_PCT,
                    "price_shards": int(price_shards),
                })

        return out

    async def _refresh_banner(self):
        """
        Ask ShopSim to rebuild/edit the single shop message. It should render current
        sales by calling db_sales_get_for_day(...) internally.
        """
        shop = self.bot.get_cog("ShopSim")
        if not shop or not hasattr(shop, "refresh_shop_banner"):
            print("[sales] ShopSim not loaded or missing refresh_shop_banner; skipping banner refresh.")
            return

        # Resolve guild to refresh
        guild = None
        if GUILD_ID:
            guild = self.bot.get_guild(GUILD_ID)
        if guild is None and self.bot.guilds:
            guild = self.bot.guilds[0]
        if guild is None:
            print("[sales] No guild available to refresh banner.")
            return

        try:
            await shop.refresh_shop_banner(guild)
        except Exception as e:
            print("[sales] refresh_shop_banner failed:", e)

    # ---------- commands ----------

    @app_commands.command(name="sales_reset", description="(Admin) Re-roll today's sale items and refresh the shop banner.")
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def sales_reset(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            await self._roll_and_store_for_day(_today_key())
            await self._refresh_banner()
            await interaction.followup.send("✅ Sales re-rolled for today (ET) and banner refreshed.", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Failed to reset sales: {e}", ephemeral=True)

    @app_commands.command(name="sales_show", description="(Admin) Show today's sale rows (debug).")
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def sales_show(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        day_key = _today_key()
        try:
            rows_by_rarity = db.db_sales_get_for_day(self.state, day_key) or {}
            total_rows = sum(len(v) for v in rows_by_rarity.values())
            if not total_rows:
                await interaction.followup.send(f"No sales for {day_key}.", ephemeral=True)
                return
            # Render quick summary
            lines: List[str] = [f"Sales for {day_key} (ET):"]
            for rarity, _count in SALE_LAYOUT:
                rows = rows_by_rarity.get(rarity) or []
                if not rows:
                    continue
                for idx, row in enumerate(rows, start=1):
                    suffix = f" #{idx}" if len(rows) > 1 else ""
                    nm = row.get("card_name", "?")
                    st = row.get("card_set", "?")
                    pct = int(row.get("discount_pct", DISCOUNT_PCT))
                    price = row.get("price_shards")
                    lines.append(
                        f"• [{rarity}{suffix}] {nm} — set:{st} (−{pct}%) → {price} shards"
                    )
            # Include any legacy rarities that might still be stored
            for rarity, rows in rows_by_rarity.items():
                if rarity in TARGET_RARITIES:
                    continue
                for row in rows:
                    nm = row.get("card_name", "?")
                    st = row.get("card_set", "?")
                    pct = int(row.get("discount_pct", DISCOUNT_PCT))
                    price = row.get("price_shards")
                    lines.append(
                        f"• [{rarity}] {nm} — set:{st} (−{pct}%) → {price} shards"
                    )
            await interaction.followup.send("\n".join(lines), ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"Error: {e}", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(Sales(bot))
