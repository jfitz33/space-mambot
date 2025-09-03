# cogs/sales.py
import os
import math
import random
import asyncio
from typing import Optional, Any, Dict, List
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import discord
from discord.ext import commands
from discord import app_commands

# Guild scoping
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

# DB helpers (you provided these)
import core.db as db

# Craft costs + rarity helper
from core.constants import CRAFT_COST_BY_RARITY, SALE_DISCOUNT_PCT
from core.cards_shop import get_card_rarity  # normalizes rarity across your data

ET = ZoneInfo("America/New_York")
DISCOUNT_PCT = SALE_DISCOUNT_PCT
TARGET_RARITIES = ["common", "rare", "super", "ultra", "secret"]


def _today_key_et() -> str:
    return datetime.now(ET).strftime("%Y%m%d")


def _seconds_until_next_et_midnight() -> float:
    now = datetime.now(ET)
    tomorrow = (now + timedelta(days=1)).date()
    next_midnight = datetime.combine(tomorrow, datetime.min.time(), tzinfo=ET)
    return max(1.0, (next_midnight - now).total_seconds())


class Sales(commands.Cog):
    """
    Rolls one sale card per rarity each ET day and refreshes the ShopSim banner.
    Uses your db_sales_* helpers exactly as provided.
    """
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state = bot.state
        self._task: Optional[asyncio.Task] = None

    # ---------- lifecycle ----------

    async def cog_load(self):
        # Start the midnight loop
        self._task = asyncio.create_task(self._midnight_loop())

        # Ensure today's sales exist; if not, roll them now; always refresh banner once
        day_key = _today_key_et()
        try:
            today_rows = db.db_sales_get_for_day(self.state, day_key) or {}
            if not today_rows:
                print("[sales] No sales for today; rolling at startup.")
                await self._roll_and_store_for_day(day_key)
            else:
                print(f"[sales] Found {len(today_rows)} sale rows for {day_key}.")
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

    async def _midnight_loop(self):
        while True:
            try:
                await asyncio.sleep(_seconds_until_next_et_midnight())
                await self._roll_and_store_for_day(_today_key_et())
                await self._refresh_banner()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print("[sales] midnight loop error:", e)
                await asyncio.sleep(5)

    async def _roll_and_store_for_day(self, day_key: str):
        """
        Build a fresh set of sales rows (one per rarity), then store with db_sales_replace_for_day.
        """
        rows = self._pick_sales_rows()
        db.db_sales_replace_for_day(self.state, day_key, rows)

    def _pick_sales_rows(self) -> List[Dict[str, Any]]:
        """
        Choose one random craftable printing per rarity from state.packs_index.
        """
        pi = getattr(self.state, "packs_index", None) or {}
        out: List[Dict[str, Any]] = []

        # Pre-index all cards by rarity
        buckets: Dict[str, List[Dict[str, Any]]] = {r: [] for r in TARGET_RARITIES}

        for pack_name, pack in pi.items():
            by_rarity = pack.get("by_rarity") or {}
            for rkey, items in by_rarity.items():
                for card in items or []:
                    rarity = (get_card_rarity(card) or "").lower()
                    if rarity in buckets:
                        # Only include craftable rarities (must have base cost)
                        base_cost = CRAFT_COST_BY_RARITY.get(rarity)
                        if base_cost:
                            # stash minimal info + where it came from
                            buckets[rarity].append({
                                "card": card,
                                "pack": pack_name,
                                "base_cost": int(base_cost),
                            })

        for rarity in TARGET_RARITIES:
            candidates = buckets.get(rarity) or []
            if not candidates:
                # nothing in this rarity; skip
                continue
            choice = random.choice(candidates)
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
            await self._roll_and_store_for_day(_today_key_et())
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
        day_key = _today_key_et()
        try:
            rows_by_rarity = db.db_sales_get_for_day(self.state, day_key) or {}
            if not rows_by_rarity:
                await interaction.followup.send(f"No sales for {day_key}.", ephemeral=True)
                return
            # Render quick summary
            lines: List[str] = [f"Sales for {day_key} (ET):"]
            for rarity in TARGET_RARITIES:
                r = rows_by_rarity.get(rarity)
                if not r:
                    continue
                nm = r.get("card_name", "?")
                st = r.get("card_set", "?")
                pct = int(r.get("discount_pct", DISCOUNT_PCT))
                price = r.get("price_shards")
                lines.append(f"• [{rarity}] {nm} — set:{st} (−{pct}%) → {price} shards")
            await interaction.followup.send("\n".join(lines), ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"Error: {e}", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(Sales(bot))
