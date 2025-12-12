# cogs/shop_sim.py
import os, asyncio, discord, math
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List
from discord.ext import commands
from discord import app_commands

from core.state import AppState
from core.cards_shop import ensure_shop_index
from core.constants import (
    PACK_COST,
    BOX_COST,
    PACKS_IN_BOX,
    CRAFT_COST_BY_RARITY,
    SHARD_YIELD_BY_RARITY,
    BUNDLE_BOX_COST,
    TIN_COST,
    RARITY_ORDER,
    SALE_LAYOUT
)
from core.currency import SHARD_SET_NAMES
from core.db import db_sales_get_for_day, db_shop_banner_load, db_shop_banner_store

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None
SHOP_CHANNEL_NAME = "shop"
ET = ZoneInfo("America/New_York")

def _today_key_et() -> str:
    return datetime.now(ET).strftime("%Y%m%d")

def _rar_badge(state: AppState, rarity: str) -> str:
    rid = getattr(state, "rarity_emoji_ids", {}) or {}
    anim = getattr(state, "rarity_emoji_animated", {}) or {}
    key = (rarity or "").strip().lower()
    eid = rid.get(key)
    if eid:
        prefix = "a" if anim.get(key) else ""
        return f"<{prefix}:rar_{key}:{eid}>"
    # fallback text
    short = {"common":"C","rare":"R","super":"SR","ultra":"UR","secret":"SEC","starlight":"SL"}.get(key, key[:1].upper())
    return f"[{short}]"

def _pretty_shard_name_for_set(set_id: int) -> str:
    return SHARD_SET_NAMES.get(set_id, f"Shards (Set {set_id})")


async def ensure_shop_channel(guild: discord.Guild, bot_user: discord.Member) -> discord.TextChannel:
    chan = discord.utils.get(guild.text_channels, name=SHOP_CHANNEL_NAME)
    if chan is None:
        try:
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=False,
                    send_messages_in_threads=False,
                    create_public_threads=False,
                    create_private_threads=False,
                    add_reactions=False,
                    use_application_commands=False,  # hide slash-commands for non-bot users here
                ),
                bot_user: discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=True,
                    send_messages_in_threads=True,
                    embed_links=True,
                    attach_files=True,
                    read_message_history=True,
                    manage_messages=True,  # lets the bot pin/unpin reliably
                    use_external_emojis=True,
                ),
            }
            chan = await guild.create_text_channel(SHOP_CHANNEL_NAME, overwrites=overwrites, reason="Create locked shop channel")
        except discord.Forbidden:
            chan = await guild.create_text_channel(SHOP_CHANNEL_NAME, reason="Create shop channel (no overwrites)")
    else:
        try:
            ow = chan.overwrites
            ow[guild.default_role] = discord.PermissionOverwrite(view_channel=True, send_messages=False, add_reactions=False)
            ow[bot_user] = discord.PermissionOverwrite(view_channel=True, send_messages=True, embed_links=True, attach_files=True, read_message_history=True)
            await chan.edit(overwrites=ow, reason="Lock shop channel to bot-only posting")
        except discord.Forbidden:
            pass
    return chan

async def _clear_channel_all_messages(channel: discord.TextChannel):
    # Unpin everything first (pinned can block bulk deletion)
    try:
        for p in await channel.pins():
            try:
                await p.unpin(reason="Refreshing shop message")
            except Exception:
                pass
    except Exception:
        pass

    # Try bulk purge a bunch (fast path)
    try:
        await channel.purge(limit=1000, check=lambda m: True, bulk=True, reason="Refreshing shop message")
        return
    except Exception:
        # Fallback: slow path—delete recent history manually
        try:
            async for m in channel.history(limit=200):
                try:
                    await m.delete()
                except Exception:
                    pass
        except Exception:
            pass


class ShopSim(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: AppState = bot.state

    def _build_shop_embed(self, *, sales: dict | None = None) -> discord.Embed:
        """
        Build the single shop banner embed. If ``sales`` is provided (mapping rarity
        → list of sale rows), render a "On sale today for <discount>% off!" section
        with compact rows.

        Discord's embed text uses a fixed font and emoji size; there isn't an option
        to enlarge these without rendering them into an image first.
        """
        e = discord.Embed(
            title="🛍️ Welcome to the Mamshop",
            description=(
                "Use **/pack** or **/box** to buy packs!\n"
                "Use **/fragment** to convert cards into shards, and **/craft** to turn shards into cards!\n"
            ),
            color=0x2b6cb0,
        )

        # Prices (packs/boxes)
        e.add_field(
            name="Sealed Products",
            value=f"• Pack: **{PACK_COST} mambucks**\n• Box (24 packs): **{BOX_COST} mambucks**\n• Bundle (1 box of each pack): **{BUNDLE_BOX_COST} mambucks**\n• Tin (1 promo card and 5 packs): **{TIN_COST} mambucks**",
            inline=False,
        )

        # Craft prices by rarity (shards)
        craft_lines = []
        for r, cost in CRAFT_COST_BY_RARITY.items():
            badge = _rar_badge(self.state, r)
            craft_lines.append(f"{badge} **{cost} shards** → **{r.title()}**")
        e.add_field(name="Craft Prices", value="\n".join(craft_lines) or "—", inline=False)

        # Fragment prices by rarity (shards)
        craft_lines = []
        for r, cost in SHARD_YIELD_BY_RARITY.items():
            badge = _rar_badge(self.state, r)
            craft_lines.append(f"{badge} **{r.title()}** → **{cost} shards**")
        e.add_field(name="Fragment Prices", value="\n".join(craft_lines) or "—", inline=False)

        # Sales section (compact format)
        if sales:
            # Determine a friendly header
            discounts = {
                int(row.get("discount_pct", 0))
                for entries in sales.values()
                for row in entries or []
                if row
            }
            if discounts:
                sale_title = (
                    f"🔥 On sale today for **{discounts.pop()}%** off!"
                    if len(discounts) == 1 else
                    f"🔥 On sale today for **up to {max(discounts)}%** off!"
                )
            else:
                sale_title = "🔥 On sale today!"

            lines = []
            
            targeted = {rarity for rarity, _ in SALE_LAYOUT}
            for rarity, count in SALE_LAYOUT:
                rows = sales.get(rarity) or []
                if not rows:
                    continue
                for row in rows[:count]:
                    badge = _rar_badge(self.state, rarity)
                    name = row.get("card_name", "?")
                    price = int(row.get("price_shards", 0))
                    lines.append(f"{badge} {name} -> **{price} shards**")
                
            # Include any leftover rarities (legacy data) at the end
            for rarity, rows in sales.items():
                if rarity in targeted:
                    continue
                for row in rows or []:
                    badge = _rar_badge(self.state, rarity)
                    name = row.get("card_name", "?")
                    price = int(row.get("price_shards", 0))
                    lines.append(f"{badge} {name} -> **{price} shards**")

            if lines:
                e.add_field(name=sale_title, value="\n".join(lines), inline=False)

        e.set_footer(text="Check back tomorrow for new deals!")
        return e

    async def refresh_shop_banner(self, guild: discord.Guild):
        """
        Resolve today's sales from DB and edit/create the single shop banner message.
        """
        # 1) Ensure channel exists
        bot_member = guild.get_member(self.bot.user.id) if self.bot.user else None
        channel = await ensure_shop_channel(guild, bot_member)

        # 1b) Make sure shop data is indexed for the embed
        ensure_shop_index(self.state)

        # 2) Fetch today's sales (ET)
        day_key = _today_key_et()
        sales = db_sales_get_for_day(self.state, day_key) or {}

        # 3) Build embed (with sales)
        embed = self._build_shop_embed(sales=sales)

        # 4) Try to edit the existing banner
        row = db_shop_banner_load(self.state, guild.id)
        if row:
            try:
                if int(row["channel_id"]) != channel.id:
                    # Channel changed: fall through to send a new one
                    raise discord.NotFound(response=None, message="channel mismatch")
                msg = await channel.fetch_message(int(row["message_id"]))
                await msg.edit(content=None, embed=embed, view=None)
                return msg
            except (discord.NotFound, discord.Forbidden):
                pass  # will create a new one below

        # 5) Clean up old messages (keep only the new banner)
        try:
            await channel.purge(limit=100)
        except Exception:
            pass

        # 6) Send + remember
        msg = await channel.send(embed=embed)
        db_shop_banner_store(self.state, guild.id, channel.id, msg.id)
        return msg

    # Optional: manual refresh command (admin only)
    @app_commands.command(name="shop_refresh", description="(Admin) Refresh the shop message in #shop")
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def shop_refresh(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)

        guild = interaction.guild
        if not guild:
            return await interaction.followup.send("Use this in a server.", ephemeral=True)
        bot_member = guild.get_member(self.bot.user.id) if self.bot.user else None
        if not bot_member:
            return await interaction.followup.send("Could not resolve bot member.", ephemeral=True)
        print(self.bot.state.rarity_emoji_ids)
        ensure_shop_index(self.state)
        channel = await ensure_shop_channel(guild, bot_member)
        await _clear_channel_all_messages(channel)
        emb = self._build_shop_embed()
        msg = await channel.send(embed=emb, content=" ")
        try:
            await msg.pin(reason="Pin shop info")
        except discord.Forbidden:
            pass

        await interaction.followup.send(f"Refreshed shop in {channel.mention}.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(ShopSim(bot))
