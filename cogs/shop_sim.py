# cogs/shop_sim.py
import os, asyncio, discord, math, re
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List
from discord.ext import commands
from discord import app_commands

from core.feature_flags import is_set1_week1_locked
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
    SALE_LAYOUT,
    CURRENT_ACTIVE_SET,
    set_id_for_pack,
)
from core.currency import SHARD_SET_NAMES
from core.images import mambuck_badge
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

def _shard_badge(state: AppState, set_id: int = CURRENT_ACTIVE_SET) -> str:
    rid = getattr(state, "rarity_emoji_ids", {}) or {}
    anim = getattr(state, "rarity_emoji_animated", {}) or {}
    shard_key = {1: "frostfire", 2: "sandstorm", 3: "temporal"}.get(set_id, "frostfire")

    eid = rid.get(shard_key) or rid.get("frostfire")
    if eid:
        prefix = "a" if anim.get(shard_key) else ""
        return f"<{prefix}:rar_{shard_key}:{eid}>"
    return "💠"

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
        if is_set1_week1_locked():
            return discord.Embed(
                title="🛍️ Welcome to the Mamshop",
                description="Shop opening soon",
                color=0x2b6cb0,
            )
        
        e = discord.Embed(
            title="🛍️ Welcome to the Mamshop",
            description=(
                "Use **/pack** or **/box** to buy packs!\n"
                "Use **/fragment** to convert cards into shards, and **/craft** to turn shards into cards!\n\n"
            ),
            color=0x2b6cb0,
        )

        # Prices (packs/boxes)
        mambuck_icon = mambuck_badge(self.state)
        pack_names = sorted((self.state.packs_index or {}).keys(), key=str.casefold)
        if pack_names:
            packs_by_set: dict[int | None, list[str]] = {}
            for name in pack_names:
                sid = set_id_for_pack(name)
                packs_by_set.setdefault(sid, []).append(name)

            ordered_set_ids = sorted([sid for sid in packs_by_set.keys() if sid is not None])
            if None in packs_by_set:
                ordered_set_ids.append(None)

            pack_lines = ["• Available packs:"]
            for sid in ordered_set_ids:
                label = f"Set {sid}" if sid is not None else "Other"
                names = ", ".join(sorted(packs_by_set.get(sid, []), key=str.casefold))
                indent = "\u2003\u2003"  # use em-spaces to preserve indentation in Discord
                pack_lines.append(f"{indent}• {label}: {names}")
            pack_text = "\n".join(pack_lines)
        else:
            pack_text = "• Available packs: None"
        e.add_field(
            name="📦 Sealed Products 📦",
            value=(
                "\n"
                f"{pack_text}\n"
                f"• Pack: **{PACK_COST} {mambuck_icon} mambucks**\n"
                f"• Box (24 packs): **{BOX_COST} {mambuck_icon} mambucks**\n"
                f"• Bundle (1 box of each pack): **{BUNDLE_BOX_COST} {mambuck_icon} mambucks**\n"
                f"• Tin (1 promo card and 5 packs): **{TIN_COST} {mambuck_icon} mambucks**"
            ),
            inline=False,
        )

        # Craft/fragment prices by rarity (shards)
        shard_badge = _shard_badge(self.state)
        emoji_sized_space = "\u2800"  # Braille blank; roughly emoji-width whitespace for alignment
        table_rows: list[tuple[str, str, str]] = []

        for rarity in RARITY_ORDER:
            craft_cost = CRAFT_COST_BY_RARITY.get(rarity)
            fragment_yield = SHARD_YIELD_BY_RARITY.get(rarity)
            if craft_cost is None or fragment_yield is None:
                continue

            rarity_label = rarity.title()
            badge = _rar_badge(self.state, rarity)
            rarity_cell = f"{badge} {rarity_label}"
            craft_cell = f"{craft_cost} {shard_badge}"
            fragment_cell = f"{fragment_yield} {shard_badge}"
            table_rows.append((rarity_cell, craft_cell, fragment_cell))

        if table_rows:
            rarity_width = max(len("Rarity"), *(len(rarity.split(" ", 1)[1]) for rarity, _, _ in table_rows))
            craft_width = max(len("Craft"), *(len(cost.split(" ", 1)[0]) for _, cost, _ in table_rows))
            frag_width = max(len("Fragment"), *(len(frag.split(" ", 1)[0]) for _, _, frag in table_rows))

            def _row_to_line(cells: tuple[str, str, str]) -> str:
                rarity_cell, craft_cell, frag_cell = cells
                rarity_label = rarity_cell.split(" ", 1)[1]
                badge = rarity_cell.split(" ", 1)[0]
                rarity_txt = rarity_label.ljust(rarity_width)
                craft_txt = craft_cell.split(" ", 1)[0].rjust(craft_width)
                frag_txt = frag_cell.split(" ", 1)[0].rjust(frag_width)
                return (
                    f"{badge} `{rarity_txt}` "
                    f"`{craft_txt}` {shard_badge} "
                    f"`{frag_txt}` {shard_badge}"
                )

            header_spacing = emoji_sized_space
            header = (
                f"{header_spacing}{header_spacing} `{'Rarity'.ljust(rarity_width)}` "
                f"`{'Craft'.rjust(craft_width)}` {header_spacing} "
                f"{header_spacing}`{'Fragment'.rjust(frag_width)}` {header_spacing}"
            )
            lines = [header] + [_row_to_line(row) for row in table_rows]
            e.add_field(name="💎 Crafting & Fragmenting 💎", value="\n".join(lines), inline=False)
        else:
            e.add_field(name="Rarity | Craft | Fragment", value="—", inline=False)

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
                    f"🔥 Today's sales, craft for **{discounts.pop()}%** off! 🔥"
                    if len(discounts) == 1 else
                    f"🔥 On sale today for **up to {max(discounts)}%** off! 🔥"
                )
            else:
                sale_title = "🔥 On sale today! 🔥"

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
                    lines.append(f"{badge} {name}")
                
            # Include any leftover rarities (legacy data) at the end
            for rarity, rows in sales.items():
                if rarity in targeted:
                    continue
                for row in rows or []:
                    badge = _rar_badge(self.state, rarity)
                    name = row.get("card_name", "?")
                    price = int(row.get("price_shards", 0))
                    lines.append(f"{badge} {name}")

            if lines:
                e.add_field(name=sale_title, value="\n" + "\n".join(lines), inline=False)

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
