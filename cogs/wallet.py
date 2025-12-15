import os
import discord
from typing import Optional
from discord.ext import commands
from discord import app_commands

from core.state import AppState
from core.db import db_wallet_get, db_shards_get
from core.constants import PACKS_BY_SET

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

def _shard_badge_or_label(state, set_id: int) -> str:
    rid = getattr(state, "rarity_emoji_ids", {}) or {}
    anim = getattr(state, "rarity_emoji_animated", {}) or {}
    sid = int(set_id)
    key, fallback = {
        1: ("frostfire", "Frostfire"),
        2: ("sandstorm", "Sandstorm"),
        3: ("temporal", "Temporal"),
    }.get(sid, (None, f"Set {sid}"))

    if key:
        eid = rid.get(key)
        if eid:
            prefix = "a" if anim.get(key) else ""
            return f"<{prefix}:rar_{key}:{eid}>"
        return fallback
    return fallback

class Wallet(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: AppState = self.bot.state

    @app_commands.command(name="wallet", description="View a wallet's balances")
    @app_commands.guilds(GUILD)  # keep guild-scoped as requested
    @app_commands.describe(user="(Optional) Whose wallet to view; defaults to you")
    async def wallet(self, interaction: discord.Interaction, user: Optional[discord.Member] = None):
        target = user or interaction.user

        bal = db_wallet_get(self.state, target.id)
        mambucks = int(bal.get("mambucks", 0))

        # Build shards breakdown
        set_ids = sorted(PACKS_BY_SET.keys()) or [1]  # default include Set 1 if mapping empty
        lines = []
        for sid in set_ids:
            amt = db_shards_get(self.state, target.id, sid)
            # Show a short label like "Frostfire" instead of "Frostfire Shards"
            badge = _shard_badge_or_label(self.state, sid)
            lines.append(f"{badge} {amt}")
        shards_value = "\n".join(lines) if lines else "—"

        embed = discord.Embed(
            title=f"{target.display_name}'s Wallet",
            color=0x2b6cb0
        )
        if target.display_avatar:
            embed.set_thumbnail(url=target.display_avatar.url)

        # Field 1: Mambucks
        embed.add_field(name="Mambucks", value=f"**{mambucks}**", inline=True)
        # Field 2: Shards (multi-line)
        embed.add_field(name="Shards", value=shards_value, inline=True)

        await interaction.response.send_message(embed=embed)

async def setup(bot: commands.Bot):
    await bot.add_cog(Wallet(bot))
