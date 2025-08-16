import os
import discord
from typing import Optional
from discord.ext import commands
from discord import app_commands

from core.state import AppState
from core.db import db_wallet_get

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

class Wallet(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: AppState = self.bot.state

    @app_commands.command(name="wallet", description="View a wallet's balances")
    @app_commands.guilds(GUILD)  # remove this decorator to make the command global
    @app_commands.describe(user="(Optional) Whose wallet to view; defaults to you")
    async def wallet(self, interaction: discord.Interaction, user: Optional[discord.Member] = None):
        # Default to the caller if no user provided
        target = user or interaction.user

        bal = db_wallet_get(self.state, target.id)

        embed = discord.Embed(
            title=f"{target.display_name}'s Wallet",
            color=0x2b6cb0
        )
        embed.set_thumbnail(url=target.display_avatar.url if target.display_avatar else discord.Embed.Empty)
        embed.add_field(name="fitzcoin", value=f"**{bal['fitzcoin']}**", inline=True)
        embed.add_field(name="mambucks", value=f"**{bal['mambucks']}**", inline=True)

        # Public message (not ephemeral)
        await interaction.response.send_message(embed=embed)

async def setup(bot: commands.Bot):
    await bot.add_cog(Wallet(bot))
