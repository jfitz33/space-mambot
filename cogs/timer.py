import os
import time
import math
import discord
from discord.ext import commands
from discord import app_commands

from core.db import db_timer_set, db_timer_get, db_timer_clear

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

class Timer(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state = bot.state

    @app_commands.command(name="set_timer", description="(Admin) Set a timer in minutes")
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(minutes="Number of minutes to run the timer")
    async def set_timer(
        self,
        interaction: discord.Interaction,
        minutes: app_commands.Range[int, 1, None],
    ):
        if not interaction.guild_id:
            await interaction.response.send_message(
                "❌ This command can only be used in a server.",
                ephemeral=True,
            )
            return

        end_ts = int(time.time()) + int(minutes) * 60
        db_timer_set(self.state, interaction.guild_id, end_ts)

        await interaction.response.send_message(
            f"✅ Timer set for {int(minutes)} minute(s).",
            ephemeral=True,
        )

    @app_commands.command(name="timer", description="Show time remaining on the current timer")
    @app_commands.guilds(GUILD)
    async def timer(self, interaction: discord.Interaction):
        if not interaction.guild_id:
            await interaction.response.send_message(
                "no active timer",
                ephemeral=True,
            )
            return

        end_ts = db_timer_get(self.state, interaction.guild_id)
        if not end_ts:
            await interaction.response.send_message("no active timer")
            return

        remaining = end_ts - time.time()
        if remaining <= 0:
            db_timer_clear(self.state, interaction.guild_id)
            await interaction.response.send_message("no active timer")
            return

        remaining_seconds = int(math.ceil(remaining))
        minutes_left, seconds_left = divmod(remaining_seconds, 60)
        await interaction.response.send_message(
            f"{minutes_left}:{seconds_left:02d} remaining"
        )

async def setup(bot: commands.Bot):
    await bot.add_cog(Timer(bot))