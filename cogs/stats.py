import os, math, discord
from typing import Optional
from discord.ext import commands
from discord import app_commands

from core.state import AppState
from core.db import (
    db_init_user_stats, db_init_match_log,
    db_stats_get, db_stats_record_loss,
    db_match_h2h,
)

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

def _win_pct(wins: int, games: int) -> float:
    return (wins / games * 100.0) if games > 0 else 0.0

class Stats(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: AppState = bot.state

    async def cog_load(self):
        db_init_user_stats(self.state)
        db_init_match_log(self.state)

    @app_commands.command(name="loss", description="Record a loss to another player (updates both players' stats).")
    @app_commands.guilds(GUILD)
    @app_commands.describe(opponent="The player you lost to")
    async def loss(self, interaction: discord.Interaction, opponent: discord.Member):
        caller = interaction.user
        if opponent.id == caller.id:
            return await interaction.response.send_message("You can’t record a loss to yourself.", ephemeral=True)
        if opponent.bot:
            return await interaction.response.send_message("You can’t record a loss to a bot.", ephemeral=True)

        await interaction.response.defer(ephemeral=False, thinking=True)

        # Atomically update stats + log match
        loser_after, winner_after = db_stats_record_loss(self.state, loser_id=caller.id, winner_id=opponent.id)

        # Optional quest ticks using your QuestManager wrappers/IDs
        quests = interaction.client.get_cog("Quests")
        try:
            if quests and getattr(quests, "qm", None):
                await quests.qm.increment(opponent.id, "win_3_matches", 1)
                await quests.qm.increment(caller.id,   "matches_played", 1)
                await quests.qm.increment(opponent.id, "matches_played", 1)
        except Exception as e:
            print("[stats] quest tick error:", e)

        lpct = _win_pct(loser_after["wins"], loser_after["games"])
        wpct = _win_pct(winner_after["wins"], winner_after["games"])

        embed = discord.Embed(
            title="Match Result Recorded",
            description=f"**{caller.display_name}** lost to **{opponent.display_name}**.",
            color=0xCC3333
        )
        embed.add_field(
            name=f"{caller.display_name} — Record",
            value=(f"W: **{loser_after['wins']}**\n"
                   f"L: **{loser_after['losses']}**\n"
                   f"Win%: **{lpct:.1f}%**"),
            inline=True
        )
        embed.add_field(
            name=f"{opponent.display_name} — Record",
            value=(f"W: **{winner_after['wins']}**\n"
                   f"L: **{winner_after['losses']}**\n"
                   f"Win%: **{wpct:.1f}%**"),
            inline=True
        )

        await interaction.followup.send(embed=embed)

    @app_commands.command(name="stats", description="View a player's win/loss record and win%.")
    @app_commands.guilds(GUILD)
    @app_commands.describe(user="(Optional) Whose stats to view; defaults to you")
    async def stats(self, interaction: discord.Interaction, user: Optional[discord.Member] = None):
        target = user or interaction.user
        data = db_stats_get(self.state, target.id)
        pct = _win_pct(data["wins"], data["games"])

        embed = discord.Embed(
            title=f"{target.display_name}'s Stats",
            color=0x2b6cb0
        )
        if target.display_avatar:
            embed.set_thumbnail(url=target.display_avatar.url)

        embed.add_field(name="Wins", value=f"**{data['wins']}**", inline=True)
        embed.add_field(name="Losses", value=f"**{data['losses']}**", inline=True)
        embed.add_field(name="Win %", value=f"**{pct:.1f}%**", inline=True)

        await interaction.response.send_message(embed=embed)

    # NEW: head-to-head
    @app_commands.command(name="h2h", description="Head-to-head record: you vs another player.")
    @app_commands.guilds(GUILD)
    @app_commands.describe(opponent="The player to compare against")
    async def h2h(self, interaction: discord.Interaction, opponent: discord.Member):
        caller = interaction.user
        if opponent.id == caller.id:
            return await interaction.response.send_message("Pick someone else for head-to-head.", ephemeral=True)
        if opponent.bot:
            return await interaction.response.send_message("Bots don’t play matches.", ephemeral=True)

        await interaction.response.defer(ephemeral=False, thinking=False)

        # A perspective: caller is "A", opponent is "B"
        res = db_match_h2h(self.state, caller.id, opponent.id)
        a_wins, b_wins, games = res["a_wins"], res["b_wins"], res["games"]
        a_pct = _win_pct(a_wins, games)
        b_pct = _win_pct(b_wins, games)

        embed = discord.Embed(
            title="Head-to-Head",
            description=f"**{caller.display_name}** vs **{opponent.display_name}**",
            color=0x6b46c1
        )
        embed.add_field(
            name=caller.display_name,
            value=(f"W vs {opponent.display_name}: **{a_wins}**\n"
                   f"L vs {opponent.display_name}: **{b_wins}**\n"
                   f"Win%: **{a_pct:.1f}%**"),
            inline=True
        )
        embed.add_field(
            name=opponent.display_name,
            value=(f"W vs {caller.display_name}: **{b_wins}**\n"
                   f"L vs {caller.display_name}: **{a_wins}**\n"
                   f"Win%: **{b_pct:.1f}%**"),
            inline=True
        )
        embed.set_footer(text=f"Total H2H games: {games}")

        await interaction.followup.send(embed=embed)

async def setup(bot: commands.Bot):
    await bot.add_cog(Stats(bot))
