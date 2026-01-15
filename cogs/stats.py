import os, discord
import asyncio
from typing import Optional
from discord.ext import commands
from discord import app_commands

from core.state import AppState
from core.db import (
    db_init_user_stats, db_init_match_log, db_init_user_set_wins,
    db_stats_get, db_stats_record_loss,
    db_match_h2h, db_team_battleground_user_points_for_user,
)
from core.constants import CURRENT_ACTIVE_SET, TEAM_ROLE_NAMES

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
        db_init_user_set_wins(self.state)

    @app_commands.command(name="report", description="Report the result of a queued duel match.")
    @app_commands.guilds(GUILD)
    @app_commands.describe(opponent="Your opponent", outcome="Did you win or lose?")
    @app_commands.choices(
        outcome=[
            app_commands.Choice(name="I won", value="win"),
            app_commands.Choice(name="I lost", value="loss"),
        ]
    )
    async def report(
        self,
        interaction: discord.Interaction,
        opponent: discord.Member,
        outcome: app_commands.Choice[str],
    ):
        caller = interaction.user
        if opponent.id == caller.id:
            return await interaction.response.send_message(
                "You can’t record a result against yourself.", ephemeral=True
            )
        if opponent.bot:
            return await interaction.response.send_message(
                "You can’t record a result against a bot.", ephemeral=True
            )

        queue = interaction.client.get_cog("DuelQueue")
        is_paired = False
        try:
            if queue and hasattr(queue, "is_active_pair"):
                is_paired = await queue.is_active_pair(caller.id, opponent.id)
        except Exception as e:
            print("[stats] failed to verify duel pairing:", e)

        if not is_paired:
            return await interaction.response.send_message(
                f"no active duel between users {caller.display_name} and {opponent.display_name}",
                ephemeral=True,
            )

        await interaction.response.defer(ephemeral=False, thinking=True)

        if outcome.value == "win":
            winner, loser = caller, opponent
        else:
            winner, loser = opponent, caller

        # Atomically update stats + log match
        loser_after, winner_after = db_stats_record_loss(
            self.state,
            loser_id=loser.id,
            winner_id=winner.id,
            set_id=CURRENT_ACTIVE_SET,
        )

        moved_points = 0
        team_message = None
        teams = interaction.client.get_cog("Teams")
        if interaction.guild and teams and hasattr(teams, "apply_duel_result"):
            try:
                moved_points, info = await teams.apply_duel_result(
                    interaction.guild,
                    winner=winner,
                    loser=loser,
                    winner_stats=winner_after,
                    loser_stats=loser_after,
                )
                winner_team = info.get("winner_team", "Unknown team")
                loser_team = info.get("loser_team", "Unknown team")
                winner_total = info.get("winner_total")
                team_message = (
                    f"{winner.display_name} claimed **{moved_points:,}** units of territory "
                    f"for the {winner_team} team."
                )
                if winner_total is not None:
                    team_message += f" Territory controlled: **{int(winner_total):,}**."
            except Exception as exc:
                print("[stats] failed to apply battleground points:", exc)
        if team_message is None:
            team_message = "Team territory could not be updated for this match."

        # Optional quest ticks using your QuestManager wrappers/IDs
        quests = interaction.client.get_cog("Quests")
        try:
            if quests and getattr(quests, "qm", None):
                await quests.qm.increment(loser.id, "matches_played", 1)
                await quests.qm.increment(winner.id, "matches_played", 2)
        except Exception as e:
            print("[stats] quest tick error:", e)

        embed = discord.Embed(
            title="Match Result Recorded",
            description=(
                f"**{winner.display_name}** defeated **{loser.display_name}**.\n"
                f"{team_message}"
            ),
            color=0xCC3333,
        )

        await interaction.followup.send(embed=embed)

        # Remove the user(s) from the queue if paired
        queue = interaction.client.get_cog("DuelQueue")
        try:
            if queue and hasattr(queue, "clear_pairing"):
                await queue.clear_pairing(caller.id, opponent.id)
        except Exception as e:
            print("[stats] failed to clear duel pairing:", e)


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

        team_lines: list[str] = []
        guild = interaction.guild
        if guild and isinstance(target, discord.Member):
            team_points = db_team_battleground_user_points_for_user(
                self.state, guild.id, CURRENT_ACTIVE_SET, target.id
            )
            team_roles = [role.name for role in target.roles if role.name in TEAM_ROLE_NAMES]
            for role_name in sorted(team_roles, key=str.lower):
                points = int(team_points.get(role_name, {}).get("earned_points", 0))
                team_lines.append(f"Team {role_name}: territory claimed: **{points:,}**")

        if team_lines:
            value = "\n".join(team_lines)
        else:
            value = "No team roles assigned."

        embed.add_field(name="Team Territory", value=value, inline=False)

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
