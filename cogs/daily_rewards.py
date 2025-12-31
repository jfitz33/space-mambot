# cogs/daily_rewards.py
"""Daily mambuck rewards for starter (Fire/Water) roles."""

import asyncio
import os
from datetime import datetime, timedelta

import discord
from discord import app_commands
from discord.ext import commands

from core.constants import TEAM_ROLE_NAMES
from core.currency import mambucks_label
from core.daily_rollover import rollover_day_key, seconds_until_next_rollover
from core.db import (
    db_daily_quest_mambuck_reward_for_day,
    db_daily_quest_pack_get_total,
    db_daily_quest_pack_increment_total,
    db_daily_quest_pack_reward_for_day,
    db_daily_quest_pack_reset_total,
    db_init_starter_daily_rewards,
    db_starter_daily_get_amount,
    db_starter_daily_get_total,
    db_starter_daily_increment_total,
    db_starter_daily_reset_total,
    db_starter_daily_set_total,
    db_starter_daily_set_amount,
    db_starter_daily_try_grant,
)

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None
WEEK1_QUEST_ID = "matches_played"

def _today_key() -> str:
    return rollover_day_key()

def _day_key_for(dt: datetime) -> str:
    return rollover_day_key(dt)

def _quest_day_key_for_previous(day_key: str) -> str | None:
    try:
        prev_day = datetime.strptime(day_key, "%Y%m%d").date() - timedelta(days=1)
    except Exception:
        return None
    return f"D:{prev_day.isoformat()}"

class DailyRewards(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._task: asyncio.Task | None = None
        self._last_grant_day_key: str | None = None
        self._week1_enabled = os.getenv("DAILY_DUEL_WEEK1_ENABLE", "1") == "1"

    async def cog_load(self):
        db_init_starter_daily_rewards(self.bot.state)
        self._task = asyncio.create_task(
            self._grant_loop(), name="daily-mambucks"
        )

    async def cog_unload(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass

    async def _grant_once(self, *, day_key: str | None = None):
        day_key = day_key or _today_key()
        self._last_grant_day_key = day_key
        amount = db_starter_daily_get_amount(self.bot.state)
        prev_quest_day_key = _quest_day_key_for_previous(day_key)
        quest_bonus = db_daily_quest_mambuck_reward_for_day(
            self.bot.state, prev_quest_day_key
        ) if prev_quest_day_key else 0
        total_increment = amount + quest_bonus

        total_after, did_total = db_starter_daily_increment_total(
            self.bot.state, day_key, total_increment
        )

        pack_increment = 0
        pack_total_after = db_daily_quest_pack_get_total(
            self.bot.state, WEEK1_QUEST_ID
        )
        if self._week1_enabled and prev_quest_day_key:
            pack_increment = db_daily_quest_pack_reward_for_day(
                self.bot.state, prev_quest_day_key, WEEK1_QUEST_ID
            )
            pack_total_after, _ = db_daily_quest_pack_increment_total(
                self.bot.state, WEEK1_QUEST_ID, prev_quest_day_key, pack_increment
            )

        awarded = 0
        seen_members = 0
        for guild in self.bot.guilds:
            members = set()
            for role_name in TEAM_ROLE_NAMES:
                role = discord.utils.get(guild.roles, name=role_name)
                if role:
                    members.update(role.members)
            seen_members += len(members)
            if amount > 0:
                for member in members:
                    _, did = db_starter_daily_try_grant(
                        self.bot.state, member.id, day_key, amount
                    )
                    if did:
                        awarded += 1

        if amount > 0:
            print(
                f"[daily-rewards] {day_key}: granted {mambucks_label(amount)} to {awarded} user(s)."
            )
            if awarded == 0 and seen_members == 0:
                print(
                    "[daily-rewards] warning: no Fire/Water members seen in cache; "
                    "grants will be skipped until role membership is available."
                )
            else:
                print(
                    f"[daily-rewards] {day_key}: skipped member grants; configured amount is {amount} mambucks."
                )
        if did_total:
            quest_note = (
                f" (+{mambucks_label(quest_bonus)} from prior daily quests)"
                if quest_bonus > 0
                else ""
            )
            print(
                f"[daily-rewards] {day_key}: total mambucks awarded now {mambucks_label(total_after)}."
                f"[daily-rewards] {day_key}: total daily earnable now {mambucks_label(total_after)}{quest_note}."
            )
        
        if pack_increment > 0:
            print(
                f"[daily-rewards] {day_key}: +{pack_increment} pack(s) added to {WEEK1_QUEST_ID} total "
                f"(now {pack_total_after})."
            )

    async def run_midnight_grant(self, *, day_key: str | None = None):
        """Run the configured rollover grant once, optionally using a custom day key."""
        try:
            await self._grant_once(day_key=day_key)
        except Exception as e:
            print(f"[daily-rewards] manual grant error: {e}")

    async def _grant_loop(self):
        try:
            await self._grant_once()
        except Exception as e:
            print(f"[daily-rewards] initial grant error: {e}")
        while True:
            try:
                await asyncio.sleep(seconds_until_next_rollover())
                await self._grant_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[daily-rewards] daily grant loop error: {e}")
                await asyncio.sleep(10)

    # --- Admin: configure daily rewards ------------------------------------
    @app_commands.command(
        name="daily_mambucks",
        description="(Admin) Configure the daily mambuck grant for starter roles",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        amount="New daily amount to grant to Fire/Water starters. Omit to view current."
    )
    async def daily_mambucks(
        self, interaction: discord.Interaction, amount: int | None = None
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        current = db_starter_daily_get_amount(self.bot.state)
        if amount is None:
            await interaction.followup.send(
                f"Current daily reward: **{mambucks_label(current)}** to each starter.",
                ephemeral=True,
            )
            return

        if amount < 0:
            await interaction.followup.send(
                "Daily amount must be zero or greater.", ephemeral=True
            )
            return

        updated = db_starter_daily_set_amount(self.bot.state, amount)
        await interaction.followup.send(
            f"Daily starter reward updated: **{mambucks_label(current)}** → **{mambucks_label(updated)}**.",
            ephemeral=True,
        )

    # --- Admin: totals ------------------------------------------------------
    @app_commands.command(
        name="daily_mambucks_total",
        description="(Admin) View the running total of daily mambucks earnable per user",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def daily_mambucks_total(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)

        total = db_starter_daily_get_total(self.bot.state)
        await interaction.followup.send(
            f"Total daily mambucks earnable per user: **{mambucks_label(total)}**.",
            ephemeral=True,
        )

    @app_commands.command(
        name="daily_mambucks_reset_total",
        description="(Admin) Reset the running total of daily mambucks earnable per user",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def daily_mambucks_reset_total(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)

        db_starter_daily_reset_total(self.bot.state)
        await interaction.followup.send(
            "Daily mambucks earnable total has been reset to 0.",
            ephemeral=True,
        )

    @app_commands.command(
        name="daily_mambucks_set_total",
        description="(Admin) Set the running total of daily mambucks earnable per user",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(total="New running total of daily mambucks earnable per user")
    async def daily_mambucks_set_total(
        self, interaction: discord.Interaction, total: int
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        if total < 0:
            await interaction.followup.send(
                "Total must be zero or greater.", ephemeral=True
            )
            return

        current = db_starter_daily_get_total(self.bot.state)
        updated = db_starter_daily_set_total(self.bot.state, total)
        await interaction.followup.send(
            "Daily mambucks earnable total updated: "
            f"**{mambucks_label(current)}** → **{mambucks_label(updated)}**.",
            ephemeral=True,
        )

    @app_commands.command(
        name="daily_packs_total",
        description="(Admin) View the running total of daily quest packs earnable per user",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        quest_id="Quest ID to inspect (default: matches_played for week 1 daily duel)",
    )
    async def daily_packs_total(
        self, interaction: discord.Interaction, quest_id: str | None = None
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        qid = quest_id or WEEK1_QUEST_ID
        total = db_daily_quest_pack_get_total(self.bot.state, qid)
        await interaction.followup.send(
            f"Total daily quest packs earnable per user for `{qid}`: **{total}**.",
            ephemeral=True,
        )

    @app_commands.command(
        name="daily_packs_reset_total",
        description="(Admin) Reset the running total of daily quest packs earnable per user",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        quest_id="Quest ID to reset (default: matches_played for week 1 daily duel)",
    )
    async def daily_packs_reset_total(
        self, interaction: discord.Interaction, quest_id: str | None = None
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        qid = quest_id or WEEK1_QUEST_ID
        db_daily_quest_pack_reset_total(self.bot.state, qid)
        await interaction.followup.send(
            f"Daily quest pack total for `{qid}` has been reset to 0.",
            ephemeral=True,
        )

async def setup(bot: commands.Bot):
    await bot.add_cog(DailyRewards(bot))