# cogs/daily_rewards.py
"""Daily mambuck rewards for starter (Fire/Water) roles."""

import asyncio
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import commands

from core.constants import TEAM_ROLE_NAMES
from core.currency import mambucks_label
from core.db import (
    db_init_starter_daily_rewards,
    db_starter_daily_get_amount,
    db_starter_daily_get_total,
    db_starter_daily_increment_total,
    db_starter_daily_reset_total,
    db_starter_daily_set_amount,
    db_starter_daily_try_grant,
)

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

ET = ZoneInfo("America/New_York")


def _today_key_et() -> str:
    return datetime.now(ET).strftime("%Y%m%d")


def _day_key_for(dt: datetime) -> str:
    return dt.astimezone(ET).strftime("%Y%m%d")


def _seconds_until_next_et_midnight() -> float:
    now = datetime.now(ET)
    tomorrow = (now + timedelta(days=1)).date()
    target = datetime.combine(tomorrow, datetime.min.time(), tzinfo=ET)
    return max(1.0, (target - now).total_seconds())


class DailyRewards(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._task: asyncio.Task | None = None
        self._last_grant_day_key: str | None = None

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
        day_key = day_key or _today_key_et()
        self._last_grant_day_key = day_key
        amount = db_starter_daily_get_amount(self.bot.state)
        if amount <= 0:
            print(
                f"[daily-rewards] skipped {day_key}: configured amount is {amount} mambucks."
            )
            return

        total_after, did_total = db_starter_daily_increment_total(
            self.bot.state, day_key, amount
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
            for member in members:
                _, did = db_starter_daily_try_grant(
                    self.bot.state, member.id, day_key, amount
                )
                if did:
                    awarded += 1

        print(
            f"[daily-rewards] {day_key}: granted {mambucks_label(amount)} to {awarded} user(s)."
        )
        if awarded == 0 and seen_members == 0:
            print(
                "[daily-rewards] warning: no Fire/Water members seen in cache; "
                "grants will be skipped until role membership is available."
            )
        if did_total:
            print(
                f"[daily-rewards] {day_key}: total mambucks awarded now {mambucks_label(total_after)}."
            )

    async def run_midnight_grant(self, *, day_key: str | None = None):
        """Run the midnight grant once, optionally using a custom ET day key."""
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
                await asyncio.sleep(_seconds_until_next_et_midnight())
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
        description="(Admin) View the running total of daily mambucks awarded",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def daily_mambucks_total(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)

        total = db_starter_daily_get_total(self.bot.state)
        await interaction.followup.send(
            f"Total daily mambucks awarded: **{mambucks_label(total)}**.",
            ephemeral=True,
        )

    @app_commands.command(
        name="daily_mambucks_reset_total",
        description="(Admin) Reset the running total of daily mambucks awarded",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def daily_mambucks_reset_total(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)

        db_starter_daily_reset_total(self.bot.state)
        await interaction.followup.send(
            "Daily mambucks awarded total has been reset to 0.",
            ephemeral=True,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(DailyRewards(bot))