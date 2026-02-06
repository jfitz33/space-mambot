# cogs/gamba_chips.py
import os, asyncio, discord
from discord.ext import commands
from discord import app_commands
from datetime import datetime

from core.constants import TEAM_ROLE_NAMES
from core.currency import shard_set_name
from core.daily_rollover import rollover_day_key, seconds_until_next_rollover
from core.db import (
    db_init_wheel_tokens,
    db_gamba_daily_increment_total,
    db_gamba_daily_get_total,
    db_gamba_daily_reset_total,
    db_gamba_daily_set_total,
    db_wheel_tokens_grant_daily,
    db_wheel_tokens_add,
    db_wheel_tokens_get,
    db_convert_all_wheel_tokens_to_shards,
)

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

def _today_key() -> str:
    return rollover_day_key()

def _day_key_for(dt: datetime) -> str:
    return rollover_day_key(dt)

class GambaChips(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._task: asyncio.Task | None = None
        self._last_grant_day_key: str | None = None
        self._week1_enabled = os.getenv("DAILY_DUEL_WEEK1_ENABLE", "1") == "1"

    async def cog_load(self):
        # Ensure table exists
        db_init_wheel_tokens(self.bot.state)
        # Start background loop
        self._task = asyncio.create_task(self._grant_loop(), name="gamba-daily-grants")

    async def cog_unload(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass

    async def _grant_once(self, *, day_key: str | None = None):
        if self._week1_enabled:
            print("[gamba] daily grants disabled during week 1 launch.")
            return
        day_key = day_key or _today_key()
        self._last_grant_day_key = day_key
        total_after, did_total = db_gamba_daily_increment_total(
            self.bot.state, day_key, 1
        )
        awarded = 0
        seen_members = 0
        for guild in self.bot.guilds:
            members = set()
            for role_name in TEAM_ROLE_NAMES:
                role = discord.utils.get(guild.roles, name=role_name)
                if role:
                    members.update(role.members)
            # NOTE: requires Server Members Intent to have role.members populated
            seen_members += len(members)
            for m in members:
                _, did = db_wheel_tokens_grant_daily(self.bot.state, m.id, day_key)
                if did:
                    awarded += 1
        print(f"[gamba] daily grant {day_key}: granted to {awarded} user(s).")
        if did_total:
            print(
                f"[gamba] {day_key}: total gamba chips earnable now {total_after}."
            )
        if awarded == 0 and seen_members == 0:
            print(
                "[gamba] warning: no Fire/Water members seen in cache; grants will be "
                "skipped until role membership is available."
            )

    async def run_midnight_grant(self, *, day_key: str | None = None):
        """Run the configured rollover grant once, optionally using a custom day key."""
        try:
            await self._grant_once(day_key=day_key)
        except Exception as e:
            print(f"[gamba] manual grant error: {e}")

    async def _grant_loop(self):
        # On startup, attempt a grant in case we restarted after a rollover
        try:
            await self._grant_once()
        except Exception as e:
            print(f"[gamba] initial grant error: {e}")
        # Then wait until the next configured rollover each time
        while True:
            try:
                await asyncio.sleep(seconds_until_next_rollover())
                await self._grant_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[gamba] daily grant loop error: {e}")
                await asyncio.sleep(10)

    # --- Admin: grant tokens manually ---------------------------------------
    @app_commands.command(name="gamba_grant", description="(Admin) Grant gamba chips to a user")
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(user="User to grant", amount="Number of chips to add (>=1)")
    async def gamba_grant(self, interaction: discord.Interaction,
                          user: discord.Member,
                          amount: app_commands.Range[int, 1, 1000]):
        await interaction.response.defer(ephemeral=True, thinking=True)
        before = db_wheel_tokens_get(self.bot.state, user.id)
        after = db_wheel_tokens_add(self.bot.state, user.id, int(amount))
        await interaction.followup.send(
            f"✅ Granted **{amount}** gamba chip(s) to {user.mention}.\n"
            f"Before: {before} → After: **{after}**",
            ephemeral=True
        )

    @app_commands.command(
        name="gamba_convert",
        description="(Admin) Convert all gamba chips into shards for a set.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        set_id="Shard set ID to receive conversions",
        chips_to_shards="Number of shards granted per gamba chip when converting balances",
    )
    async def gamba_convert(
        self,
        interaction: discord.Interaction,
        set_id: app_commands.Range[int, 1, None],
        chips_to_shards: app_commands.Range[int, 1, None],
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        conversion = db_convert_all_wheel_tokens_to_shards(
            self.bot.state, int(set_id), int(chips_to_shards)
        )
        converted_users = int(conversion.get("users", 0))
        total_tokens = int(conversion.get("total_tokens", 0))
        total_shards = int(conversion.get("total_shards", 0))
        shard_title = shard_set_name(int(set_id))

        if converted_users == 0 or total_tokens == 0:
            line = f"ℹ️ No gamba chip balances needed conversion. Shard type: **{shard_title}**."
        else:
            line = (
                f"✅ Converted **{total_tokens}** gamba chip(s) from **{converted_users}** user(s) "
                f"into **{total_shards} {shard_title}** at **1 → {int(chips_to_shards)}**."
            )

        await interaction.followup.send(line, ephemeral=True)

    # --- Admin: totals ------------------------------------------------------
    @app_commands.command(
        name="gamba_chips_total",
        description="(Admin) View the running total of gamba chips earnable per user",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def gamba_chips_total(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)

        total = db_gamba_daily_get_total(self.bot.state)
        await interaction.followup.send(
            f"Total gamba chips earnable per user: **{total}**.",
            ephemeral=True,
        )

    @app_commands.command(
        name="gamba_chips_reset_total",
        description="(Admin) Reset the running total of gamba chips earnable per user",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def gamba_chips_reset_total(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)

        db_gamba_daily_reset_total(self.bot.state)
        await interaction.followup.send(
            "Gamba chips earnable total has been reset to 0.",
            ephemeral=True,
        )

    @app_commands.command(
        name="gamba_chips_set_total",
        description="(Admin) Set the running total of gamba chips earnable per user",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(total="New running total of gamba chips earnable per user")
    async def gamba_chips_set_total(
        self, interaction: discord.Interaction, total: int
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        if total < 0:
            await interaction.followup.send(
                "Total must be zero or greater.", ephemeral=True
            )
            return

        current = db_gamba_daily_get_total(self.bot.state)
        updated = db_gamba_daily_set_total(self.bot.state, total)
        await interaction.followup.send(
            "Gamba chips earnable total updated: "
            f"**{current}** → **{updated}**.",
            ephemeral=True,
        )

async def setup(bot: commands.Bot):
    await bot.add_cog(GambaChips(bot))
