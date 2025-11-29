# cogs/gamba_chips.py
import os, asyncio, discord
from discord.ext import commands
from discord import app_commands
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from core.db import (
    db_init_wheel_tokens,
    db_wheel_tokens_grant_daily,
    db_wheel_tokens_add,
    db_wheel_tokens_get,
)

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None
STARTER_ROLE_NAME = os.getenv("STARTER_ROLE_NAME", "starter")  # rename any time

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

class GambaChips(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._task: asyncio.Task | None = None

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
        day_key = day_key or _today_key_et()
        awarded = 0
        for guild in self.bot.guilds:
            role = discord.utils.get(guild.roles, name=STARTER_ROLE_NAME)
            if not role:
                continue
            # NOTE: requires Server Members Intent to have role.members populated
            for m in role.members:
                _, did = db_wheel_tokens_grant_daily(self.bot.state, m.id, day_key)
                if did:
                    awarded += 1
        print(f"[gamba] daily grant {day_key}: granted to {awarded} user(s).")

    async def run_midnight_grant(self, *, day_key: str | None = None):
        """Run the midnight grant once, optionally using a custom ET day key."""
        try:
            await self._grant_once(day_key=day_key)
        except Exception as e:
            print(f"[gamba] manual grant error: {e}")

    async def _grant_loop(self):
        # On startup, attempt a grant in case we restarted after midnight
        try:
            await self._grant_once()
        except Exception as e:
            print(f"[gamba] initial grant error: {e}")
        # Then wait until the next ET midnight each time
        while True:
            try:
                await asyncio.sleep(_seconds_until_next_et_midnight())
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

async def setup(bot: commands.Bot):
    await bot.add_cog(GambaChips(bot))
