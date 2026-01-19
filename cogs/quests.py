import os, discord, math, asyncio
from datetime import datetime, date
from typing import List
from discord import app_commands
from discord.ext import commands

from core.quests.engine import QuestManager
from core.quests.timekeys import now_et, rollover_date
from core.daily_rollover import seconds_until_next_rollover, rollover_day_key
from core.constants import TEAM_ROLE_NAMES
from core.constants import TEAM_ROLE_NAMES

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

def fmt_bar(progress: int, target: int, width: int = 12) -> str:
    done = min(target, progress)
    fill = math.floor(width * done / max(1, target))
    return "[" + "█"*fill + "—"*(width-fill) + f"] {done}/{target}"

class Quests(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.qm = QuestManager(bot.state)
        self._rollover_task: asyncio.Task | None = None

    def _starter_member_ids(self) -> set[int]:
        ids: set[int] = set()
        for guild in self.bot.guilds:
            for role_name in TEAM_ROLE_NAMES:
                role = discord.utils.get(guild.roles, name=role_name)
                if role:
                    ids.update(m.id for m in role.members)
        return ids

    async def cog_load(self):
        await self.bot.wait_until_ready()
        await self.qm.load_defs()
        # Ensure rollover-style daily quests have their slots advanced through
        # today in case the bot was restarted and missed the regular scheduler.
        await self._prepare_rollover_for_date(rollover_date())

        self._rollover_task = asyncio.create_task(
            self._rollover_loop(), name="quest-rollovers"
        )

    async def cog_unload(self):
        if self._rollover_task:
            self._rollover_task.cancel()
            try:
                await self._rollover_task
            except Exception:
                pass

    async def _prepare_rollover_for_date(self, target_date: date):
        starter_ids = self._starter_member_ids()
        await self.qm.fast_forward_daily_rollovers(
            target_date, include_user_ids=starter_ids
        )

    async def _rollover_loop(self):
        while True:
            try:
                await asyncio.sleep(seconds_until_next_rollover())
                # Use the rollover day key to align with other daily systems.
                day_key = rollover_day_key()
                try:
                    target_date = datetime.strptime(day_key, "%Y%m%d").date()
                except Exception:
                    target_date = rollover_date()
                await self._prepare_rollover_for_date(target_date)
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(5)

    @app_commands.command(name="daily", description="View your daily duel progress and claim reward(s)")
    @app_commands.guilds(GUILD)
    async def quests(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        viewdata = await self.qm.get_user_view(interaction.user.id)
        if not viewdata:
            await interaction.edit_original_response(content="Daily duel unavailable right now.")
            return

        embed = discord.Embed(title="⚔️ Daily Duel")
        lines: List[str] = []
        for row in viewdata:
            q = row["quest"]
            target = row.get("target", q.target_count)
            show_bar = not (row.get("claimed") and not row.get("rollover_pending"))
            bar = fmt_bar(row["progress"], target) if show_bar else ""
            if row.get("milestone_mode", False):
                if row["claimed"] and row["completed"]:
                    state = "✅ All rewards claimed"
                elif row["completed"]:
                    state = f"🏁 Milestone ready: {target} (claim it!)"
                else:
                    remain = target - row["progress"]
                    state = f"• Next reward at {target} ({remain} more)"
            else:
                state = "✅ Claimed" if row["claimed"] else ("🏁 Completed" if row["completed"] else "• In progress")

            if row.get("rollover_pending", 0) > 1:
                ready = row.get("rollover_claimables", 0)
                pending = row.get("rollover_pending", 0)
                queued_days = max(pending - 1, 0)
                state += f" — {ready} claimable / {queued_days} day(s) queued"

            cat = q.category.capitalize()
            progress_line = f"{bar} — {state}" if show_bar else state

            lines.append(f"**{q.title}** · *{cat}*\n{q.description}\n{progress_line}\n")
        embed.description = "\n".join(lines)

        # Claim buttons for anything completed & unclaimed (next milestone)
        # inside your quests(...) command, replace ClaimView with this:
        class ClaimView(discord.ui.View):
            def __init__(self, outer: "Quests", rows):
                super().__init__(timeout=60)
                self.outer = outer
                # build a button for each completed & unclaimed quest/milestone
                for r in rows:
                    q = r["quest"]
                    completed = r["completed"]
                    unclaimed = not r["claimed"]
                    if completed and unclaimed:
                        self.add_item(self._make_claim_button(q.quest_id, q.title))

            def _make_claim_button(self, quest_id: str, title: str) -> discord.ui.Button:
                btn = discord.ui.Button(
                    label=f"Claim: {title}",
                    style=discord.ButtonStyle.success
                )

                # capture quest_id via default arg to avoid late-binding bug
                async def _on_click(inter: discord.Interaction, qid=quest_id):
                    await inter.response.defer(ephemeral=True)
                    for child in self.children:
                        child.disabled = True
                    try:
                        await inter.edit_original_response(view=self)
                    except discord.HTTPException:
                        pass
                    # optional: restrict to the original requester
                    # if inter.user.id != interaction.user.id:
                    #     await inter.response.send_message("This panel isn’t for you.", ephemeral=True)
                    #     return
                    ok, msg = await self.outer.qm.claim(
                        inter.user.id, qid, roles=[r.name for r in inter.user.roles]
                    )
                    try:
                        await inter.delete_original_response()
                    except discord.HTTPException:
                        # Fallback in case the message cannot be deleted (e.g., ephemeral deletion unsupported)
                        await inter.edit_original_response(content=None, embed=None, view=None)
                    await inter.followup.send(msg, ephemeral=True)

                btn.callback = _on_click
                return btn

        view = ClaimView(self, viewdata)
        await interaction.edit_original_response(embed=embed, view=(view if view.children else None))

async def setup(bot: commands.Bot):
    await bot.add_cog(Quests(bot))
