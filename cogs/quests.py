import os, discord, math
from typing import List
from discord import app_commands
from discord.ext import commands

from core.quests.engine import QuestManager
from core.quests.schema import db_reset_all_user_quests   

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

    async def cog_load(self):
        await self.qm.load_defs()

    # --- ADMIN: reset all quest progress for a user ---
    @app_commands.command(name="quest_reset", description="(Admin) Reset ALL quest progress for a user.")
    @app_commands.guilds(GUILD)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(user="User whose quest progress you want to wipe")
    async def quest_reset(self, interaction: discord.Interaction, user: discord.User):
        await interaction.response.defer(ephemeral=True)
        deleted = await db_reset_all_user_quests(self.bot.state, user.id)
        await interaction.edit_original_response(
            content=f"🧹 Reset quests for {user.mention}. Removed **{deleted}** progress row(s)."
        )

    # Optional: cleaner error if non-admins try to use it
    @quest_reset.error
    async def quest_reset_error(self, interaction: discord.Interaction, error: Exception):
        if isinstance(error, app_commands.errors.MissingPermissions):
            if interaction.response.is_done():
                await interaction.followup.send("You need **Administrator** permissions to run this.", ephemeral=True)
            else:
                await interaction.response.send_message("You need **Administrator** permissions to run this.", ephemeral=True)
        else:
            # fall back to a generic error
            msg = f"Error: {error.__class__.__name__}: {error}"
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)

    @app_commands.command(name="quests", description="View your quests and claim rewards.")
    @app_commands.guilds(GUILD)
    async def quests(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        viewdata = await self.qm.get_user_view(interaction.user.id)
        if not viewdata:
            await interaction.edit_original_response(content="No quests available right now.")
            return

        embed = discord.Embed(title="📜 Your Quests")
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
                    # optional: restrict to the original requester
                    # if inter.user.id != interaction.user.id:
                    #     await inter.response.send_message("This panel isn’t for you.", ephemeral=True)
                    #     return
                    ok, msg = await self.outer.qm.claim(inter.user.id, qid)
                    await inter.response.send_message(msg, ephemeral=True)
                    try:
                        await inter.message.delete()
                    except Exception:
                        pass

                btn.callback = _on_click
                return btn

        view = ClaimView(self, viewdata)
        await interaction.edit_original_response(embed=embed, view=(view if view.children else None))

    # ---- /quest_claim with autocomplete of claimable quests ----
    @app_commands.command(name="quest_claim", description="Claim a quest reward by name.")
    @app_commands.guilds(GUILD)
    @app_commands.describe(quest="Start typing the quest name (suggests claimable ones).")
    async def quest_claim(self, interaction: discord.Interaction, quest: str):
        await interaction.response.defer(ephemeral=True)

        viewdata = await self.qm.get_user_view(interaction.user.id)
        # Next-claimable only
        claimables = {r["quest"].title.lower(): r["quest"].quest_id
                    for r in viewdata if r["completed"] and not r["claimed"]}

        # Resolve by exact id or fuzzy title match
        qid = None
        # exact id match?
        for r in viewdata:
            if quest == r["quest"].quest_id:
                qid = quest
                break
        if not qid:
            # title contains match
            qid = next((qid for title, qid in claimables.items() if quest.lower() in title), None)

        if not qid:
            if not claimables:
                await interaction.edit_original_response(content="You have no claimable quests right now.")
            else:
                s = ", ".join([t.title() for t in claimables.keys()])
                await interaction.edit_original_response(content=f"Could not find that quest. Try one of: {s}")
            return

        ok, msg = await self.qm.claim(interaction.user.id, qid)
        await interaction.edit_original_response(content=msg)

    # ✅ Proper autocomplete attachment (must be async and return Choices)
    @quest_claim.autocomplete("quest")
    async def quest_claim_autocomplete(self, interaction: discord.Interaction, current: str):
        viewdata = await self.qm.get_user_view(interaction.user.id)
        names = [r["quest"].title for r in viewdata if r["completed"] and not r["claimed"]]
        current_l = (current or "").lower()
        filtered = [n for n in names if current_l in n.lower()]
        return [app_commands.Choice(name=n, value=n) for n in filtered[:20]]


    # Helper you can call from other cogs when packs are opened:
    async def tick_pack_open(self, user_id: int, amount: int = 1):
        # Milestone quest id we seeded above:
        try:
            await self.qm.increment(user_id, "open_packs", amount)
        except Exception as e:
            print("[quests] tick_pack_open error:", e)

async def setup(bot: commands.Bot):
    await bot.add_cog(Quests(bot))
