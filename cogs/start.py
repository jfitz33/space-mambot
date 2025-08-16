# cogs/start.py
import os, discord
from collections import Counter
from discord.ext import commands
from discord import app_commands

from core.state import AppState
from core.starters import load_starters_from_csv, grant_starter_to_user
from core.packs import open_pack_from_csv, persist_pulls_to_db, RARITY_ORDER  # reuse your pack code  :contentReference[oaicite:4]{index=4}
from core.views import PackResultsPaginator  # your paginator view

# Guild scoping (same as your other cogs)  :contentReference[oaicite:5]{index=5}
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

STARTER_ROLE_NAME = "starter"

# Map starter deck name → which pack to auto-open. If empty, we fall back to using the deck name as pack name.
STARTER_TO_PACK = {
    "start_Water": "Water",
    "start_Fire": "Fire"
}

async def _resolve_member(interaction: discord.Interaction) -> discord.Member | None:
    # Must be in a server
    if not interaction.guild:
        return None
    # Try cache first
    m = interaction.guild.get_member(interaction.user.id)
    if m:
        return m
    # Fallback to API (works without privileged intents)
    try:
        return await interaction.guild.fetch_member(interaction.user.id)
    except discord.NotFound:
        return None

# cogs/start.py (replace the whole class)

import discord
from discord.ui import View
from core.starters import grant_starter_to_user
from core.packs import open_pack_from_csv
from core.views import PackResultsPaginator

STARTER_ROLE_NAME = "starter"  # keep your constant
# STARTER_TO_PACK should already be defined at module top

class StarterDeckSelectView(View):
    def __init__(self, state, member: discord.Member, timeout: float = 180):
        super().__init__(timeout=timeout)
        self.state = state
        self.member = member
        self._options = []
        for deck in sorted((state.starters_index or {}).keys(), key=str.lower):
            self._options.append(discord.SelectOption(label=deck[:100], value=deck[:100], description="Starter deck"))
        if not self._options:
            self._options = [discord.SelectOption(label="No starters found", value="__none__", description="Load CSVs")]

    @discord.ui.select(
        placeholder="Choose your starter deck…",
        min_values=1,
        max_values=1,
        options=[]  # will be filled in setup hook below
    )
    async def deck_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        # Ensure only the intended user can use this
        if interaction.user.id != self.member.id:
            await interaction.response.send_message("Only the recipient can choose this.", ephemeral=True)
            return

        deck_name = select.values[0] if select.values else "__none__"
        if deck_name == "__none__":
            await interaction.response.send_message("No starter decks are loaded.", ephemeral=True)
            return

        # 1) Grant the starter deck (DB upsert). If nothing granted, bail.
        granted = grant_starter_to_user(self.state, self.member.id, deck_name)
        if not granted:
            await interaction.response.send_message("That starter deck appears to be empty.", ephemeral=True)
            return

        # 2) Which pack to open?
        pack_name = STARTER_TO_PACK.get(deck_name, deck_name)

        # 3) Open 3 packs and show with your paginator
        try:
            per_pack_pulls: list[list[dict]] = []
            for _ in range(3):
                pulls = open_pack_from_csv(self.state, pack_name, amount=1)
                persist_pulls_to_db(self.state, self.member.id, pack_name, pulls)  # <-- persist to DB
                per_pack_pulls.append(pulls)

            # Flavor line for Water
            if deck_name.lower().__contains__("water"):
                await interaction.channel.send(f"Sploosh! {self.member.mention} selected the **{deck_name}** starter deck!")
            else:
                await interaction.channel.send(f"{self.member.mention} selected the **{deck_name}** starter deck!")

            paginator = PackResultsPaginator(self.member, pack_name, per_pack_pulls=per_pack_pulls)
            await interaction.channel.send(embed=paginator._embed_for_index(), view=paginator)

        except Exception as e:
            # Do not assign role, let user retry after you fix
            await interaction.response.send_message(
                f"Starter granted, but opening packs failed: `{e}`. Please try again after the issue is fixed.",
                ephemeral=True
            )
            return

        # 4) Assign the 'starter' role only after success
        try:
            role = discord.utils.get(interaction.guild.roles, name=STARTER_ROLE_NAME)
            if role is None:
                role = await interaction.guild.create_role(name=STARTER_ROLE_NAME, reason="Starter gate")
            await self.member.add_roles(role, reason="Completed starter flow")
        except discord.Forbidden:
            await interaction.followup.send(
                "Packs opened, but I couldn’t assign the **starter** role. "
                "Please grant me **Manage Roles** and ensure my top role is above `starter`.",
                ephemeral=True
            )

        # 5) Disable dropdown and update the original ephemeral message
        for child in self.children:
            child.disabled = True
        if not interaction.response.is_done():
            await interaction.response.edit_message(view=self)
        else:
            try:
                await interaction.followup.edit_message(message_id=interaction.message.id, view=self)
            except Exception:
                pass

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Optional: block others from interacting with the view entirely
        return interaction.user.id == self.member.id

    async def setup(self):
        """Call this immediately after constructing the view to populate options."""
        # The decorator created a Select as the first item in self.children
        # We now inject the options gathered in __init__
        for child in self.children:
            if isinstance(child, discord.ui.Select):
                child.options = self._options
                break


class Start(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: AppState = self.bot.state

    @app_commands.command(name="start", description="Claim your starter deck and 3 matching packs")
    @app_commands.guilds(GUILD)
    async def start(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("Use this in a server.", ephemeral=True)
            return

        member = await _resolve_member(interaction)
        if not member:
            await interaction.response.send_message(
                "Could not resolve your member (I may not see members in cache). Try again, or check my role permissions.",
                ephemeral=True
            )
            return

        # Ensure packs and starters are loaded
        # (Your packs loader already exists; starter loader mirrors that approach.)
        load_starters_from_csv(self.state)

        if not getattr(self.state, "starters_index", None):
            await interaction.response.send_message("No starter decks found. Add CSVs to `starters_csv/` and reload.", ephemeral=True)
            return

        # Starter role gate
        role = discord.utils.get(interaction.guild.roles, name="starter")
        if role and role in member.roles:
            await interaction.response.send_message(f"{member.mention} already has their starter cards.", ephemeral=True)
            return
        if not role:
            try:
                role = await interaction.guild.create_role(name=STARTER_ROLE_NAME, reason="Starter gate")
            except discord.Forbidden:
                await interaction.response.send_message("I need **Manage Roles** to create/assign the starter role.", ephemeral=True)
                return
        try:
            await member.add_roles(role, reason="Claimed starter pack")
        except discord.Forbidden:
            await interaction.response.send_message("I need **Manage Roles** to assign the starter role.", ephemeral=True)
            return

        # Prompt with dropdown (ephemeral)
        view = StarterDeckSelectView(self.state, member)
        await view.setup()
        await interaction.response.send_message(
            content=f"{member.mention}, choose your starter deck:",
            view=view,
            ephemeral=True
        )

async def setup(bot: commands.Bot):
    await bot.add_cog(Start(bot))
