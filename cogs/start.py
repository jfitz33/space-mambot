# cogs/start.py
import os, discord, asyncio
from collections import Counter
from discord.ext import commands
from discord import app_commands

from core.state import AppState
from core.starters import load_starters_from_csv, grant_starter_to_user
from core.packs import open_pack_from_csv, persist_pulls_to_db, RARITY_ORDER  # reuse your pack code  :contentReference[oaicite:4]{index=4}
from core.views import _pack_embed_for_cards  
from core.db import db_wallet_add, db_add_cards
from core.images import ensure_rarity_emojis
from core.wallet_api import get_mambucks, credit_mambucks, get_shards, add_shards
from core.constants import PACKS_BY_SET
from core.currency import shard_set_name
# Guild scoping (same as your other cogs)  :contentReference[oaicite:5]{index=5}
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

STARTER_ROLE_NAME = "starter"

# Map starter deck name → which pack to auto-open. If empty, we fall back to using the deck name as pack name.
STARTER_TO_PACK = {
    "Cult of the Mambo": "Storm of the Abyss",
    "Hellfire Heretics": "Blazing Genesis",
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

        # Acknowledge component interaction to avoid it expiring
        await interaction.response.defer(ephemeral=True, thinking=False)

        # Immediately disable the dropdown so the user cannot make additional selections
        # while we process their starter choice.
        for child in self.children:
            child.disabled = True
        try:
            await interaction.followup.edit_message(message_id=interaction.message.id, view=self)
        except Exception:
            pass

        if deck_name.lower().__contains__("mambo"):
            await interaction.channel.send(f"Sploosh! {self.member.mention} selected the **{deck_name}** starter deck!")
        elif deck_name.lower().__contains__("fire"):
            await interaction.channel.send(f"Thats Hot! {self.member.mention} selected the **{deck_name}** starter deck!")
        else:
            await interaction.channel.send(f"{self.member.mention} selected the **{deck_name}** starter deck!")

        # 2) Open 3 packs
        pack_name = STARTER_TO_PACK.get(deck_name, deck_name)

        # How many packs to open as part of /start
        START_PACKS = 3

        # (A) Open packs in memory
        per_pack: list[list[dict]] = []
        for _ in range(START_PACKS):
            per_pack.append(open_pack_from_csv(self.state, pack_name, 1))  # pack_name = the pack you want for /start

        # (B) Persist pulled cards to the player's collection
        flat = [c for cards in per_pack for c in cards]
        db_add_cards(self.state, interaction.user.id, flat, pack_name)

        # (C) Try to DM one message per pack
        dm_sent = False
        try:
            dm = await interaction.user.create_dm()
            for i, cards in enumerate(per_pack, start=1):
                embed, f = _pack_embed_for_cards(interaction.client, pack_name, cards, i, START_PACKS)
                if f:
                    await dm.send(embed=embed, file=f)
                else:
                    await dm.send(embed=embed)
                if START_PACKS > 5:
                    await asyncio.sleep(0.2)  # gentle rate limiting safety
            dm_sent = True
        except Exception:
            dm_sent = False

        # (D) Post a succinct summary; if DMs failed, fall back with embeds in-channel
        summary = (
            f"{interaction.user.mention} opened **{START_PACKS}** "
            f"pack{'s' if START_PACKS != 1 else ''} of **{pack_name}**."
            f"{' Results sent via DM.' if dm_sent else ' I couldn’t DM you; posting results here.'}"
        )
        # Update packs opened counter for quests
        quests_cog = interaction.client.get_cog("Quests")  # same as self.bot
        if quests_cog:
            await quests_cog.tick_pack_open(user_id=interaction.user.id, amount=START_PACKS)

        await interaction.channel.send(summary)

        if not dm_sent:
            for i, cards in enumerate(per_pack, start=1):
                embed, f = _pack_embed_for_cards(interaction.client, pack_name, cards, i, START_PACKS)
            if f:
                await interaction.channel.send(embed=embed, file=f)
            else:
                await interaction.channel.send(embed=embed)
                if START_PACKS > 5:
                    await asyncio.sleep(0.2)

        # 4) Assign the user's starting currency
        cur_mb = get_mambucks(self.state, self.member.id)
        delta_mb = 100 - cur_mb
        if delta_mb != 0:
            credit_mambucks(self.state, self.member.id, delta_mb)

        # --- Set shards to exactly 300 for each known set ---
        set_ids = sorted(PACKS_BY_SET.keys()) or [1]  # default to Set 1 if mapping empty
        for sid in set_ids:
            cur_sh = get_shards(self.state, self.member.id, sid)
            delta_sh = 300 - cur_sh
            if delta_sh != 0:
                add_shards(self.state, self.member.id, sid, delta_sh)

        # 5) Assign the 'starter' role only after success
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
        
        # 6) Delete the original message upon completion
        try:
            await interaction.delete_original_response()
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
