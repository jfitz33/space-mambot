# cogs/start.py
import os, discord, asyncio
from collections import Counter
from discord.ext import commands
from discord import app_commands

from core.state import AppState
from core.starters import load_starters_from_csv, grant_starter_to_user
from core.packs import (
    open_pack_from_csv,
    open_pack_with_guaranteed_top_from_csv,
    persist_pulls_to_db,
    RARITY_ORDER,
)
from core.views import _pack_embed_for_cards  
from core.db import db_wallet_add, db_add_cards
from core.images import ensure_rarity_emojis
from core.wallet_api import get_mambucks, credit_mambucks, get_shards, add_shards
from core.constants import PACKS_BY_SET, TEAM_ROLE_MAPPING, TEAM_ROLE_NAMES
from core.currency import shard_set_name
# Guild scoping (same as your other cogs)  :contentReference[oaicite:5]{index=5}
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

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

class StarterDeckSelectView(View):
    def __init__(self, state, member: discord.Member, timeout: float = 180, on_complete=None):
        super().__init__(timeout=timeout)
        self.state = state
        self.member = member
        self._on_complete = on_complete
        self._completed = False
        self._options = []
        for deck in sorted((state.starters_index or {}).keys(), key=str.lower):
            self._options.append(discord.SelectOption(label=deck[:100], value=deck[:100], description="Starter deck"))
        if not self._options:
            self._options = [discord.SelectOption(label="No starters found", value="__none__", description="Load CSVs")]

    def _complete(self):
        if self._completed:
            return
        self._completed = True
        if self._on_complete:
            try:
                self._on_complete()
            except Exception:
                pass

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
            self._complete()
            return

        try:
            # 1) Grant the starter deck (DB upsert). If nothing granted, bail.
            granted = grant_starter_to_user(self.state, self.member.id, deck_name)
            if not granted:
                await interaction.response.send_message("That starter deck appears to be empty.", ephemeral=True)
                self._complete()
                return

            # Acknowledge component interaction to avoid it expiring
            await interaction.response.defer(ephemeral=True, thinking=False)

            # Assign the appropriate team role immediately to prevent users from
            # repeatedly invoking /start before role gating kicks in.
            team_role_name = TEAM_ROLE_MAPPING.get(deck_name)
            if team_role_name:
                try:
                    role = discord.utils.get(interaction.guild.roles, name=team_role_name)
                    if role is None:
                        role = await interaction.guild.create_role(name=team_role_name, reason="Starter gate")
                    await self.member.add_roles(role, reason="Claimed starter deck")
                except discord.Forbidden:
                    await interaction.followup.send(
                        "I couldn't assign your team role due to permissions. "
                        "Please grant me **Manage Roles** and ensure my top role is above the team roles, then try again.",
                        ephemeral=True
                    )
                    self._complete()
                    return
                except Exception:
                    await interaction.followup.send(
                        "Something went wrong while assigning your team role. Please try again shortly.",
                        ephemeral=True
                    )
                    self._complete()
                    return

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
                await interaction.channel.send(f"That's Hot! {self.member.mention} selected the **{deck_name}** starter deck!")
            else:
                await interaction.channel.send(f"{self.member.mention} selected the **{deck_name}** starter deck!")

            # 2) Open starter packs with guaranteed rarity progression
            pack_name = STARTER_TO_PACK.get(deck_name, deck_name)

            # How many packs to open as part of /start
            START_PACKS = 12
            guaranteed_tops = ["super"] * 8 + ["ultra"] * 3 + ["secret"]

            # (A) Open packs in memory
            per_pack: list[list[dict]] = []
            for top_rarity in guaranteed_tops:
                try:
                    cards = open_pack_with_guaranteed_top_from_csv(self.state, pack_name, top_rarity)
                except ValueError:
                    cards = open_pack_from_csv(self.state, pack_name, 1)
                per_pack.append(cards)  # pack_name = the pack you want for /start

            # (B) Persist pulled cards to the player's collection
            flat = [c for cards in per_pack for c in cards]
            db_add_cards(self.state, interaction.user.id, flat, pack_name)

            # (C) Try to DM one message per pack
            dm_sent = False
            try:
                dm = await interaction.user.create_dm()
                for i, cards in enumerate(per_pack, start=1):
                    content, embeds, files = _pack_embed_for_cards(interaction.client, pack_name, cards, i, START_PACKS)
                    send_kwargs: dict = {"embeds": embeds}
                    if content:
                        send_kwargs["content"] = content
                    if files:
                        send_kwargs["files"] = files
                    await dm.send(**send_kwargs)
                    if START_PACKS > 5:
                        await asyncio.sleep(0.2)  # gentle rate limiting safety
                dm_sent = True
            except Exception:
                dm_sent = False

            # (D) Post a succinct summary; if DMs failed, fall back with embeds in-channel
            summary = (
                f"Welcome to the **{TEAM_ROLE_MAPPING.get(deck_name)}** team {interaction.user.mention}!"
                f" I sent you **{START_PACKS}** "
                f"pack{'s' if START_PACKS != 1 else ''} of **{pack_name}** to get started!"
                f"{' Results sent via DM.' if dm_sent else ' I couldn’t DM you; posting results here.'}"
                f" You can view your collection with the /collection command, or use the /collection_export command to get a csv version "
                f"to upload to ygoprodeck. Happy dueling!"
            )
            # Update packs opened counter for quests
            quests_cog = interaction.client.get_cog("Quests")  # same as self.bot
            if quests_cog:
                await quests_cog.tick_pack_open(user_id=interaction.user.id, amount=START_PACKS)

            await interaction.channel.send(summary)

            if not dm_sent:
                for i, cards in enumerate(per_pack, start=1):
                    content, embeds, files = _pack_embed_for_cards(interaction.client, pack_name, cards, i, START_PACKS)
                    send_kwargs: dict = {"embeds": embeds}
                    if content:
                        send_kwargs["content"] = content
                    if files:
                        send_kwargs["files"] = files
                    await dm.send(**send_kwargs)
                    if START_PACKS > 5:
                        await asyncio.sleep(0.2)

            # 5) Delete the original message upon completion
            try:
                await interaction.delete_original_response()
            except Exception:
                pass
        finally:
            self._complete()

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        self._complete()

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
        self._active_starters: set[int] = set()

    @app_commands.command(name="start", description="Claim your starter deck and receive your matching starter packs")
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

        # Team role gate
        if any(role.name in TEAM_ROLE_NAMES for role in member.roles):
            await interaction.response.send_message(f"{member.mention} already has their starter cards.", ephemeral=True)
            return

        # Prevent concurrent /start flows for the same user so the starter can only be claimed once
        if member.id in self._active_starters:
            await interaction.response.send_message(
                "You already have an active starter selection in progress. Please finish that one first.",
                ephemeral=True,
            )
            return
        self._active_starters.add(member.id)

        # Prompt with dropdown (ephemeral)
        view = StarterDeckSelectView(self.state, member, on_complete=lambda: self._active_starters.discard(member.id))
        await view.setup()
        try:
            await interaction.response.send_message(
                content=f"{member.mention}, choose your starter deck:",
                view=view,
                ephemeral=True
            )
        except Exception:
            self._active_starters.discard(member.id)
            raise

async def setup(bot: commands.Bot):
    await bot.add_cog(Start(bot))
