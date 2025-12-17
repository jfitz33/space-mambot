import os, discord, asyncio
from collections import Counter
from discord.ext import commands
from discord import app_commands
from discord.ui import View

from core.state import AppState
from core.starters import load_starters_from_csv, grant_starter_to_user
from core.packs import (
    open_pack_from_csv,
    open_pack_with_guaranteed_top_from_csv,
    persist_pulls_to_db,
    RARITY_ORDER,
)
from core.views import _pack_embed_for_cards
from core.db import (
    db_add_cards,
    db_daily_quest_pack_get_total,
    db_starter_claim_abort,
    db_starter_claim_begin,
    db_starter_claim_complete,
    db_starter_daily_get_total,
    db_wallet_add,
)
from core.images import ensure_rarity_emojis
from core.wallet_api import credit_mambucks, add_shards
from core.constants import TEAM_ROLE_MAPPING, TEAM_ROLE_NAMES, CURRENT_ACTIVE_SET
from core.currency import mambucks_label, shards_label
from core.wallet_api import get_mambucks, credit_mambucks, get_shards, add_shards
from core.constants import PACKS_BY_SET, TEAM_ROLE_MAPPING, TEAM_ROLE_NAMES
from core.currency import mambucks_label, shard_set_name
# Guild scoping (same as your other cogs)  :contentReference[oaicite:5]{index=5}
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

# Map starter deck name → which pack to auto-open. If empty, we fall back to using the deck name as pack name.
STARTER_TO_PACK = {
    "Cult of the Mambo": "Storm of the Abyss",
    "Hellfire Heretics": "Blazing Genesis",
}
WEEK1_QUEST_ID = "matches_played"
WEEK1_PACK_BY_ROLE = {
    "Water": "Storm of the Abyss",
    "Fire": "Blazing Genesis",
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

def _week1_pack_for_member(member: discord.Member) -> str:
    role_names = {r.name for r in member.roles}
    for role_name, pack in WEEK1_PACK_BY_ROLE.items():
        if role_name in role_names:
            return pack
    return WEEK1_PACK_BY_ROLE.get("Water", "Storm of the Abyss")

class StarterDeckSelectView(View):
    def __init__(self, state, member: discord.Member, timeout: float = 180, on_success=None, on_abort=None):
        super().__init__(timeout=timeout)
        self.state = state
        self.member = member
        self._on_success = on_success
        self._on_abort = on_abort
        self._completed = False
        self._options = []
        for deck in sorted((state.starters_index or {}).keys(), key=str.lower):
            self._options.append(discord.SelectOption(label=deck[:100], value=deck[:100], description="Starter deck"))
        if not self._options:
            self._options = [discord.SelectOption(label="No starters found", value="__none__", description="Load CSVs")]
    
    def _complete(self, success: bool):
        if self._completed:
            return
        self._completed = True
        try:
            if success:
                if self._on_success:
                    self._on_success()
            else:
                if self._on_abort:
                    self._on_abort()
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
            self._complete(False)
            return
        success = False
        try:
            # 1) Grant the starter deck (DB upsert). If nothing granted, bail.
            granted = grant_starter_to_user(self.state, self.member.id, deck_name)
            if not granted:
                await interaction.response.send_message("That starter deck appears to be empty.", ephemeral=True)
                self._complete(False)
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
                    self._complete(False)
                    return
                except Exception:
                    await interaction.followup.send(
                        "Something went wrong while assigning your team role. Please try again shortly.",
                        ephemeral=True
                    )
                    self._complete(False)
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

            catchup_lines: list[str] = []
            catchup_note = (
                "Because you joined after day 1 and missed out on daily rewards, "
                "I've awarded you these catch-up rewards!"
            )

            if CURRENT_ACTIVE_SET == 1:
                pack_catchup_total = db_daily_quest_pack_get_total(self.state, WEEK1_QUEST_ID)
                if pack_catchup_total > 0:
                    pack_ack = await self.state.shop.grant_pack(
                        self.member.id, _week1_pack_for_member(self.member), pack_catchup_total
                    )
                    catchup_lines.append(pack_ack)

            daily_total = db_starter_daily_get_total(self.state)
            if daily_total > 0:
                new_wallet = credit_mambucks(self.state, self.member.id, daily_total)
                catchup_lines.append(
                    f"Credited {mambucks_label(daily_total)} (new total: {mambucks_label(new_wallet)})."
                )
            
            if CURRENT_ACTIVE_SET in (2, 3):
                frostfire_total = add_shards(self.state, self.member.id, 1, 35000)
                catchup_lines.append(
                    f"Credited {shards_label(35000, 1)} (new total: {shards_label(frostfire_total, 1)})."
                )

            if CURRENT_ACTIVE_SET == 3:
                sandstorm_total = add_shards(self.state, self.member.id, 2, 35000)
                catchup_lines.append(
                    f"Credited {shards_label(35000, 2)} (new total: {shards_label(sandstorm_total, 2)})."
                )

            # (D) Post a succinct summary; if DMs failed, fall back with embeds in-channel
            summary = (
                f"Welcome to the **{TEAM_ROLE_MAPPING.get(deck_name)}** team {interaction.user.mention}!"
                f" I sent you **{START_PACKS}** "
                f"pack{'s' if START_PACKS != 1 else ''} of **{pack_name}** to get started!"
                f"{' Results sent via DM.' if dm_sent else ' I couldn’t DM you; posting results here.'}"
                f" You can view your collection with the /collection command, or use the /collection_export command to get a csv version "
                f"to upload to ygoprodeck. Happy dueling!"
            )

            if catchup_lines:
                summary += "\n\n" + catchup_note + "\n" + "\n".join(f"• {line}" for line in catchup_lines)

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
            success = True
        finally:
            self._complete(success)

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        self._complete(False)

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
        self._starter_lock = asyncio.Lock()

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

        # Cross-process starter guard (DB-backed)
        claim_status = db_starter_claim_begin(self.state, member.id)
        if claim_status == "complete":
            await interaction.response.send_message(
                f"{member.mention} already has their starter cards.",
                ephemeral=True,
            )
            return
        if claim_status == "in_progress":
            await interaction.response.send_message(
                "You already have an active starter selection in progress. Please finish that one first.",
                ephemeral=True,
            )
            return
        
        # Team role gate (also mark the claim complete for legacy users who already have the role)
        if any(role.name in TEAM_ROLE_NAMES for role in member.roles):
            db_starter_claim_complete(self.state, member.id)
            await interaction.response.send_message(
                f"{member.mention} already has their starter cards.",
                ephemeral=True,
            )
            return

        # Prompt with dropdown (ephemeral)
        view = StarterDeckSelectView(
            self.state,
            member,
            on_success=lambda: db_starter_claim_complete(self.state, member.id),
            on_abort=lambda: db_starter_claim_abort(self.state, member.id),
        )
        await view.setup()
        try:
            await interaction.response.send_message(
                content=f"{member.mention}, choose your starter deck:",
                view=view,
                ephemeral=True
            )
        except Exception:
            db_starter_claim_abort(self.state, member.id)
            raise

async def setup(bot: commands.Bot):
    await bot.add_cog(Start(bot))
