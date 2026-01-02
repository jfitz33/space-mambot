import os, discord, asyncio, tempfile
from pathlib import Path
from copy import deepcopy
from discord.ext import commands
from discord import app_commands
from discord.ui import View

from core.state import AppState
from core.starters import load_starters_from_csv, grant_starter_to_user
from core.packs import (
    open_pack_from_csv,
    open_pack_with_guaranteed_top_from_csv,
    RARITY_ORDER,
)
from core.views import _pack_embed_for_cards, _pack_image_path
from core.db import (
    db_add_cards,
    db_daily_quest_pack_get_total,
    db_starter_claim_abort,
    db_starter_claim_begin,
    db_starter_claim_complete,
    db_starter_claim_status,
    db_starter_daily_get_total,
)
from core.images import ensure_rarity_emojis
from core.wallet_api import credit_mambucks, add_shards
from core.constants import TEAM_ROLE_MAPPING, TEAM_ROLE_NAMES, CURRENT_ACTIVE_SET
from core.currency import mambucks_label, shards_label
from core.wallet_api import get_mambucks, credit_mambucks, get_shards, add_shards
from core.quests.timekeys import now_et
from PIL import Image
# Guild scoping (same as your other cogs)  :contentReference[oaicite:5]{index=5}
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

# Map starter deck name → which pack to auto-open. If empty, we fall back to using the deck name as pack name.
STARTER_TO_PACK = {
    "Starter Deck Water": "Storm of the Abyss",
    "Starter Deck Fire": "Blazing Genesis",
}
STARTER_DECK_URLS = {
    "Starter Deck Water": os.getenv("STARTER_DECK_WATER_URL", "https://www.duelingbook.com/deck?id=18723000"),
    "Starter Deck Fire": os.getenv("STARTER_DECK_FIRE_URL", "https://www.duelingbook.com/deck?id=18723001"),
}
TEAM_TO_STARTER = {team: deck for deck, team in TEAM_ROLE_MAPPING.items()}
SET1_TEAM_ORDER = ("Fire", "Water")
WEEK1_QUEST_ID = "matches_played"
WEEK1_PACK_BY_ROLE = {
    "Water": "Storm of the Abyss",
    "Fire": "Blazing Genesis",
}
STARTER_IMAGE_DIR = Path(__file__).resolve().parent.parent / "images" / "starter_images"

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

def _team_info() -> dict[str, dict[str, str]]:
    return {
        team: {
            "team": team,
            "deck_name": TEAM_TO_STARTER.get(team, ""),
            "pack_name": STARTER_TO_PACK.get(TEAM_TO_STARTER.get(team, ""), TEAM_TO_STARTER.get(team, "")),
            "deck_url": STARTER_DECK_URLS.get(TEAM_TO_STARTER.get(team, ""), ""),
        }
        for team in SET1_TEAM_ORDER
    }

def _starter_image_path(team_name: str, deck_name: str) -> Path | None:
    names = [team_name, deck_name]
    candidates: list[str] = []

    for name in filter(None, names):
        normalized = name.strip().replace("/", "_")
        slug = "".join(ch.lower() if ch.isalnum() else "_" for ch in normalized)
        slug = "_".join(filter(None, slug.split("_")))

        candidates.extend(
            [
                normalized,
                normalized.replace(" ", "_"),
                normalized.replace(" ", "-"),
                normalized.lower(),
                slug,
                f"starter_{slug}",
            ]
        )

    seen: set[str] = set()
    for stem in candidates:
        if stem in seen:
            continue
        seen.add(stem)
        candidate = STARTER_IMAGE_DIR / f"{stem}.png"
        if candidate.exists():
            return candidate
    return None


def _starter_pack_image_path(deck_name: str) -> Path | None:
    if not deck_name:
        return None

    normalized = deck_name.strip().replace("/", "_")
    slug = "".join(ch.lower() if ch.isalnum() else "_" for ch in normalized)
    slug = "_".join(filter(None, slug.split("_")))

    candidates = [
        f"{normalized}_plus_pack",
        f"{normalized.replace(' ', '_')}_plus_pack",
        f"{normalized.replace(' ', '-')}_plus_pack",
        f"{normalized.lower()}_plus_pack",
        f"{slug}_plus_pack",
    ]

    seen: set[str] = set()
    for stem in candidates:
        if stem in seen:
            continue
        seen.add(stem)
        candidate = STARTER_IMAGE_DIR / f"{stem}.png"
        if candidate.exists():
            return candidate
    return None


def _starter_pack_confirmation_embed(
    pack_name: str,
    description: str,
    deck_name: str,
    starter_image_path: Path | None = None,
) -> tuple[discord.Embed | None, list[discord.File]]:
    pack_image_path = _pack_image_path(pack_name)
    starter_path = starter_image_path if starter_image_path and starter_image_path.exists() else None
    combined_path = _starter_pack_image_path(deck_name)

    if not pack_image_path and not starter_path and not combined_path:
        return None, []

    embed = discord.Embed(title=pack_name, description=description)
    files: list[discord.File] = []

    if combined_path and combined_path.exists():
        combined_file = discord.File(str(combined_path), filename=combined_path.name)
        embed.set_image(url=f"attachment://{combined_path.name}")
        files.append(combined_file)
        return embed, files

    if pack_image_path:
        pack_file = discord.File(str(pack_image_path), filename=pack_image_path.name)
        embed.set_image(url=f"attachment://{pack_image_path.name}")
        files.append(pack_file)

    if starter_path:
        starter_file = discord.File(str(starter_path), filename=starter_path.name)
        embed.set_thumbnail(url=f"attachment://{starter_path.name}")
        files.append(starter_file)

    return embed, files

class StarterConfirmationView(View):
    def __init__(self, cog: "Start", member: discord.Member, team_name: str, deck_name: str, pack_name: str, *, timeout: float = 180):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.member = member
        self.team_name = team_name
        self.deck_name = deck_name
        self.pack_name = pack_name
        self._files: list[discord.File] = []
        self._confirmation_embed: discord.Embed | None = None

    def attach_files(self, files: list[discord.File]):
        self._files = files

    def set_confirmation_embed(self, embed: discord.Embed | None):
        self._confirmation_embed = deepcopy(embed) if embed else None

    async def _edit_interaction_message(
        self,
        interaction: discord.Interaction,
        *,
        content: str | None = None,
        embeds: list[discord.Embed] | None = None,
        attachments: list[discord.File] | None = None,
        view: discord.ui.View | None = None,
    ) -> None:
        """Safely edit the original interaction message.

        Mirrors the pack/box confirmation helpers to ensure we remove buttons
        even when the interaction has already been deferred.
        """

        kwargs = {"content": content, "view": view}
        if embeds is not None:
            kwargs["embeds"] = embeds
        if attachments is not None:
            kwargs["attachments"] = attachments

        try:
            if not interaction.response.is_done():
                await interaction.response.edit_message(**kwargs)
            else:
                await interaction.edit_original_response(**kwargs)
        except Exception:
            try:
                if interaction.message:
                    await interaction.message.edit(**kwargs)
            except Exception:
                pass

    async def _show_processing_state(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            try:
                await interaction.response.defer(ephemeral=True, thinking=False)
            except Exception:
                pass

        status_message = "Sending you your packs via DM"
        current_content = None
        current_embeds: list[discord.Embed] = []
        message = getattr(interaction, "message", None) or getattr(self, "message", None)

        if not message:
            try:
                message = await interaction.original_response()
            except Exception:
                message = None

        if message:
            current_embeds = [deepcopy(e) for e in (getattr(message, "embeds", []) or [])]
            current_content = getattr(message, "content", None)
            # Reuse the existing embed imagery without reattaching the file to
            # the message (which can otherwise display a duplicate pack image).
            if current_embeds and current_embeds[0].image:
                img = current_embeds[0].image
                img_url = getattr(img, "url", None) or getattr(img, "proxy_url", None)
                if img_url:
                    current_embeds[0].set_image(url=img_url)

        if not current_embeds and self._confirmation_embed:
            current_embeds = [deepcopy(self._confirmation_embed)]

        if current_embeds:
            footer_icon = current_embeds[0].footer.icon_url if current_embeds[0].footer else None
            existing_footer = (current_embeds[0].footer.text or "") if current_embeds[0].footer else ""
            footer_text = status_message if not existing_footer else f"{existing_footer} • {status_message}"
            current_embeds[0].set_footer(text=footer_text, icon_url=footer_icon)
        else:
            combined = "\n".join(filter(None, [current_content, status_message]))
            current_content = combined or None

        await self._edit_interaction_message(
            interaction,
            content=current_content,
            embeds=current_embeds or None,
            attachments=[],
            view=None,
        )

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        try:
            if hasattr(self, "message") and self.message:
                await self.message.edit(content="This starter selection timed out.", view=None)
        except Exception:
            pass
        for f in self._files:
            try:
                f.close()
            except Exception:
                pass

    @discord.ui.button(label="Yes, join this team", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.member.id:
            await interaction.response.send_message("Only the recipient can confirm this starter.", ephemeral=True)
            return

        status = db_starter_claim_begin(self.cog.state, self.member.id)
        if status == "complete":
            await interaction.response.send_message(
                f"{self.member.mention} already has their starter cards.",
                ephemeral=True,
            )
            return
        if status == "in_progress":
            await interaction.response.send_message(
                "You already have an active starter selection in progress. Please finish that one first.",
                ephemeral=True,
            )
            return
        if status != "acquired":
            await interaction.response.send_message(
                "I couldn't start your starter reward right now. Please try again shortly.",
                ephemeral=True,
            )
            return

        success = False
        try:
            await interaction.response.defer(ephemeral=True, thinking=False)
            await self._show_processing_state(interaction)
            success = await self.cog._grant_starter_rewards(interaction, self.member, self.team_name, self.deck_name, self.pack_name)
        finally:
            if success:
                db_starter_claim_complete(self.cog.state, self.member.id)
            else:
                db_starter_claim_abort(self.cog.state, self.member.id)

        for child in self.children:
            child.disabled = True
        message = getattr(interaction, "message", None) or getattr(self, "message", None)
        preserved_attachments = None
        if message:
            preserved_attachments = list(getattr(message, "attachments", []) or []) or None
        await self._edit_interaction_message(
            interaction,
            view=None,
            attachments=preserved_attachments,
        )
        for f in self._files:
            try:
                f.close()
            except Exception:
                pass

    @discord.ui.button(label="No, go back", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.member.id:
            await interaction.response.send_message("Only the recipient can cancel this starter.", ephemeral=True)
            return

        for child in self.children:
            child.disabled = True
        try:
            await interaction.response.edit_message(content="Starter selection canceled.", view=None, attachments=[], embeds=[])
        except Exception:
            try:
                await interaction.delete_original_response()
            except Exception:
                pass
        db_starter_claim_abort(self.cog.state, self.member.id)
        for f in self._files:
            try:
                f.close()
            except Exception:
                pass


class StarterTeamSelect(discord.ui.Select):
    def __init__(self, parent: "StarterTeamSelectView"):
        self.parent_view = parent
        options: list[discord.SelectOption] = []
        for team_name in SET1_TEAM_ORDER:
            info = parent.team_lookup.get(team_name)
            if not info:
                continue
            options.append(
                discord.SelectOption(
                    label=team_name,
                    value=team_name,
                    description=f"Join Team {team_name}",
                )
            )
        if not options:
            options = [discord.SelectOption(label="No teams available", value="__none__", default=True)]
        super().__init__(
            placeholder="Choose your team…",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.parent_view.member.id:
            await interaction.response.send_message("Only the recipient can choose this.", ephemeral=True)
            return

        value = self.values[0]
        if value == "__none__":
            await interaction.response.send_message("No team starters are configured.", ephemeral=True)
            return

        info = self.parent_view.team_lookup.get(value)
        if not info:
            await interaction.response.send_message("That team is not available right now.", ephemeral=True)
            return

        deck_name = info.get("deck_name")
        deck_url = info.get("deck_url") or ""
        pack_name = info.get("pack_name") or deck_name
        if not deck_name:
            await interaction.response.send_message("No starter deck is configured for that team.", ephemeral=True)
            return
        if deck_name not in (self.parent_view.state.starters_index or {}):
            await interaction.response.send_message(
                "That starter deck isn't available right now. Please ask an admin to load the starter lists.",
                ephemeral=True,
            )
            return

        # Show a loading state while we build confirmation assets.
        self.disabled = True
        self.placeholder = "loading starter data..."

        # Acknowledge the interaction quickly so Discord doesn't expire the
        # token while we render confirmation assets (this has been observed for
        # the Water team selection in particular).
        if not interaction.response.is_done():
            try:
                await interaction.response.defer(thinking=False, ephemeral=True)
            except Exception:
                pass

        try:
            if interaction.response.is_done():
                await interaction.edit_original_response(view=self.parent_view)
            else:
                await interaction.response.edit_message(view=self.parent_view)
        except Exception:
            pass

        confirmation_lines = [
            f"Are you sure you want to join the **{value}** team? You will receive the following:",
            f"- **{deck_name}** starter deck: {deck_url or 'URL coming soon'}",
            f"- 12 packs of **{pack_name}**",
        ]
        confirmation_text = "\n".join(confirmation_lines)
        starter_image = _starter_image_path(value, deck_name)
        confirmation = StarterConfirmationView(self.parent_view.cog, self.parent_view.member, value, deck_name, pack_name)
        embed, files = _starter_pack_confirmation_embed(
            pack_name, confirmation_text, deck_name, starter_image_path=starter_image
        )
        confirmation.set_confirmation_embed(embed)

        confirmation.attach_files(files)

        for child in self.parent_view.children:
            child.disabled = True
        try:
            kwargs = {
                "content": confirmation_text if not embed else None,
                "embeds": [embed] if embed else None,
                "attachments": files or None,
                "view": confirmation,
            }

            if interaction.response.is_done():
                message = await interaction.edit_original_response(**kwargs)
            else:
                message = await interaction.response.edit_message(**kwargs)
            confirmation.message = message or interaction.message
        except Exception:
            for f in files:
                try:
                    f.close()
                except Exception:
                    pass
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "I couldn't show the confirmation right now. Please try again.",
                        ephemeral=True,
                    )
                else:
                    await interaction.followup.send(
                        "I couldn't show the confirmation right now. Please try again.",
                        ephemeral=True,
                    )
            except Exception:
                pass


class StarterTeamSelectView(View):
    def __init__(self, cog: "Start", state, member: discord.Member, timeout: float = 180):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.state = state
        self.member = member
        self.team_lookup = _team_info()
        self.add_item(StarterTeamSelect(self))

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        try:
            if hasattr(self, "message") and self.message:
                await self.message.edit(content="This starter selection timed out.", view=None)
        except Exception:
            pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.member.id


class Start(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: AppState = self.bot.state

    async def _grant_starter_rewards(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        team_name: str,
        deck_name: str,
        pack_name: str,
    ) -> bool:
        granted = grant_starter_to_user(self.state, member.id, deck_name)
        if not granted:
            await interaction.followup.send("That starter deck appears to be empty.", ephemeral=True)
            return False

        try:
            await ensure_rarity_emojis(
                self.bot,
                guild_ids=[interaction.guild.id],
                create_if_missing=True,
                verbose=False,
            )
        except discord.Forbidden:
            await interaction.followup.send(
                "I need the Manage Emojis and Stickers permission to create rarity emojis.",
                ephemeral=True,
            )
            return False
        except Exception:
            await interaction.followup.send(
                "Something went wrong while preparing rarity emojis. Please try again shortly.",
                ephemeral=True,
            )
            return False

        role = discord.utils.get(interaction.guild.roles, name=team_name)
        if not role:
            try:
                role = await interaction.guild.create_role(name=team_name, reason="Starter gate")
            except discord.Forbidden:
                await interaction.followup.send(
                    "I couldn't create your team role. Please ensure I have Manage Roles permission and try again.",
                    ephemeral=True,
                )
                return False
            except Exception:
                await interaction.followup.send(
                    "I couldn't set up your team role right now. Please try again shortly.",
                    ephemeral=True,
                )
                return False

        try:
            await member.add_roles(role, reason="Claimed starter deck")
        except discord.Forbidden:
            await interaction.followup.send(
                "I need permission to manage roles in order to assign your team role.",
                ephemeral=True,
            )
            return False
        except Exception:
            await interaction.followup.send(
                "Something went wrong while assigning your team role. Please try again shortly.",
                ephemeral=True,
            )
            return False

        if deck_name.lower().__contains__("mambo"):
            await interaction.channel.send(f"Sploosh! {member.mention} selected the **{team_name}** team! "
                                           f"Sending you your starter cards now.")
        elif deck_name.lower().__contains__("fire"):
            await interaction.channel.send(f"That's Hot! {member.mention} selected the **{team_name}** team! "
                                           f"Sending you your starter cards now.")
        else:
            await interaction.channel.send(f"{member.mention} selected the **{team_name}** team!")

        START_PACKS = 12
        guaranteed_tops = ["super"] * 8 + ["ultra"] * 3 + ["secret"]

        per_pack: list[list[dict]] = []
        for top_rarity in guaranteed_tops:
            try:
                cards = open_pack_with_guaranteed_top_from_csv(self.state, pack_name, top_rarity)
            except ValueError:
                cards = open_pack_from_csv(self.state, pack_name, 1)
            per_pack.append(cards)

        flat = [c for cards in per_pack for c in cards]
        db_add_cards(self.state, interaction.user.id, flat, pack_name)

        dm_sent = False
        dm_channel = None
        try:
            dm_channel = await interaction.user.create_dm()
            for i, cards in enumerate(per_pack, start=1):
                content, embeds, files = _pack_embed_for_cards(interaction.client, pack_name, cards, i, START_PACKS)
                send_kwargs: dict = {"embeds": embeds}
                if content:
                    send_kwargs["content"] = content
                if files:
                    send_kwargs["files"] = files
                await dm_channel.send(**send_kwargs)
                if START_PACKS > 5:
                    await asyncio.sleep(0.2)
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
                    member.id, _week1_pack_for_member(member), pack_catchup_total
                )
                catchup_lines.append(pack_ack)

        daily_total = db_starter_daily_get_total(self.state)
        if daily_total > 0:
            new_wallet = credit_mambucks(self.state, member.id, daily_total)
            catchup_lines.append(
                f"Credited {mambucks_label(daily_total)} (new total: {mambucks_label(new_wallet)})."
            )

        if CURRENT_ACTIVE_SET in (2, 3):
            frostfire_total = add_shards(self.state, member.id, 1, 35000)
            catchup_lines.append(
                f"Credited {shards_label(35000, 1)} (new total: {shards_label(frostfire_total, 1)})."
            )

        if CURRENT_ACTIVE_SET == 3:
            sandstorm_total = add_shards(self.state, member.id, 2, 35000)
            catchup_lines.append(
                f"Credited {shards_label(35000, 2)} (new total: {shards_label(sandstorm_total, 2)})."
            )

        summary = (
            f"Welcome to the **{team_name}** team {interaction.user.mention}!"
            f" I sent you your starter deck and **{START_PACKS}** pack{'s' if START_PACKS != 1 else ''} of **{pack_name}** to get started!"
            f"{' Pack results sent via DM.' if dm_sent else ' I couldn’t DM you; posting results here.'}"
            f" You can view your collection with the /collection command, or use the /collection_export command to get a csv version "
            f"to upload to ygoprodeck. Happy dueling!"
        )

        if catchup_lines:
            summary += "\n\n" + catchup_note + "\n" + "\n".join(f"• {line}" for line in catchup_lines)

        # create daily reward quest entry for this user
        quests_cog = self.bot.get_cog("Quests")
        try:
            if quests_cog and getattr(quests_cog, "qm", None):
                await quests_cog.qm.fast_forward_daily_rollovers(
                    now_et().date(), include_user_ids=[member.id]
                )
        except Exception:
            pass

        await interaction.channel.send(summary)

        if not dm_sent:
            try:
                dm_channel = dm_channel or await interaction.user.create_dm()
                for i, cards in enumerate(per_pack, start=1):
                    content, embeds, files = _pack_embed_for_cards(interaction.client, pack_name, cards, i, START_PACKS)
                    send_kwargs: dict = {"embeds": embeds}
                    if content:
                        send_kwargs["content"] = content
                    if files:
                        send_kwargs["files"] = files
                    await dm_channel.send(**send_kwargs)
                    if START_PACKS > 5:
                        await asyncio.sleep(0.2)
            except Exception:
                pass

        try:
            await interaction.delete_original_response()
        except Exception:
            pass

        return True

    @app_commands.command(name="start", description="Choose your team and receive your matching starter deck and packs")
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

        # Starter guard (check only; the lock is acquired after confirmation)
        claim_status = db_starter_claim_status(self.state, member.id)
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

        view = StarterTeamSelectView(self, self.state, member)
        try:
            await interaction.response.send_message(
                content=f"{member.mention}, choose your team to get started:",
                view=view,
                ephemeral=True
            )
            try:
                view.message = await interaction.original_response()
            except Exception:
                pass
        except Exception:
            raise

async def setup(bot: commands.Bot):
    await bot.add_cog(Start(bot))
