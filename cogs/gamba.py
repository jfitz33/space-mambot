# cogs/gamba.py
import os
import random
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence

import discord
from discord import app_commands
from discord.ext import commands

from core.feature_flags import is_set1_week1_locked
from core.cards_shop import card_label
from core.constants import (
    CURRENT_ACTIVE_SET,
    GAMBA_DEFAULT_SHARD_SET_ID,
    GAMBA_PRIZES,
    set_id_for_pack,
)
from core.currency import shards_label
from core.images import mambuck_badge
from core.db import (
    db_add_cards,
    db_shards_add,
    db_wallet_add,
    db_wheel_tokens_get,
    db_wheel_tokens_try_spend,
)


GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None


_REPO_ROOT = Path(__file__).resolve().parents[1]
_SNIPE_HUNTER_IMAGE = _REPO_ROOT / "images" / "snipe_hunter_slots.png"


@dataclass(frozen=True)
class GambaPrize:
    key: str
    weight: float
    prize_type: str
    description: str
    rarity: Optional[str] = None
    amount: Optional[int] = None
    shard_set_id: Optional[int] = None
    shard_type: Optional[str] = None


def _normalize_rarity(rarity: str) -> str:
    normalized = (rarity or "").strip().upper()
    if normalized in {"C", "COMMON"}:
        return "COMMON"
    if normalized in {"R", "RARE"}:
        return "RARE"
    if normalized in {"SR", "SUPER", "SUPER RARE"}:
        return "SUPER RARE"
    if normalized in {"UR", "ULTRA", "ULTRA RARE"}:
        return "ULTRA RARE"
    if normalized in {"SCR", "SECRET", "SECRET RARE"}:
        return "SECRET RARE"
    return "SECRET RARE"


def _build_rarity_pools_from_state(
    state, *, target_set_id: Optional[int] = None
) -> dict[str, list[tuple[str, dict]]]:
    pools: dict[str, list[tuple[str, dict]]] = {
        "COMMON": [],
        "RARE": [],
        "SUPER RARE": [],
        "ULTRA RARE": [],
        "SECRET RARE": [],
    }

    set_filter = None
    try:
        set_filter = int(target_set_id) if target_set_id is not None else None
    except (TypeError, ValueError):
        set_filter = None

    def add_from(index):
        if not isinstance(index, dict):
            return
        for set_name, container in index.items():
            set_id = set_id_for_pack(set_name)
            if set_filter is not None and set_id != set_filter:
                continue
            if set_filter is not None and set_id is None:
                continue
            if isinstance(container, dict):
                by_rarity = container.get("by_rarity")
                if isinstance(by_rarity, dict):
                    for raw_key, items in by_rarity.items():
                        norm = _normalize_rarity(raw_key)
                        if not isinstance(items, list):
                            continue
                        for printing in items:
                            if isinstance(printing, dict):
                                pools.setdefault(norm, []).append((set_name, printing))
                cards = container.get("cards")
                if isinstance(cards, list):
                    for printing in cards:
                        if isinstance(printing, dict):
                            rarity = _normalize_rarity(printing.get("rarity"))
                            pools.setdefault(rarity, []).append((set_name, printing))
            elif isinstance(container, list):
                for printing in container:
                    if isinstance(printing, dict):
                        rarity = _normalize_rarity(printing.get("rarity"))
                        pools.setdefault(rarity, []).append((set_name, printing))

    add_from(getattr(state, "packs_index", None))
    starters = getattr(state, "starters_index", None) or getattr(state, "starters", None)
    add_from(starters)
    return pools


def _pick_random_card_by_rarity(
    state, rarity: str, *, target_set_id: Optional[int] = None
) -> Optional[tuple[str, dict]]:
    target = _normalize_rarity(rarity)
    pools = _build_rarity_pools_from_state(state, target_set_id=target_set_id)

    degrade_order = {
        "SECRET RARE": ["SECRET RARE", "ULTRA RARE", "SUPER RARE", "RARE", "COMMON"],
        "ULTRA RARE": ["ULTRA RARE", "SUPER RARE", "RARE", "COMMON"],
        "SUPER RARE": ["SUPER RARE", "RARE", "COMMON"],
        "RARE": ["RARE", "COMMON"],
        "COMMON": ["COMMON"],
    }

    for bucket in degrade_order.get(target, ["COMMON"]):
        options = pools.get(bucket) or []
        if options:
            return random.choice(options)
    return None


async def _award_card_to_user(state, user_id: int, printing: dict, set_name: str, qty: int = 1) -> None:
    try:
        db_add_cards(state, user_id, [printing] * int(max(1, qty)), set_name)
    except Exception as exc:
        print(f"[gamba] failed to add card prize: {exc}")


def _load_prizes() -> List[GambaPrize]:
    prizes: List[GambaPrize] = []
    for entry in GAMBA_PRIZES:
        prizes.append(
            GambaPrize(
                key=entry.get("key", ""),
                weight=float(entry.get("weight", 0.0) or 0.0),
                prize_type=str(entry.get("prize_type", "")).strip(),
                description=str(entry.get("description", "")).strip(),
                rarity=entry.get("rarity"),
                amount=entry.get("amount"),
                shard_set_id=entry.get("shard_set_id"),
                shard_type=entry.get("shard_type"),
            )
        )
    return prizes


def _prize_weights(prizes: Sequence[GambaPrize]) -> List[float]:
    return [max(p.weight, 0.0) for p in prizes]

def _shard_type_to_set_id(shard_type: Optional[str]) -> Optional[int]:
    normalized = str(shard_type or "").strip().casefold()
    if not normalized:
        return None

    shard_aliases = {
        "elemental": 1,
        "frostfire": 1,
        "frost": 1,
        "set1": 1,
        "sandstorm": 2,
        "sand": 2,
        "set2": 2,
        "temporal": 3,
        "time": 3,
        "set3": 3,
    }
    return shard_aliases.get(normalized)


def _resolve_shard_set_id(prize: GambaPrize) -> int:
    for candidate in (prize.shard_set_id, _shard_type_to_set_id(prize.shard_type)):
        try:
            if candidate is not None:
                return int(candidate)
        except (TypeError, ValueError):
            continue
    try:
        return int(GAMBA_DEFAULT_SHARD_SET_ID)
    except (TypeError, ValueError):
        return 1


def _rarity_badge_tokens(state) -> dict[str, str]:
    rid = getattr(state, "rarity_emoji_ids", {}) or {}
    anim = getattr(state, "rarity_emoji_animated", {}) or {}

    def badge(key: str, fallback: str) -> str:
        eid = rid.get(key)
        if eid:
            prefix = "a" if anim.get(key) else ""
            return f"<{prefix}:rar_{key}:{eid}>"
        return fallback

    tokens = {
        "super": badge("super", ":rar_super:"),
        "ultra": badge("ultra", ":rar_ultra:"),
        "secret": badge("secret", ":rar_secret:"),
        "frostfire": badge("frostfire", ":rar_frostfire:"),
        "sandstorm": badge("sandstorm", ":rar_sandstorm:"),
        "temporal": badge("temporal", ":rar_temporal:"),
    }

    tokens["mambuck"] = badge("mambuck", mambuck_badge(state))

    return tokens


def _render_prize_description(prize: GambaPrize, state) -> str:
    desc = prize.description
    badges = _rarity_badge_tokens(state)
    for token, badge in badges.items():
        desc = desc.replace(f":rar_{token}:", badge)
    
    if prize.prize_type == "mambucks":
        icon = badges.get("mambuck", mambuck_badge(state))
        amount = int(prize.amount or 0)
        if amount:
            return f"{amount} {icon} Mambucks"
        return f"{icon} {desc}"
    return desc

def _shard_badge_for_set(state, set_id: int) -> str:
    set_key = {1: "frostfire", 2: "sandstorm", 3: "temporal"}.get(
        int(set_id), "frostfire"
    )
    return _rarity_badge_tokens(state).get(set_key, f":rar_{set_key}:")

async def _resolve_and_award_prize(state, user_id: int, prize: GambaPrize) -> str:
    if prize.prize_type == "card" and prize.rarity:
        picked = _pick_random_card_by_rarity(
            state, prize.rarity, target_set_id=CURRENT_ACTIVE_SET
        )
        if picked:
            set_name, printing = picked
            await _award_card_to_user(state, user_id, printing, set_name, qty=1)
            return card_label(printing)
        return prize.description

    if prize.prize_type == "shards":
        amount = int(prize.amount or 0)
        set_id = _resolve_shard_set_id(prize)
        if amount:
            try:
                db_shards_add(state, user_id, set_id, amount)
            except Exception as exc:
                print(f"[gamba] failed to add shards: {exc}")
        badge = _shard_badge_for_set(state, set_id)
        return f"{amount}{badge}"

    if prize.prize_type == "mambucks":
        amount = int(prize.amount or 0)
        if amount:
            try:
                db_wallet_add(state, user_id, d_mambucks=amount)
            except Exception as exc:
                print(f"[gamba] failed to add mambucks: {exc}")
        icon = mambuck_badge(state)
        return f"{amount} {icon} Mambucks"

    return prize.description


class GambaConfirmView(discord.ui.View):
    def __init__(self, state, requester_id: int, prizes: Sequence[GambaPrize]):
        super().__init__(timeout=90)
        self.state = state
        self.requester_id = requester_id
        self.prizes = list(prizes)
        self._weights = _prize_weights(prizes)
        self._processing = False
        self.message: Optional[discord.Message] = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("This isn’t for you.", ephemeral=True)
            return False
        return True

    def _choose_prize(self) -> GambaPrize:
        if not self.prizes:
            raise RuntimeError("gamba prizes unavailable")
        total = sum(self._weights)
        if total <= 0:
            return random.choice(self.prizes)
        return random.choices(self.prizes, weights=self._weights, k=1)[0]

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.success, emoji="✅")
    async def yes(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._processing:
            await interaction.response.send_message("Processing another spin…", ephemeral=True)
            return

        new_balance = db_wheel_tokens_try_spend(self.state, self.requester_id, 1)
        if new_balance is None:
            await interaction.response.send_message("must have a gamba chip to gamba", ephemeral=True)
            return

        self._processing = True
        await interaction.response.defer()

        prize = self._choose_prize()
        prize_text = await _resolve_and_award_prize(self.state, self.requester_id, prize)

        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.delete()
        except Exception:
            pass

        await interaction.followup.send(f"Congrats! {interaction.user.mention} won {prize_text}!")
        self.stop()

    @discord.ui.button(label="No", style=discord.ButtonStyle.danger)
    async def no(self, interaction: discord.Interaction, _: discord.ui.Button):
        if self._processing:
            await interaction.response.send_message("Please wait for the current spin to finish.", ephemeral=True)
            return
        self.stop()
        for child in self.children:
            child.disabled = True
        content = f"{interaction.user.mention} decided not to gamble right now"
        try:
            await interaction.response.edit_message(
                content=content,
                embeds=[],
                view=None,
                attachments=[],
            )
        except discord.InteractionResponded:
            try:
                await interaction.message.edit(
                    content=content,
                    embeds=[],
                    view=None,
                    attachments=[],
                )
            except Exception:
                pass
        except Exception:
            pass
        self.message = None

    async def on_timeout(self) -> None:
        if self._processing:
            return

        self.stop()
        for child in self.children:
            child.disabled = True

        message = getattr(self, "message", None)
        if message is None:
            return

        try:
            await message.edit(
                content="Snipe Hunter got tired of waiting",
                embeds=[],
                view=None,
                attachments=[],
            )
        except Exception:
            pass


class Gamba(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.prizes = _load_prizes()
        self._weights = _prize_weights(self.prizes)

    def _prize_lines(self, state) -> List[str]:
        total = sum(self._weights)
        if total <= 0:
            count = len(self.prizes) or 1
            pct = 100.0 / count
            return [
                f"• {_render_prize_description(p, state)} — {pct:.1f}%"
                for p in self.prizes
            ]
        return [
            f"• {_render_prize_description(p, state)} — {(max(w, 0.0) / total) * 100:.1f}%"
            for p, w in zip(self.prizes, self._weights)
        ]

    def _gamba_embed(self, chips: int) -> tuple[discord.Embed, list[discord.File]]:
        description = (
            "Welcome to snipe hunter's slots! I run a fair casino and like to let my clients know what they're playing for. "
            "Spin for a chance at a random card or currency. Here's what I got on my wheel today:\n\n"
            + "\n".join(self._prize_lines(self.bot.state))
            + "\n\n**Spend 1 gamba chip to spin the slots?**"
        )
        embed = discord.Embed(title="Snipe Hunter's Slots", description=description, color=0x71368a)
        embed.set_footer(text=f"You have {chips} gamba chip(s).")

        files: list[discord.File] = []
        if _SNIPE_HUNTER_IMAGE.is_file():
            files.append(discord.File(_SNIPE_HUNTER_IMAGE, filename="snipe_hunter.png"))
            embed.set_image(url="attachment://snipe_hunter.png")
        return embed, files

    @app_commands.command(name="gamba", description="Play Snipe Hunter's slots using a gamba chip.")
    @app_commands.guilds(GUILD)
    async def gamba(self, interaction: discord.Interaction):
        chips = db_wheel_tokens_get(self.bot.state, interaction.user.id)
        if chips <= 0:
            await interaction.response.send_message("must have a gamba chip to gamba", ephemeral=True)
            return

        embed, files = self._gamba_embed(chips)
        view = GambaConfirmView(self.bot.state, interaction.user.id, self.prizes)

        if files:
            await interaction.response.send_message(embed=embed, view=view, files=files)
        else:
            await interaction.response.send_message(embed=embed, view=view)

        try:
            view.message = await interaction.original_response()
        except Exception:
            view.message = None


async def setup(bot: commands.Bot):
    await bot.add_cog(Gamba(bot))

    if is_set1_week1_locked():
        for guild in (GUILD, None):
            bot.tree.remove_command(
                "gamba", type=discord.AppCommandType.chat_input, guild=guild
            )