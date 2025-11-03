# cogs/gamba.py
import os
import random
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence

import discord
from discord import app_commands
from discord.ext import commands

from core.cards_shop import card_label
from core.constants import GAMBA_DEFAULT_SHARD_SET_ID, GAMBA_PRIZES
from core.currency import mambucks_label, shards_label
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


def _build_rarity_pools_from_state(state) -> dict[str, list[tuple[str, dict]]]:
    pools: dict[str, list[tuple[str, dict]]] = {
        "COMMON": [],
        "RARE": [],
        "SUPER RARE": [],
        "ULTRA RARE": [],
        "SECRET RARE": [],
    }

    def add_from(index):
        if not isinstance(index, dict):
            return
        for set_name, container in index.items():
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


def _pick_random_card_by_rarity(state, rarity: str) -> Optional[tuple[str, dict]]:
    target = _normalize_rarity(rarity)
    pools = _build_rarity_pools_from_state(state)

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
            )
        )
    return prizes


def _prize_weights(prizes: Sequence[GambaPrize]) -> List[float]:
    return [max(p.weight, 0.0) for p in prizes]


async def _resolve_and_award_prize(state, user_id: int, prize: GambaPrize) -> str:
    if prize.prize_type == "card" and prize.rarity:
        picked = _pick_random_card_by_rarity(state, prize.rarity)
        if picked:
            set_name, printing = picked
            await _award_card_to_user(state, user_id, printing, set_name, qty=1)
            return card_label(printing)
        return prize.description

    if prize.prize_type == "shards":
        amount = int(prize.amount or 0)
        set_id = int(prize.shard_set_id or GAMBA_DEFAULT_SHARD_SET_ID)
        if amount:
            try:
                db_shards_add(state, user_id, set_id, amount)
            except Exception as exc:
                print(f"[gamba] failed to add shards: {exc}")
        return f"💎 {shards_label(amount, set_id)}"

    if prize.prize_type == "mambucks":
        amount = int(prize.amount or 0)
        if amount:
            try:
                db_wallet_add(state, user_id, d_mambucks=amount)
            except Exception as exc:
                print(f"[gamba] failed to add mambucks: {exc}")
        return f"💰 {mambucks_label(amount)}"

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

    def _prize_lines(self) -> List[str]:
        total = sum(self._weights)
        if total <= 0:
            count = len(self.prizes) or 1
            pct = 100.0 / count
            return [f"• {p.description} — {pct:.1f}%" for p in self.prizes]
        return [
            f"• {p.description} — {(max(w, 0.0) / total) * 100:.1f}%"
            for p, w in zip(self.prizes, self._weights)
        ]

    def _gamba_embed(self, chips: int) -> tuple[discord.Embed, list[discord.File]]:
        description = (
            "Welcome to snipe hunter's slots! I run a fair casino and like to let my clients know what they're playing for. "
            "Here's what I got on my wheel today:\n\n"
            + "\n".join(self._prize_lines())
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