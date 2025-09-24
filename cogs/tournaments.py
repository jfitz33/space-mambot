from __future__ import annotations

import os
from collections import Counter

import discord
from discord import app_commands
from discord.ext import commands

from core.banlist import load_banlist
from core.cards_shop import find_card_name_by_id
from core.db import db_get_collection
from core.state import AppState

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None


def _normalize_card_id(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        try:
            return str(int(text))
        except ValueError:
            return None
    return text.lower()


def _parse_ydk(text: str) -> tuple[Counter[str], list[str]]:
    counts: Counter[str] = Counter()
    invalid: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            continue
        if line.startswith("!"):
            # Section markers such as !side
            continue
        cid = _normalize_card_id(line)
        if not cid:
            invalid.append(line)
            continue
        counts[cid] += 1
    return counts, invalid


def _format_card_label(card_id: str, card_name: str | None) -> str:
    if card_name:
        return f"{card_name} ({card_id})"
    return f"Card ID {card_id}"


class Tournaments(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: AppState = bot.state

    @app_commands.command(name="deck_check", description="Verify a YDK deck against your collection and the banlist")
    @app_commands.describe(deck_file="Upload a .ydk deck file")
    @app_commands.guilds(GUILD)
    async def deck_check(self, interaction: discord.Interaction, deck_file: discord.Attachment):
        filename = deck_file.filename or ""
        if not filename.lower().endswith(".ydk"):
            await interaction.response.send_message("Please upload a `.ydk` deck file.", ephemeral=True)
            return

        try:
            raw_bytes = await deck_file.read()
        except Exception:
            await interaction.response.send_message("I couldn't read that file. Please try again.", ephemeral=True)
            return

        text = raw_bytes.decode("utf-8-sig", errors="ignore")
        card_counts, invalid_lines = _parse_ydk(text)
        if not card_counts:
            message = "I couldn't find any card IDs in that deck file."
            if invalid_lines:
                example = invalid_lines[0]
                message += f" Example ignored line: `{example}`."
            await interaction.response.send_message(message, ephemeral=True)
            return

        collection_rows = db_get_collection(self.state, interaction.user.id) or []
        owned_by_id: dict[str, int] = {}
        owned_by_name: dict[str, int] = {}
        for row in collection_rows:
            name, qty, *_rest, _, raw_id = row
            try:
                qty_int = int(qty)
            except (TypeError, ValueError):
                continue
            if qty_int <= 0:
                continue
            norm_id = _normalize_card_id(raw_id)
            if norm_id:
                owned_by_id[norm_id] = owned_by_id.get(norm_id, 0) + qty_int
            name_key = (name or "").strip().lower()
            if name_key:
                owned_by_name[name_key] = owned_by_name.get(name_key, 0) + qty_int

        banlist_path = getattr(self.state, "banlist_path", None)
        banlist = load_banlist(banlist_path)

        issues: list[str] = []
        for card_id, required_qty in card_counts.items():
            card_name = find_card_name_by_id(self.state, card_id)
            display = _format_card_label(card_id, card_name)

            owned_qty = owned_by_id.get(card_id)
            if owned_qty is None:
                owned_qty = 0
            if card_name:
                owned_qty = max(owned_qty, owned_by_name.get(card_name.strip().lower(), 0))

            if owned_qty < required_qty:
                issues.append(
                    f"• {display}: deck needs {required_qty}, you only own {owned_qty}."
                )

            limit = banlist.limit_for(card_id=card_id, card_name=card_name)
            if required_qty > limit:
                if limit == 0:
                    issues.append(
                        f"• {display} is forbidden on this banlist but appears {required_qty} times."
                    )
                else:
                    copy_word = "copy" if limit == 1 else "copies"
                    issues.append(
                        f"• {display} is limited to {limit} {copy_word} (deck has {required_qty})."
                    )

        if issues:
            details = "\n".join(issues)
            if invalid_lines:
                snippet = ", ".join(f"`{line}`" for line in invalid_lines[:3])
                details += (
                    f"\n\nIgnored {len(invalid_lines)} non-card line"
                    f"{'s' if len(invalid_lines) != 1 else ''}: {snippet}"
                )
            await interaction.response.send_message(f"**Deck issues detected:**\n{details}", ephemeral=True)
            return

        await interaction.response.send_message("Your deck is perfectly legal.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(Tournaments(bot))
