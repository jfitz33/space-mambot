from __future__ import annotations

import asyncio
import base64
import binascii
import os
import struct
from collections import Counter

import discord
from discord import app_commands
from discord.ext import commands

from core.banlist import load_banlist
from core.cards_shop import fetch_card_names_by_id, find_card_name_by_id
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


def _ydke_to_ydk_text(ydke: str) -> str | None:
    prefix = "ydke://"
    if ydke.lower().startswith(prefix):
        payload = ydke[len(prefix) :]
    else:
        payload = ydke

    sections = payload.split("!")
    if len(sections) < 3:
        return None

    converted: list[str] = []
    labels = ("#main", "#extra", "#side")

    for label, segment in zip(labels, sections[:3]):
        converted.append(label)
        if not segment:
            continue
        padding = (-len(segment)) % 4
        segment_padded = segment + ("=" * padding)
        try:
            raw = base64.urlsafe_b64decode(segment_padded)
        except binascii.Error:
            return None
        if len(raw) % 4 != 0:
            return None
        for index in range(0, len(raw), 4):
            card_id = struct.unpack("<I", raw[index : index + 4])[0]
            converted.append(str(card_id))

    return "\n".join(converted)


def _format_card_label(card_id: str, card_name: str | None) -> str:
    if card_name:
        return f"{card_name} ({card_id})"
    return f"Card ID {card_id}"


def _chunk_issue_messages(header: str, lines: list[str], *, limit: int = 2000) -> list[str]:
    segments: list[str] = []
    for line in lines:
        if len(line) <= limit:
            segments.append(line)
            continue
        start = 0
        while start < len(line):
            segments.append(line[start : start + limit])
            start += limit

    chunks: list[str] = []
    current = header
    for segment in segments:
        addition = ("\n" if current else "") + segment
        if len(current) + len(addition) > limit:
            if current:
                chunks.append(current)
            current = segment
        else:
            current += addition

    if current:
        chunks.append(current)

    return chunks


class Tournaments(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: AppState = bot.state

    async def _fetch_missing_card_names(self, card_ids: list[str]) -> dict[str, str]:
        if not card_ids:
            return {}

        numeric_ids = [cid for cid in card_ids if cid.isdigit()]
        if not numeric_ids:
            return {}

        loop = asyncio.get_running_loop()

        def _do_fetch() -> dict[str, str]:
            return fetch_card_names_by_id(numeric_ids)

        fetched = await loop.run_in_executor(None, _do_fetch)
        return {cid: name for cid, name in fetched.items() if name}

    @app_commands.command(name="deck_check", description="Verify a YDK deck against your collection and the banlist")
    @app_commands.guilds(GUILD)
    async def deck_check(self, interaction: discord.Interaction):
        await interaction.response.send_message(
            "Please check DMs for deck check submission", ephemeral=True
        )

        try:
            dm_channel = interaction.user.dm_channel or await interaction.user.create_dm()
        except discord.Forbidden:
            await interaction.followup.send(
                "I couldn't send you a DM. Please enable direct messages and try again.",
                ephemeral=True,
            )
            return

        await dm_channel.send("Please attach a ydk file or copy and paste a ydke code below")

        def check(message: discord.Message) -> bool:
            return (
                message.author.id == interaction.user.id
                and message.channel == dm_channel
            )

        try:
            submission: discord.Message = await self.bot.wait_for(
                "message", timeout=300, check=check
            )
        except asyncio.TimeoutError:
            await dm_channel.send("Deck check cancelled because no deck was submitted in time.")
            return

        text: str | None = None

        if submission.attachments:
            attachment = submission.attachments[0]
            filename = (attachment.filename or "").lower()
            if not filename.endswith(".ydk"):
                await dm_channel.send(
                    "That file isn't a `.ydk` deck. Please try the command again with a valid file."
                )
                return
            try:
                raw_bytes = await attachment.read()
            except Exception:
                await dm_channel.send("I couldn't read that file. Please try again later.")
                return
            text = raw_bytes.decode("utf-8-sig", errors="ignore")
        else:
            content = (submission.content or "").strip()
            if not content:
                await dm_channel.send(
                    "I didn't receive any deck information. Please run the command again."
                )
                return
            if content.lower().startswith("ydke://"):
                text = _ydke_to_ydk_text(content)
                if text is None:
                    await dm_channel.send(
                        "That YDKE code couldn't be parsed. Please make sure it's a valid code."
                    )
                    return
            else:
                text = content

        card_counts, invalid_lines = _parse_ydk(text)
        if not card_counts:
            message = "I couldn't find any card IDs in that submission."
            if invalid_lines:
                example = invalid_lines[0]
                message += f" Example ignored line: `{example}`."
            await dm_channel.send(message)
            return

        async with dm_channel.typing():
            await self._send_deck_results(
                dm_channel, interaction.user.id, card_counts, invalid_lines
            )

    async def _send_deck_results(
        self,
        channel: discord.abc.Messageable,
        user_id: int,
        card_counts: Counter[str],
        invalid_lines: list[str],
    ) -> None:
        collection_rows = db_get_collection(self.state, user_id) or []
        owned_by_id: dict[str, int] = {}
        owned_by_name: dict[str, int] = {}
        owned_name_by_id: dict[str, str] = {}
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
                clean_name = (name or "").strip()
                if clean_name:
                    owned_name_by_id.setdefault(norm_id, clean_name)
            name_key = (name or "").strip().lower()
            if name_key:
                owned_by_name[name_key] = owned_by_name.get(name_key, 0) + qty_int

        banlist_path = getattr(self.state, "banlist_path", None)
        banlist = load_banlist(banlist_path)

        issues: list[str] = []

        known_names: dict[str, str] = {}
        missing_ids: list[str] = []
        for card_id in card_counts:
            card_name = find_card_name_by_id(self.state, card_id)
            if not card_name:
                card_name = owned_name_by_id.get(card_id)
            if card_name:
                known_names[card_id] = card_name
            else:
                missing_ids.append(card_id)

        invalid_card_ids: set[str] = set()
        if missing_ids:
            fetched_names = await self._fetch_missing_card_names(missing_ids)
            for cid, name in fetched_names.items():
                if name:
                    known_names[cid] = name
                    invalid_card_ids.add(cid)

        for card_id, required_qty in card_counts.items():
            card_name = known_names.get(card_id)
            if not card_name:
                display = _format_card_label(card_id, card_name)
                issues.append(f"{display} is not in the legal cardpool.")
                continue

            display = _format_card_label(card_id, card_name)
            display_name = card_name or display

            if card_id in invalid_card_ids:
                issues.append(
                    f"Invalid card: {display_name} is not in the legal cardpool."
                )
                continue

            owned_qty = owned_by_id.get(card_id) or 0
            owned_qty = max(
                owned_qty,
                owned_by_name.get(card_name.strip().lower(), 0),
            )

            if owned_qty < required_qty:
                issues.append(
                    f"Not enough copies: You only own {owned_qty} copies of {display_name}"
                )

            limit = banlist.limit_for(card_name)
            if required_qty > limit:
                issues.append(
                    f"Banlist: Only {limit} copies of {display_name} may be played"
                )

        if issues:
            issue_lines = issues.copy()
            if invalid_lines:
                snippet = ", ".join(f"`{line}`" for line in invalid_lines[:3])
                suffix = (
                    f"Ignored {len(invalid_lines)} non-card line"
                    f"{'s' if len(invalid_lines) != 1 else ''}: {snippet}"
                )
                issue_lines.extend(["", suffix])

            messages = _chunk_issue_messages("**Deck issues detected:**", issue_lines)
            for message in messages:
                await channel.send(message)
            return

        await channel.send("Your deck is perfectly legal.")


async def setup(bot: commands.Bot):
    await bot.add_cog(Tournaments(bot))