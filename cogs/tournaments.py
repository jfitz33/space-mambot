from __future__ import annotations

import asyncio
import base64
import binascii
import os
import struct
from collections import Counter
from dataclasses import dataclass

import discord
from discord import app_commands
from discord.ext import commands

from core.banlist import load_banlist
from core.cards_shop import fetch_card_details_by_id, find_card_name_by_id
from core.deck_render import DeckCardEntry, render_deck_section_image
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


def _parse_ydk(
    text: str,
) -> tuple[Counter[str], list[str], dict[str, list[str]]]:
    counts: Counter[str] = Counter()
    invalid: list[str] = []
    sections: dict[str, list[str]] = {"main": [], "extra": [], "side": []}

    current_section = "main"

    markers = {
        "#main": "main",
        "#extra": "extra",
        "#side": "side",
        "!side": "side",
    }

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        lower_line = line.lower()
        if lower_line in markers:
            current_section = markers[lower_line]
            continue

        if line.startswith("#") or line.startswith("!"):
            # Ignore any other metadata markers
            continue

        cid = _normalize_card_id(line)
        if not cid:
            invalid.append(line)
            continue

        counts[cid] += 1
        sections.setdefault(current_section, []).append(cid)

    return counts, invalid, sections


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


@dataclass(slots=True)
class CardMetadata:
    name: str | None = None
    card_type: str | None = None
    from_state: bool = False
    from_api: bool = False


def _categorize_card_type(card_type: str | None) -> str:
    if not card_type:
        return "monster"
    lowered = card_type.lower()
    if "spell" in lowered:
        return "spell"
    if "trap" in lowered:
        return "trap"
    return "monster"


_CARD_CATEGORY_ORDER = {"monster": 0, "spell": 1, "trap": 2}


class Tournaments(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: AppState = bot.state

    async def _fetch_card_details(self, card_ids: list[str]) -> dict[str, dict[str, str]]:
        numeric_ids = []
        seen = set()
        for cid in card_ids:
            if not cid or not cid.isdigit():
                continue
            if cid in seen:
                continue
            seen.add(cid)
            numeric_ids.append(cid)

        if not numeric_ids:
            return {}

        loop = asyncio.get_running_loop()

        def _do_fetch() -> dict[str, dict[str, str]]:
            return fetch_card_details_by_id(numeric_ids)

        try:
            return await loop.run_in_executor(None, _do_fetch)
        except Exception:
            return {}

    async def _resolve_card_metadata(
        self,
        card_ids: list[str],
        owned_name_by_id: dict[str, str],
    ) -> tuple[dict[str, CardMetadata], set[str]]:
        metadata: dict[str, CardMetadata] = {}

        for cid in card_ids:
            meta = CardMetadata()
            local_name = find_card_name_by_id(self.state, cid)
            if not local_name:
                local_name = owned_name_by_id.get(cid)
            if local_name:
                meta.name = local_name
                meta.from_state = True
            metadata[cid] = meta

        fetched = await self._fetch_card_details(card_ids)

        for cid, details in fetched.items():
            meta = metadata.setdefault(cid, CardMetadata())
            name = (details.get("name") or "").strip()
            if name and not meta.name:
                meta.name = name
            card_type = (details.get("type") or "").strip()
            if card_type:
                meta.card_type = card_type
            meta.from_api = True

        invalid_card_ids = {
            cid for cid, meta in metadata.items() if meta.from_api and not meta.from_state
        }

        return metadata, invalid_card_ids

    def _build_section_entries(
        self,
        card_ids: list[str],
        metadata: dict[str, CardMetadata],
    ) -> list[DeckCardEntry]:
        decorated: list[tuple[int, str, int, DeckCardEntry]] = []
        for index, card_id in enumerate(card_ids):
            meta = metadata.get(card_id, CardMetadata())
            name = meta.name or card_id
            category = _categorize_card_type(meta.card_type)
            order = _CARD_CATEGORY_ORDER.get(category, len(_CARD_CATEGORY_ORDER))
            decorated.append(
                (
                    order,
                    name.lower(),
                    index,
                    DeckCardEntry(card_id=card_id, name=name, card_type=meta.card_type),
                )
            )

        decorated.sort()
        return [entry for *_rest, entry in decorated]

    async def _send_deck_images(
        self,
        channel: discord.abc.Messageable,
        deck_sections: dict[str, list[str]],
        metadata: dict[str, CardMetadata],
    ) -> None:
        section_specs = (
            ("Main Deck", deck_sections.get("main", []), 10),
            ("Side Deck", deck_sections.get("side", []), 15),
            ("Extra Deck", deck_sections.get("extra", []), 15),
        )

        for title, ids, max_columns in section_specs:
            entries = self._build_section_entries(ids, metadata)
            image_buffer, filename = render_deck_section_image(
                title,
                entries,
                max_columns=max_columns,
            )
            file = discord.File(image_buffer, filename=filename)
            await channel.send(f"{title} ({len(entries)} cards)", file=file)

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

        card_counts, invalid_lines, deck_sections = _parse_ydk(text)
        if not card_counts:
            message = "I couldn't find any card IDs in that submission."
            if invalid_lines:
                example = invalid_lines[0]
                message += f" Example ignored line: `{example}`."
            await dm_channel.send(message)
            return

        async with dm_channel.typing():
            await self._send_deck_results(
                dm_channel,
                interaction.user.id,
                card_counts,
                invalid_lines,
                deck_sections,
            )

    async def _send_deck_results(
        self,
        channel: discord.abc.Messageable,
        user_id: int,
        card_counts: Counter[str],
        invalid_lines: list[str],
        deck_sections: dict[str, list[str]],
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

        metadata, invalid_card_ids = await self._resolve_card_metadata(
            list(card_counts.keys()),
            owned_name_by_id,
        )

        for card_id, required_qty in card_counts.items():
            meta = metadata.get(card_id, CardMetadata())
            card_name = meta.name

            if not card_name and not meta.from_api:
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
            name_key = (card_name or "").strip().lower()
            if name_key:
                owned_qty = max(owned_qty, owned_by_name.get(name_key, 0))

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

        await self._send_deck_images(channel, deck_sections, metadata)


async def setup(bot: commands.Bot):
    await bot.add_cog(Tournaments(bot))