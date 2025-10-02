from __future__ import annotations

import asyncio
import base64
import binascii
import logging
import os
import re
import struct
from pprint import pformat
from urllib.parse import urlencode, urlparse
from collections import Counter
from dataclasses import dataclass
from html import unescape

import discord
from discord import app_commands
from discord.ext import commands
import requests

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
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def _summarize_error(detail: str, *, max_length: int = 400) -> str:
        cleaned = detail.strip()
        if not cleaned:
            return "No additional details provided."

        lowered = cleaned.lower()
        summary = cleaned

        if "<html" in lowered or "<!doctype" in lowered:
            title_match = re.search(r"<title>(.*?)</title>", cleaned, re.IGNORECASE | re.DOTALL)
            if title_match:
                summary = unescape(re.sub(r"\s+", " ", title_match.group(1)).strip())
            else:
                without_tags = re.sub(r"<[^>]+>", " ", cleaned)
                summary = re.sub(r"\s+", " ", without_tags).strip()
        else:
            summary = re.sub(r"\s+", " ", cleaned)

        if len(summary) > max_length:
            summary = summary[: max_length - 1] + "…"

        return summary

    def _get_challonge_credentials(self) -> tuple[str, str]:
        username = os.getenv("CHALLONGE_USERNAME")
        api_key = os.getenv("CHALLONGE_API_KEY")
        if not username or not api_key:
            raise RuntimeError(
                "Challonge credentials are not configured. Please set both "
                "CHALLONGE_USERNAME and CHALLONGE_API_KEY environment variables."
            )
        return username, api_key

    async def _challonge_request(
        self,
        method: str,
        path: str,
        *,
        data: dict[str, str] | None = None,
    ) -> dict:
        username, api_key = self._get_challonge_credentials()
        base_url = os.getenv("CHALLONGE_API_BASE", "https://api.challonge.com/v1")
        url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"

        parsed_url = urlparse(url)
        headers = {
            "Accept": "application/json",
            "Host": parsed_url.netloc,
        }

        def _do_request() -> dict:
            request_headers = dict(headers)
            encoded_body: bytes
            if data is not None:
                encoded_body = urlencode(data).encode("utf-8")
                request_headers["Content-Length"] = str(len(encoded_body))
                request_headers.setdefault(
                    "Content-Type", "application/x-www-form-urlencoded"
                )
            else:
                encoded_body = b""
                request_headers["Content-Length"] = "0"

            request = requests.Request(
                method,
                url,
                headers=request_headers,
                data=encoded_body,
                auth=(username, api_key),
            )
            prepared = request.prepare()

            if self.logger.isEnabledFor(logging.DEBUG):
                prepared_headers = dict(prepared.headers)
                body_payload = prepared.body
                if isinstance(body_payload, bytes):
                    body_text = body_payload.decode("utf-8", "replace")
                else:
                    body_text = str(body_payload) if body_payload is not None else ""
                self.logger.debug(
                    "Challonge request prepared:\nURL: %s\nMethod: %s\nHeaders: %s\nBody: %s",
                    prepared.url,
                    prepared.method,
                    pformat(prepared_headers),
                    body_text,
                )

            with requests.Session() as session:
                response = session.send(prepared, timeout=30)
            response.raise_for_status()
            if not response.content:
                return {}
            try:
                return response.json()
            except ValueError:
                return {"raw": response.text}

        try:
            return await asyncio.to_thread(_do_request)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            detail = exc.response.text if exc.response is not None else str(exc)
            summary = self._summarize_error(detail)

            if exc.response is not None:
                self.logger.debug(
                    "Challonge API HTTPError (%s %s): status=%s, payload_length=%d",
                    method,
                    url,
                    status,
                    len(exc.response.text or ""),
                )

            raise RuntimeError(
                f"Challonge API request failed with status {status}: {summary}"
            ) from exc
        except requests.RequestException as exc:
            summary = self._summarize_error(str(exc))
            raise RuntimeError(f"Challonge request failed: {summary}") from exc

    async def _create_challonge_tournament(
        self,
        *,
        name: str,
        tournament_type: str,
        url_slug: str | None = None,
    ) -> dict:
        payload = {
            "tournament[name]": name,
            "tournament[tournament_type]": tournament_type,
        }
        if url_slug:
            payload["tournament[url]"] = url_slug
        response = await self._challonge_request("POST", "/tournaments.json", data=payload)
        return response.get("tournament", response)

    async def _create_challonge_participant(
        self,
        tournament_id: str,
        *,
        name: str,
        discord_id: int | None = None,
    ) -> dict:
        payload = {
            "participant[name]": name,
        }
        if discord_id is not None:
            payload["participant[misc]"] = str(discord_id)
        response = await self._challonge_request(
            "POST",
            f"/tournaments/{tournament_id}/participants.json",
            data=payload,
        )
        return response.get("participant", response)

    @app_commands.command(
        name="challonge_create",
        description="Create a Challonge tournament in the configured organization.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.describe(
        name="Name that will be shown on Challonge",
        format="Tournament format",
        url_slug="Optional URL slug. Only letters, numbers, and underscores.",
    )
    @app_commands.choices(
        format=[
            app_commands.Choice(name="Single Elimination", value="single elimination"),
            app_commands.Choice(name="Double Elimination", value="double elimination"),
            app_commands.Choice(name="Swiss", value="swiss"),
        ]
    )
    async def challonge_create(
        self,
        interaction: discord.Interaction,
        name: str,
        format: app_commands.Choice[str],
        url_slug: str | None = None,
    ) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            tournament = await self._create_challonge_tournament(
                name=name,
                tournament_type=format.value,
                url_slug=url_slug,
            )
        except RuntimeError as exc:
            await interaction.followup.send(f"Failed to create tournament: {exc}", ephemeral=True)
            return

        url = tournament.get("full_challonge_url") or tournament.get("url")
        identifier = tournament.get("id") or url_slug or tournament.get("slug")
        parts = [f"Created Challonge tournament **{tournament.get('name', name)}**."]
        if identifier:
            parts.append(f"Identifier: `{identifier}`")
        if url:
            parts.append(f"URL: {url}")
        await interaction.followup.send("\n".join(parts), ephemeral=True)

    @app_commands.command(
        name="challonge_add_participant",
        description="Register a Discord member into a Challonge tournament.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.describe(
        tournament_id="The Challonge tournament identifier (slug or ID).",
        member="Discord member to register.",
    )
    async def challonge_add_participant(
        self,
        interaction: discord.Interaction,
        tournament_id: str,
        member: discord.Member,
    ) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            participant = await self._create_challonge_participant(
                tournament_id,
                name=member.display_name,
                discord_id=member.id,
            )
        except RuntimeError as exc:
            await interaction.followup.send(f"Failed to add participant: {exc}", ephemeral=True)
            return

        display_name = participant.get("name") or member.display_name
        await interaction.followup.send(
            f"Added **{display_name}** to Challonge tournament `{tournament_id}`.",
            ephemeral=True,
        )

    @app_commands.command(
        name="challonge_add_role",
        description="Register every non-bot member of a Discord role to a Challonge tournament.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.describe(
        tournament_id="The Challonge tournament identifier (slug or ID).",
        role="Role whose members should be registered.",
    )
    async def challonge_add_role(
        self,
        interaction: discord.Interaction,
        tournament_id: str,
        role: discord.Role,
    ) -> None:
        members = [member for member in role.members if not member.bot]
        if not members:
            await interaction.response.send_message(
                "That role has no eligible members to register.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        successes: list[str] = []
        failures: list[str] = []

        for member in members:
            try:
                participant = await self._create_challonge_participant(
                    tournament_id,
                    name=member.display_name,
                    discord_id=member.id,
                )
            except RuntimeError as exc:
                failures.append(f"{member.display_name}: {exc}")
                continue

            successes.append(participant.get("name") or member.display_name)

        message_lines = [
            f"Registered {len(successes)} member(s) from {role.mention} to `{tournament_id}`.",
        ]
        if failures:
            message_lines.append("Some members could not be registered:")
            for failure in failures[:10]:
                message_lines.append(f"- {failure}")
            if len(failures) > 10:
                message_lines.append(
                    f"...and {len(failures) - 10} more failure(s). Check the logs for details."
                )

        await interaction.followup.send("\n".join(message_lines), ephemeral=True)

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