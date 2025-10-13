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

# Challonge exposes a handful of tournament states. "pending" means the
# tournament has been created but not started yet, while "underway" indicates
# that matches are currently being played. "awaiting_review" happens after play
# concludes but before the organizer finalizes results. All of these cases
# should be surfaced as "active" to Discord users, but only "pending"
# tournaments should be joinable. Previously we only accepted "pending" when
# fetching Challonge data, which meant ongoing events such as Swiss tournaments
# in the "underway" state were filtered out and never shown in
# `/tournament_view` or `/tournament_standings`.
ACTIVE_TOURNAMENT_STATES = {"pending", "underway", "awaiting_review"}
JOINABLE_TOURNAMENT_STATES = {"pending"}

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

    async def _fetch_challonge_tournament(
        self,
        tournament_id: str,
        *,
        include_participants: bool = False,
    ) -> dict:
        query = "?include_participants=1" if include_participants else ""
        response = await self._challonge_request(
            "GET", f"/tournaments/{tournament_id}.json{query}"
        )
        tournament = response.get("tournament", response)
        if isinstance(tournament, dict):
            return tournament
        return {}

    async def _fetch_challonge_participants(self, tournament_id: str) -> list[dict]:
        response = await self._challonge_request(
            "GET",
            f"/tournaments/{tournament_id}/participants.json",
        )

        if isinstance(response, list):
            raw_entries = response
        elif isinstance(response, dict):
            potential = response.get("participants") or response.get("data") or []
            raw_entries = potential if isinstance(potential, list) else []
        else:
            raw_entries = []

        participants: list[dict] = []
        for entry in raw_entries:
            participant = None
            if isinstance(entry, dict):
                potential = entry.get("participant")
                if isinstance(potential, dict):
                    participant = potential
                else:
                    participant = entry
            if isinstance(participant, dict):
                participants.append(participant)

        return participants

    def _extract_tournament_participants(self, tournament: dict) -> list[dict]:
        raw_entries = tournament.get("participants")
        if not isinstance(raw_entries, list):
            return []

        participants: list[dict] = []
        for entry in raw_entries:
            participant: dict | None = None
            if isinstance(entry, dict):
                potential = entry.get("participant")
                if isinstance(potential, dict):
                    participant = potential
                else:
                    participant = entry if isinstance(entry, dict) else None
            if participant is not None:
                participants.append(participant)
        return participants

    def _render_tournament_standings(
        self, tournament: dict
    ) -> tuple[list[str], str | None]:
        participants = self._extract_tournament_participants(tournament)
        name = tournament.get("name") or "Unnamed Tournament"
        state = (tournament.get("state") or "").replace("_", " ")
        state_display = state.title() if state else "Unknown"

        if not participants:
            return [], f"I couldn't find any participants for **{name}**."

        def _to_int(value: object) -> int | None:
            if value is None:
                return None
            try:
                number = int(value)
            except (TypeError, ValueError):
                return None
            return number if number > 0 else None

        def _sort_key(participant: dict) -> tuple[int, int, str]:
            final_rank = _to_int(participant.get("final_rank"))
            seed = _to_int(participant.get("seed"))
            name_key = (participant.get("display_name") or participant.get("name") or "").lower()
            return (
                final_rank if final_rank is not None else 10**9,
                seed if seed is not None else 10**9,
                name_key,
            )

        lines = [f"Standings for **{name}** ({state_display})"]
        for participant in sorted(participants, key=_sort_key):
            final_rank = _to_int(participant.get("final_rank"))
            seed = _to_int(participant.get("seed"))
            display_name = (
                participant.get("display_name")
                or participant.get("name")
                or participant.get("username")
                or f"Participant #{participant.get('id', '?')}"
            )

            rank_text = f"{final_rank}" if final_rank is not None else "—"
            seed_text = f"Seed {seed}" if seed is not None else "Seed ?"

            wins = _to_int(participant.get("matches_won"))
            losses = _to_int(participant.get("matches_lost"))
            ties = _to_int(participant.get("matches_tied"))
            status_bits: list[str] = []
            if wins is not None and losses is not None:
                record = f"{wins}-{losses}"
                if ties is not None and ties > 0:
                    record += f"-{ties}"
                status_bits.append(record)
            active_flag = participant.get("active")
            if isinstance(active_flag, bool) and not active_flag:
                status_bits.append("Dropped")

            status_text = f" — {', '.join(status_bits)}" if status_bits else ""
            lines.append(f"{rank_text:>3} | {display_name} ({seed_text}){status_text}")

        content = "\n".join(lines)
        chunks = _chunk_issue_messages("", [content], limit=2000)
        if not chunks:
            chunks = [content]
        return chunks, None

    async def _find_existing_challonge_participant(
        self,
        tournament_id: str,
        *,
        discord_id: int,
    ) -> dict | None:
        participants = await self._fetch_challonge_participants(tournament_id)
        target_id = str(discord_id)

        for participant in participants:
            misc = participant.get("misc")
            if isinstance(misc, str) and misc.strip() == target_id:
                return participant

        return None
    
    async def _fetch_active_tournaments(
        self, *, allowed_states: set[str] | None = None
    ) -> list[dict]:
        def _is_truthy(value: object) -> bool:
            if value is None:
                return False
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                return value != 0
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered in {"", "0", "false", "no", "off", "f", "null", "none"}:
                    return False
                if lowered in {"1", "true", "yes", "on", "t"}:
                    return True
            return True

        def _resolve_response_entries(response: object) -> list[dict]:
            if isinstance(response, list):
                raw_entries = response
            elif isinstance(response, dict):
                potential = response.get("tournaments") or response.get("data") or []
                raw_entries = potential if isinstance(potential, list) else []
            else:
                raw_entries = []

            resolved: list[dict] = []
            for entry in raw_entries:
                if not isinstance(entry, dict):
                    continue

                tournament = entry.get("tournament")
                if isinstance(tournament, dict):
                    resolved.append(tournament)
                    continue

                attributes = entry.get("attributes")
                if isinstance(attributes, dict):
                    merged: dict = {}
                    merged.update(attributes)
                    for key, value in entry.items():
                        if key in {"attributes", "relationships"}:
                            continue
                        merged.setdefault(key, value)
                    resolved.append(merged)
                    continue

                resolved.append(entry)
            return resolved

        def _resolve_response_entries(response: object) -> list[dict]:
            if isinstance(response, list):
                raw_entries = response
            elif isinstance(response, dict):
                potential = response.get("tournaments") or response.get("data") or []
                raw_entries = potential if isinstance(potential, list) else []
            else:
                raw_entries = []

            resolved: list[dict] = []
            for entry in raw_entries:
                if not isinstance(entry, dict):
                    continue

                tournament = entry.get("tournament")
                if isinstance(tournament, dict):
                    resolved.append(tournament)
                    continue

                attributes = entry.get("attributes")
                if isinstance(attributes, dict):
                    merged: dict = {}
                    merged.update(attributes)
                    for key, value in entry.items():
                        if key in {"attributes", "relationships"}:
                            continue
                        merged.setdefault(key, value)
                    resolved.append(merged)
                    continue

                resolved.append(entry)
            return resolved

        def _get_candidate_value(candidate: dict, key: str) -> object | None:
            if key in candidate:
                return candidate.get(key)

            attributes = candidate.get("attributes")
            if isinstance(attributes, dict):
                return attributes.get(key)
            return None
        
        response = await self._challonge_request("GET", "/tournaments.json")

        tournaments: list[dict] = []
        seen_identifiers: set[str] = set()
        valid_states = allowed_states or ACTIVE_TOURNAMENT_STATES
        for candidate in _resolve_response_entries(response):
            archived_flag = _get_candidate_value(candidate, "archived")
            archived_at = _get_candidate_value(candidate, "archived_at")
            hidden_flag = _get_candidate_value(candidate, "hidden")

            if (
                _is_truthy(archived_flag)
                or _is_truthy(archived_at)
                or _is_truthy(hidden_flag)
            ):
                continue

            normalized_state = (
                (_get_candidate_value(candidate, "state") or "").strip().lower()
            )
            if normalized_state not in valid_states:
                continue

            identifier = (
                _get_candidate_value(candidate, "id")
                or _get_candidate_value(candidate, "url")
                or _get_candidate_value(candidate, "slug")
                or _get_candidate_value(candidate, "full_challonge_url")
            )
            if not identifier:
                continue

            identifier_text = str(identifier)
            if identifier_text in seen_identifiers:
                continue

            seen_identifiers.add(identifier_text)
            tournaments.append(candidate)

        return tournaments

    def _resolve_tournament_identifier(self, tournament: dict) -> str | None:
        keys = ("id", "url", "slug")
        for key in keys:
            value = tournament.get(key)
            if value:
                return str(value)

        full_url = (tournament.get("full_challonge_url") or tournament.get("url"))
        if full_url:
            slug = full_url.rstrip("/").split("/")[-1]
            if slug:
                return slug

        return None

    @app_commands.command(
        name="tournament_create",
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
    async def tournament_create(
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
        name="tournament_join",
        description="Join an active Challonge tournament by submitting your deck.",
    )
    @app_commands.guilds(GUILD)
    async def tournament_join(self, interaction: discord.Interaction) -> None:
        try:
            tournaments = await self._fetch_active_tournaments(
                allowed_states=JOINABLE_TOURNAMENT_STATES
            )
        except RuntimeError as exc:
            await interaction.response.send_message(
                f"Failed to retrieve pending tournaments: {exc}",
                ephemeral=True,
            )
            return

        if not tournaments:
            await interaction.response.send_message(
                "There are no pending tournaments available to join right now.",
                ephemeral=True,
            )
            return

        # Deduplicate tournaments by resolved identifier while preserving order.
        seen_identifiers: set[str] = set()
        unique_tournaments: list[dict] = []
        for tournament in tournaments:
            identifier = self._resolve_tournament_identifier(tournament)
            if not identifier or identifier in seen_identifiers:
                continue
            seen_identifiers.add(identifier)
            unique_tournaments.append(tournament)

        if not unique_tournaments:
            await interaction.response.send_message(
                "I couldn't find any pending tournaments you can join right now.",
                ephemeral=True,
            )
            return

        if len(unique_tournaments) == 1:
            tournament = unique_tournaments[0]
            tournament_name = tournament.get("name") or "Unnamed Tournament"
            await interaction.response.send_message(
                f"Please check your DMs to submit your deck for **{tournament_name}**.",
                ephemeral=True,
            )
            await self._start_tournament_join_flow(interaction, tournament)
            return

        view = TournamentJoinSelectView(self, unique_tournaments)
        if not view.options_available():
            await interaction.response.send_message(
                "I couldn't find any pending tournaments you can join right now.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            "Select a pending tournament to join:",
            view=view,
            ephemeral=True,
        )

        try:
            view.message = await interaction.original_response()
        except Exception:
            self.logger.exception("Failed to capture tournament selection message")

    @app_commands.command(
        name="tournament_view",
        description="Show active Challonge tournaments and their brackets.",
    )
    @app_commands.guilds(GUILD)
    async def tournament_view(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        try:
            tournaments = await self._fetch_active_tournaments()
        except RuntimeError as exc:
            await interaction.followup.send(
                f"Failed to retrieve active tournaments: {exc}",
                ephemeral=True,
            )
            return

        seen_identifiers: set[str] = set()
        lines: list[str] = []

        for tournament in tournaments:
            identifier = self._resolve_tournament_identifier(tournament)
            if identifier and identifier in seen_identifiers:
                continue
            if identifier:
                seen_identifiers.add(identifier)

            name = tournament.get("name") or "Unnamed Tournament"
            url = tournament.get("full_challonge_url") or tournament.get("url")

            if not url:
                slug = tournament.get("slug") or identifier
                subdomain = tournament.get("subdomain")
                if slug:
                    slug_text = str(slug).strip("/")
                    if subdomain:
                        url = f"https://{subdomain}.challonge.com/{slug_text}"
                    else:
                        url = f"https://challonge.com/{slug_text}"

            if url:
                lines.append(f"• **{name}** — {url}")
            else:
                lines.append(f"• **{name}** — Bracket link unavailable")

        if not lines:
            await interaction.followup.send(
                "There are no active Challonge tournaments right now.",
                ephemeral=False,
            )
            return

        header = "Active Nemeses Tournaments"
        await interaction.followup.send("\n".join([header, *lines]))

    @app_commands.command(
        name="tournament_standings",
        description="View the standings for a Challonge tournament.",
    )
    @app_commands.guilds(GUILD)
    async def tournament_standings(self, interaction: discord.Interaction) -> None:
        try:
            tournaments = await self._fetch_active_tournaments()
        except RuntimeError as exc:
            await interaction.response.send_message(
                f"Failed to retrieve active tournaments: {exc}",
                ephemeral=True,
            )
            return

        seen_identifiers: set[str] = set()
        unique_tournaments: list[dict] = []
        for tournament in tournaments:
            identifier = self._resolve_tournament_identifier(tournament)
            if not identifier or identifier in seen_identifiers:
                continue
            seen_identifiers.add(identifier)
            unique_tournaments.append(tournament)

        if not unique_tournaments:
            await interaction.response.send_message(
                "There are no active Challonge tournaments right now.",
                ephemeral=True,
            )
            return

        view = TournamentStandingsSelectView(self, unique_tournaments)
        if not view.options_available():
            await interaction.response.send_message(
                "I couldn't find any tournaments with available standings right now.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            "Select a tournament to view standings:",
            view=view,
            ephemeral=True,
        )

        try:
            view.message = await interaction.original_response()
        except Exception:
            self.logger.exception(
                "Failed to capture tournament standings selection message"
            )

    @app_commands.command(
        name="tournament_add_participant",
        description="Register a Discord member into a Challonge tournament.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.describe(
        tournament_id="The Challonge tournament identifier (slug or ID).",
        member="Discord member to register.",
    )
    async def tournament_add_participant(
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

    @app_commands.command(
        name="challonge_shuffle_seeds",
        description="Shuffle the seeding for a pending single or double elimination tournament.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.describe(tournament_id="The Challonge tournament identifier (slug or ID).")
    async def challonge_shuffle_seeds(
        self,
        interaction: discord.Interaction,
        tournament_id: str,
    ) -> None:
        await interaction.response.defer(ephemeral=True)

        try:
            tournament = await self._fetch_challonge_tournament(tournament_id)
        except RuntimeError as exc:
            await interaction.followup.send(
                f"Failed to load tournament details: {exc}", ephemeral=True
            )
            return

        if not tournament:
            await interaction.followup.send(
                "I couldn't find that tournament on Challonge.", ephemeral=True
            )
            return

        tournament_type = (tournament.get("tournament_type") or "").strip().lower()
        if tournament_type not in {"single elimination", "double elimination"}:
            await interaction.followup.send(
                "Seeding can only be shuffled for single or double elimination tournaments.",
                ephemeral=True,
            )
            return

        state = (tournament.get("state") or "").strip().lower()
        if state not in {"pending", "checking_in"}:
            await interaction.followup.send(
                "Seeding can only be shuffled before the tournament begins.",
                ephemeral=True,
            )
            return

        try:
            await self._challonge_request(
                "POST",
                f"/tournaments/{tournament_id}/participants/randomize.json",
            )
        except RuntimeError as exc:
            await interaction.followup.send(
                f"Failed to shuffle seeds: {exc}", ephemeral=True
            )
            return

        name = tournament.get("name") or tournament_id
        await interaction.followup.send(
            f"Randomized seeding for **{name}**.",
            ephemeral=True,
        )

    @app_commands.command(
        name="challonge_drop_player",
        description="Drop or remove a participant from a Challonge tournament.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.describe(
        tournament_id="The Challonge tournament identifier (slug or ID).",
        player="Discord mention/ID, participant ID, or exact Challonge name.",
    )
    async def challonge_drop_player(
        self,
        interaction: discord.Interaction,
        tournament_id: str,
        player: str,
    ) -> None:
        token = (player or "").strip()
        if not token:
            await interaction.response.send_message(
                "You need to provide a participant to drop or remove.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        try:
            tournament = await self._fetch_challonge_tournament(
                tournament_id, include_participants=True
            )
        except RuntimeError as exc:
            await interaction.followup.send(
                f"Failed to load tournament details: {exc}", ephemeral=True
            )
            return

        if not tournament:
            await interaction.followup.send(
                "I couldn't find that tournament on Challonge.", ephemeral=True
            )
            return

        participants = self._extract_tournament_participants(tournament)
        if not participants:
            await interaction.followup.send(
                "That tournament has no registered participants.",
                ephemeral=True,
            )
            return

        mention_match = re.fullmatch(r"<@!?(\d+)>", token)
        numeric_tokens: set[str] = set()
        if mention_match:
            numeric_tokens.add(mention_match.group(1))
        if token.isdigit():
            numeric_tokens.add(token)
        lower_token = token.lower()

        matches: list[dict] = []
        for participant in participants:
            candidate_ids = {
                str(participant.get("id")) if participant.get("id") is not None else "",
                str(participant.get("seed")) if participant.get("seed") is not None else "",
                str(participant.get("misc")) if participant.get("misc") is not None else "",
            }
            candidate_ids = {value for value in candidate_ids if value}
            if numeric_tokens and candidate_ids.intersection(numeric_tokens):
                matches.append(participant)
                continue

            for key in ("display_name", "name", "username", "challonge_username"):
                value = participant.get(key)
                if isinstance(value, str) and value.strip().lower() == lower_token:
                    matches.append(participant)
                    break

        if not matches:
            await interaction.followup.send(
                "I couldn't find a participant matching that value.",
                ephemeral=True,
            )
            return

        if len(matches) > 1:
            preview = ", ".join(
                (match.get("display_name") or match.get("name") or str(match.get("id")) or "?")
                for match in matches[:5]
            )
            if len(matches) > 5:
                preview += ", ..."
            await interaction.followup.send(
                f"That reference matches multiple participants: {preview}. Please be more specific.",
                ephemeral=True,
            )
            return

        participant = matches[0]
        participant_id = participant.get("id")
        if participant_id is None:
            await interaction.followup.send(
                "I couldn't determine the participant's Challonge ID.",
                ephemeral=True,
            )
            return

        normalized_state = (tournament.get("state") or "").strip().lower()
        pending_states = {"pending", "checking_in"}
        completed_states = {"complete"}

        if normalized_state in completed_states:
            await interaction.followup.send(
                "The tournament is complete and participants can no longer be modified.",
                ephemeral=True,
            )
            return

        participant_name = (
            participant.get("display_name")
            or participant.get("name")
            or participant.get("username")
            or f"Participant #{participant_id}"
        )

        if normalized_state in pending_states:
            try:
                await self._challonge_request(
                    "DELETE",
                    f"/tournaments/{tournament_id}/participants/{participant_id}.json",
                )
            except RuntimeError as exc:
                await interaction.followup.send(
                    f"Failed to remove {participant_name}: {exc}",
                    ephemeral=True,
                )
                return

            await interaction.followup.send(
                f"Removed **{participant_name}** from `{tournament_id}`.",
                ephemeral=True,
            )
            return

        active_flag = participant.get("active")
        if isinstance(active_flag, bool) and not active_flag:
            await interaction.followup.send(
                f"**{participant_name}** is already dropped from `{tournament_id}`.",
                ephemeral=True,
            )
            return

        try:
            await self._challonge_request(
                "POST",
                f"/tournaments/{tournament_id}/participants/{participant_id}/mark_inactive.json",
            )
        except RuntimeError as exc:
            await interaction.followup.send(
                f"Failed to drop {participant_name}: {exc}",
                ephemeral=True,
            )
            return

        await interaction.followup.send(
            f"Dropped **{participant_name}** from `{tournament_id}`.",
            ephemeral=True,
        )

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

    async def _collect_deck_submission(
        self,
        *,
        interaction: discord.Interaction,
        dm_prompt: str,
        timeout: float = 300.0,
    ) -> tuple[discord.abc.Messageable, Counter[str], list[str], dict[str, list[str]]] | None:
        async def _notify_user(message: str) -> None:
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(message, ephemeral=True)
                else:
                    await interaction.response.send_message(message, ephemeral=True)
            except Exception:
                self.logger.exception("Failed to notify user about deck submission issue", exc_info=True)

        try:
            dm_channel = interaction.user.dm_channel or await interaction.user.create_dm()
        except discord.Forbidden:
            await _notify_user(
                "I couldn't send you a DM. Please enable direct messages and try again."
            )
            return None
        except Exception:
            self.logger.exception("Unexpected error creating DM channel")
            await _notify_user("Something went wrong while opening a DM with you. Please try again later.")
            return None

        try:
            await dm_channel.send(dm_prompt)
        except Exception:
            self.logger.exception("Failed to send deck submission prompt")
            await _notify_user("I couldn't send you a DM. Please try again later.")
            return None

        def check(message: discord.Message) -> bool:
            return message.author.id == interaction.user.id and message.channel == dm_channel

        try:
            submission: discord.Message = await self.bot.wait_for("message", timeout=timeout, check=check)
        except asyncio.TimeoutError:
            await dm_channel.send("Deck submission cancelled because no deck was received in time.")
            return None

        text: str | None = None

        if submission.attachments:
            attachment = submission.attachments[0]
            filename = (attachment.filename or "").lower()
            if not filename.endswith(".ydk"):
                await dm_channel.send("That file isn't a `.ydk` deck. Please try the command again with a valid file.")
                return None
            try:
                raw_bytes = await attachment.read()
            except Exception:
                self.logger.exception("Failed to read deck attachment")
                await dm_channel.send("I couldn't read that file. Please try again later.")
                return None
            text = raw_bytes.decode("utf-8-sig", errors="ignore")
        else:
            content = (submission.content or "").strip()
            if not content:
                await dm_channel.send("I didn't receive any deck information. Please run the command again.")
                return None
            if content.lower().startswith("ydke://"):
                text = _ydke_to_ydk_text(content)
                if text is None:
                    await dm_channel.send("That YDKE code couldn't be parsed. Please make sure it's a valid code.")
                    return None
            else:
                text = content

        card_counts, invalid_lines, deck_sections = _parse_ydk(text)
        if not card_counts:
            message = "I couldn't find any card IDs in that submission."
            if invalid_lines:
                example = invalid_lines[0]
                message += f" Example ignored line: `{example}`."
            await dm_channel.send(message)
            return None
        
        return dm_channel, card_counts, invalid_lines, deck_sections
    
    async def _start_tournament_join_flow(
        self,
        interaction: discord.Interaction,
        tournament: dict,
    ) -> None:
        tournament_name = tournament.get("name") or "Unnamed Tournament"
        identifier = self._resolve_tournament_identifier(tournament)
        existing_participant: dict | None = None

        if identifier:
            try:
                existing_participant = await self._find_existing_challonge_participant(
                    identifier,
                    discord_id=interaction.user.id,
                )
            except RuntimeError:
                self.logger.exception(
                    "Failed to determine if user is already registered for tournament",
                )

        is_resubmission = existing_participant is not None
        if is_resubmission:
            prompt = (
                f"You're already registered for **{tournament_name}**. Please attach a ydk file or copy and paste a ydke code to resubmit your deck."
            )
        else:
            prompt = (
                f"Please attach a ydk file or copy and paste a ydke code for the deck you plan to use in **{tournament_name}**."
            )
        result = await self._collect_deck_submission(interaction=interaction, dm_prompt=prompt)
        if not result:
            return

        dm_channel, card_counts, invalid_lines, deck_sections = result

        async with dm_channel.typing():
            deck_is_legal = await self._send_deck_results(
                dm_channel,
                interaction.user.id,
                card_counts,
                invalid_lines,
                deck_sections,
                success_message=(
                    f"Updated deck submission received for **{tournament_name}**. Here's your new decklist."
                    if is_resubmission
                    else (
                        f"Deck submission received for **{tournament_name}**. Here's the deck you'll be using in the tournament."
                    )
                ),
            )

        if not deck_is_legal:
            return

        if not identifier:
            await dm_channel.send(
                "I couldn't determine which Challonge tournament to register you for. Please contact a staff member."
            )
            return
        
        if is_resubmission:
            await dm_channel.send(
                f"Thanks for resubmitting your deck for **{tournament_name}**. I've recorded this updated list."
            )
            return

        try:
            participant = await self._create_challonge_participant(
                identifier,
                name=interaction.user.display_name,
                discord_id=interaction.user.id,
            )
        except RuntimeError as exc:
            await dm_channel.send(
                f"Your deck is legal, but I couldn't register you for **{tournament_name}**: {exc}"
            )
            return

        display_name = participant.get("name") or interaction.user.display_name
        await dm_channel.send(
            f"You're now registered for **{tournament_name}** on Challonge as **{display_name}**. Good luck!"
        )

    @app_commands.command(name="deck_check", description="Verify a YDK deck against your collection and the banlist")
    @app_commands.guilds(GUILD)
    async def deck_check(self, interaction: discord.Interaction):
        await interaction.response.send_message(
            "Please check DMs for deck check submission", ephemeral=True
        )

        result = await self._collect_deck_submission(
            interaction=interaction,
            dm_prompt="Please attach a ydk file or copy and paste a ydke code below",
        )
        if not result:
            return

        dm_channel, card_counts, invalid_lines, deck_sections = result

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
        *,
        success_message: str | None = None,
    ) -> bool:
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
            return False

        await channel.send(success_message or "Your deck is perfectly legal.")

        await self._send_deck_images(channel, deck_sections, metadata)

        return True

class TournamentJoinSelect(discord.ui.Select):
    def __init__(self, view: "TournamentJoinSelectView", options: list[discord.SelectOption]):
        super().__init__(
            placeholder="Select a tournament…",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.parent_view = view

    async def callback(self, interaction: discord.Interaction) -> None:
        selected_value = self.values[0]
        tournament = self.parent_view.tournament_map.get(selected_value)
        if not tournament:
            await interaction.response.send_message(
                "The selected tournament is no longer available.",
                ephemeral=True,
            )
            return

        self.parent_view.disable_all_items()
        self.parent_view.stop()
        tournament_name = tournament.get("name") or "Unnamed Tournament"

        await interaction.response.edit_message(
            content=(
                f"You selected **{tournament_name}**. Please check your DMs to submit your deck."
            ),
            view=self.parent_view,
        )

        async def runner() -> None:
            try:
                await self.parent_view.cog._start_tournament_join_flow(interaction, tournament)
            except Exception:
                self.parent_view.cog.logger.exception("Error while processing tournament join request")

        asyncio.create_task(runner())


class TournamentJoinSelectView(discord.ui.View):
    def __init__(self, cog: Tournaments, tournaments: list[dict]):
        super().__init__(timeout=60)
        self.cog = cog
        self.message: discord.Message | None = None
        self.tournament_map: dict[str, dict] = {}

        options: list[discord.SelectOption] = []
        for tournament in tournaments:
            identifier = cog._resolve_tournament_identifier(tournament)
            if not identifier or identifier in self.tournament_map:
                continue

            name = (tournament.get("name") or "Unnamed Tournament")[:100]
            state = (tournament.get("state") or "").replace("_", " ").title() or "Pending"
            start_at = tournament.get("start_at") or tournament.get("started_at")
            description_parts = [state]
            if start_at:
                description_parts.append(f"Start: {start_at}")
            description = " • ".join(description_parts)

            options.append(
                discord.SelectOption(
                    label=name,
                    description=description[:100] if description else None,
                    value=identifier,
                )
            )
            self.tournament_map[identifier] = tournament

            if len(options) >= 25:
                break

        self.select: TournamentJoinSelect | None = None
        if options:
            self.select = TournamentJoinSelect(self, options)
            self.add_item(self.select)

    def options_available(self) -> bool:
        return bool(self.tournament_map)

    def disable_all_items(self) -> None:
        for child in self.children:
            if hasattr(child, "disabled"):
                child.disabled = True

    async def on_timeout(self) -> None:
        if not self.message:
            return
        self.disable_all_items()
        try:
            await self.message.edit(
                content="Tournament selection timed out.",
                view=self,
            )
        except Exception:
            self.cog.logger.exception("Failed to update tournament selection message on timeout")

class TournamentStandingsSelect(discord.ui.Select):
    def __init__(
        self, view: "TournamentStandingsSelectView", options: list[discord.SelectOption]
    ):
        super().__init__(
            placeholder="Select a tournament…",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.parent_view = view

    async def callback(self, interaction: discord.Interaction) -> None:
        selected_value = self.values[0]
        tournament = self.parent_view.tournament_map.get(selected_value)
        if not tournament:
            await interaction.response.send_message(
                "The selected tournament is no longer available.",
                ephemeral=True,
            )
            return

        self.parent_view.disable_all_items()
        self.parent_view.stop()
        tournament_name = tournament.get("name") or "Unnamed Tournament"

        await interaction.response.edit_message(
            content=f"Generating standings for **{tournament_name}**…",
            view=self.parent_view,
        )

        async def runner() -> None:
            try:
                await self.parent_view.show_standings(interaction, tournament, selected_value)
            except Exception:
                self.parent_view.cog.logger.exception(
                    "Error while preparing tournament standings"
                )
                try:
                    await interaction.followup.send(
                        "Failed to load standings due to an unexpected error.",
                        ephemeral=True,
                    )
                except Exception:
                    self.parent_view.cog.logger.exception(
                        "Failed to send tournament standings error message"
                    )

        asyncio.create_task(runner())


class TournamentStandingsSelectView(discord.ui.View):
    def __init__(self, cog: Tournaments, tournaments: list[dict]):
        super().__init__(timeout=60)
        self.cog = cog
        self.message: discord.Message | None = None
        self.tournament_map: dict[str, dict] = {}

        options: list[discord.SelectOption] = []
        for tournament in tournaments:
            identifier = cog._resolve_tournament_identifier(tournament)
            if not identifier or identifier in self.tournament_map:
                continue

            name = (tournament.get("name") or "Unnamed Tournament")[:100]
            state = (tournament.get("state") or "").replace("_", " ").title() or "Pending"
            start_at = tournament.get("start_at") or tournament.get("started_at")
            description_parts = [state]
            if start_at:
                description_parts.append(f"Start: {start_at}")
            description = " • ".join(description_parts)

            options.append(
                discord.SelectOption(
                    label=name,
                    description=description[:100] if description else None,
                    value=identifier,
                )
            )
            self.tournament_map[identifier] = tournament

            if len(options) >= 25:
                break

        self.select: TournamentStandingsSelect | None = None
        if options:
            self.select = TournamentStandingsSelect(self, options)
            self.add_item(self.select)

    def options_available(self) -> bool:
        return bool(self.tournament_map)

    def disable_all_items(self) -> None:
        for child in self.children:
            if hasattr(child, "disabled"):
                child.disabled = True

    async def show_standings(
        self,
        interaction: discord.Interaction,
        tournament: dict,
        identifier: str,
    ) -> None:
        resolved_identifier = self.cog._resolve_tournament_identifier(tournament) or identifier
        if not resolved_identifier:
            await interaction.followup.send(
                "I couldn't determine the identifier for that tournament.",
                ephemeral=True,
            )
            return

        try:
            detailed = await self.cog._fetch_challonge_tournament(
                resolved_identifier,
                include_participants=True,
            )
        except RuntimeError as exc:
            await interaction.followup.send(
                f"Failed to retrieve tournament: {exc}",
                ephemeral=True,
            )
            return

        if not detailed:
            await interaction.followup.send(
                "I couldn't find that tournament on Challonge.",
                ephemeral=True,
            )
            return

        chunks, error_message = self.cog._render_tournament_standings(detailed)
        if error_message:
            await interaction.followup.send(error_message, ephemeral=True)
            return

        if not chunks:
            await interaction.followup.send(
                "No standings are available for that tournament yet.",
                ephemeral=True,
            )
            return

        for chunk in chunks:
            await interaction.followup.send(chunk, ephemeral=True)

    async def on_timeout(self) -> None:
        if not self.message:
            return
        self.disable_all_items()
        try:
            await self.message.edit(
                content="Tournament selection timed out.",
                view=self,
            )
        except Exception:
            self.cog.logger.exception(
                "Failed to update tournament standings selection message on timeout"
            )

async def setup(bot: commands.Bot):
    await bot.add_cog(Tournaments(bot))