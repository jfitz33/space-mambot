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
from datetime import datetime

import discord
from discord import app_commands
from discord.ext import commands
import requests

from core.banlist import load_banlist
from core.cards_shop import fetch_card_details_by_id, find_card_name_by_id
from core.deck_render import DeckCardEntry, render_deck_section_image
from core.db import db_get_collection, db_stats_record_loss, db_stats_revert_result
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
DROP_ELIGIBLE_TOURNAMENT_STATES = {"pending", "checking_in"}
DROP_DISCOVERY_TOURNAMENT_STATES = (
    ACTIVE_TOURNAMENT_STATES | DROP_ELIGIBLE_TOURNAMENT_STATES
)


def _parse_challonge_timestamp(value: object) -> float:
    """Return a comparable timestamp value from Challonge API fields."""

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return 0.0

        normalized = text.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized).timestamp()
        except ValueError:
            try:
                return float(text)
            except ValueError:
                return 0.0

    return 0.0

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


@dataclass(slots=True)
class TournamentDropEntry:
    identifier: str
    tournament: dict
    participant_id: str

    def display_name(self) -> str:
        if isinstance(self.tournament, dict):
            name = self.tournament.get("name")
            if isinstance(name, str):
                cleaned = name.strip()
                if cleaned:
                    return cleaned
        return self.identifier


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
    def _win_pct(stats: dict) -> float:
        games = int(stats.get("games", 0) or 0)
        wins = int(stats.get("wins", 0) or 0)
        return (wins / games * 100.0) if games else 0.0

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
        api_base: str | None = None,
    ) -> dict:
        username, api_key = self._get_challonge_credentials()
        base_url = api_base or os.getenv(
            "CHALLONGE_API_BASE", "https://api.challonge.com/v1"
        )
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

    async def _fetch_challonge_matches(self, tournament_id: str) -> list[dict]:
        response = await self._challonge_request(
            "GET",
            f"/tournaments/{tournament_id}/matches.json",
        )

        if isinstance(response, list):
            raw_entries = response
        elif isinstance(response, dict):
            potential = response.get("matches") or response.get("data") or []
            raw_entries = potential if isinstance(potential, list) else []
        else:
            raw_entries = []

        matches: list[dict] = []
        for entry in raw_entries:
            match = None
            if isinstance(entry, dict):
                potential = entry.get("match")
                if isinstance(potential, dict):
                    match = potential
                else:
                    match = entry
            if isinstance(match, dict):
                matches.append(match)

        return matches

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

    def _find_matching_participant(
        self, participants: list[dict], user: "discord.abc.User"
    ) -> dict | None:
        user_id = getattr(user, "id", None)
        if user_id is None:
            return None

        user_id_str = str(user_id)
        name_candidates = {
            getattr(user, "display_name", None),
            getattr(user, "global_name", None),
            getattr(user, "name", None),
            getattr(user, "nick", None),
        }
        normalized_names = {
            value.strip().lower()
            for value in name_candidates
            if isinstance(value, str) and value.strip()
        }

        for entry in participants:
            if not isinstance(entry, dict):
                continue

            misc = entry.get("misc")
            if isinstance(misc, str) and misc.strip() == user_id_str:
                return entry

            for key in ("display_name", "name", "username", "challonge_username"):
                value = entry.get(key)
                if not isinstance(value, str):
                    continue
                cleaned = value.strip().lower()
                if cleaned and cleaned in normalized_names:
                    return entry

        return None

    async def _resolve_open_match_context(
        self,
        tournaments: list[dict],
        *,
        loser: discord.Member,
        winner: discord.Member,
    ) -> tuple[str, str, str, str, str] | None:
        """Locate an open Challonge match between ``loser`` and ``winner``."""

        for tournament in tournaments:
            identifier = self._resolve_tournament_identifier(tournament)
            if not identifier:
                continue

            try:
                detailed = await self._fetch_challonge_tournament(
                    identifier, include_participants=True
                )
            except RuntimeError:
                continue

            participants = self._extract_tournament_participants(detailed)
            if not participants:
                continue

            loser_participant = self._find_matching_participant(participants, loser)
            winner_participant = self._find_matching_participant(participants, winner)
            if loser_participant is None or winner_participant is None:
                continue

            loser_participant_id = loser_participant.get("id")
            winner_participant_id = winner_participant.get("id")
            if loser_participant_id is None or winner_participant_id is None:
                continue

            loser_id_str = str(loser_participant_id)
            winner_id_str = str(winner_participant_id)

            try:
                matches = await self._fetch_challonge_matches(identifier)
            except RuntimeError:
                continue

            for match in matches:
                match_state = (match.get("state") or "").strip().lower()
                if match_state != "open":
                    continue

                player1_id = match.get("player1_id")
                player2_id = match.get("player2_id")
                if player1_id is None or player2_id is None:
                    continue

                player_ids = {str(player1_id), str(player2_id)}
                if player_ids != {loser_id_str, winner_id_str}:
                    continue

                match_id = match.get("id")
                if match_id is None:
                    continue

                tournament_name = (
                    detailed.get("name")
                    or tournament.get("name")
                    or str(identifier)
                )

                return (
                    str(identifier),
                    str(match_id),
                    winner_id_str,
                    loser_id_str,
                    str(tournament_name),
                )

        return None

    async def _finalize_tournament_loss(
        self,
        interaction: discord.Interaction,
        *,
        tournament_identifier: str,
        match_id: str,
        winner_id_str: str,
        loser_id_str: str,
        loser: discord.Member,
        winner: discord.Member,
        quest_context: str,
        reporter: discord.abc.User | None,
        tournament_name: str | None = None,
        announce_publicly: bool = True,
    ) -> bool:
        try:
            await self._challonge_request(
                "PUT",
                f"/tournaments/{tournament_identifier}/matches/{match_id}.json",
                data={
                    "match[scores_csv]": "0-1",
                    "match[winner_id]": winner_id_str,
                    "match[loser_id]": loser_id_str,
                },
            )
        except RuntimeError as exc:
            await interaction.followup.send(
                f"Failed to update Challonge match: {exc}",
                ephemeral=not announce_publicly,
            )
            return False

        db_stats_record_loss(
            self.state,
            loser_id=loser.id,
            winner_id=winner.id,
        )

        quests = interaction.client.get_cog("Quests")
        try:
            if quests and getattr(quests, "qm", None):
                await quests.qm.increment(winner.id, "win_3_matches", 1)
                await quests.qm.increment(loser.id, "matches_played", 1)
                await quests.qm.increment(winner.id, "matches_played", 1)
        except Exception:
            self.logger.exception(
                f"[tournaments] quest tick error during {quest_context}"
            )

        tournament_fragment = (
            f" in **{tournament_name}**" if tournament_name else ""
        )
        reporter_note = ""
        if reporter and reporter.id not in {loser.id, winner.id}:
            reporter_note = f" (reported by {getattr(reporter, 'display_name', 'an admin')})"

        await interaction.followup.send(
            (
                f"Tournament result of {loser.display_name}'s loss to {winner.display_name} "
                f"has been recorded{tournament_fragment}.{reporter_note}"
            ).strip(),
            ephemeral=not announce_publicly,
        )

        return True

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

    async def _drop_tournament_participant(
        self, tournament_id: str, participant_id: str
    ) -> None:
        await self._challonge_request(
            "DELETE",
            f"/tournaments/{tournament_id}/participants/{participant_id}.json",
        )

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
        name="tournament_loss",
        description="Report a Challonge tournament loss to another player.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.describe(opponent="The player you lost to")
    async def tournament_loss(
        self, interaction: discord.Interaction, opponent: discord.Member
    ) -> None:
        caller = interaction.user
        if opponent.id == caller.id:
            await interaction.response.send_message(
                "You can’t record a loss to yourself.",
                ephemeral=True,
            )
            return
        if opponent.bot:
            await interaction.response.send_message(
                "You can’t record a loss to a bot.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=False, thinking=True)

        try:
            tournaments = await self._fetch_active_tournaments()
        except RuntimeError as exc:
            await interaction.followup.send(
                f"Failed to retrieve active tournaments: {exc}",
                ephemeral=False,
            )
            return

        match_context = await self._resolve_open_match_context(
            tournaments,
            loser=caller,
            winner=opponent,
        )

        if match_context is None:
            await interaction.followup.send(
                f"no active tournament match between {caller.display_name} and {opponent.display_name}",
                ephemeral=False,
            )
            return

        tournament_identifier, match_id, winner_id_str, loser_id_str, tournament_name = (
            match_context
        )

        await self._finalize_tournament_loss(
            interaction,
            tournament_identifier=tournament_identifier,
            match_id=match_id,
            winner_id_str=winner_id_str,
            loser_id_str=loser_id_str,
            loser=caller,
            winner=opponent,
            quest_context="tournament_loss",
            reporter=caller,
            tournament_name=tournament_name,
            announce_publicly=True,
        )

    @app_commands.command(
        name="tournament_admin_loss",
        description="(Admin) Report a Challonge tournament loss between two players.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        loser="Player who lost the match",
        winner="Player who won the match",
    )
    async def tournament_admin_loss(
        self,
        interaction: discord.Interaction,
        loser: discord.Member,
        winner: discord.Member,
    ) -> None:
        if loser.id == winner.id:
            await interaction.response.send_message(
                "You must choose two different players.",
                ephemeral=True,
            )
            return
        if loser.bot or winner.bot:
            await interaction.response.send_message(
                "Bots cannot play matches.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=False, thinking=True)

        try:
            tournaments = await self._fetch_active_tournaments()
        except RuntimeError as exc:
            await interaction.followup.send(
                f"Failed to retrieve active tournaments: {exc}",
                ephemeral=False,
            )
            return

        match_context = await self._resolve_open_match_context(
            tournaments,
            loser=loser,
            winner=winner,
        )

        if match_context is None:
            await interaction.followup.send(
                f"no active tournament match between {loser.display_name} and {winner.display_name}",
                ephemeral=False,
            )
            return

        tournament_identifier, match_id, winner_id_str, loser_id_str, tournament_name = (
            match_context
        )

        await self._finalize_tournament_loss(
            interaction,
            tournament_identifier=tournament_identifier,
            match_id=match_id,
            winner_id_str=winner_id_str,
            loser_id_str=loser_id_str,
            loser=loser,
            winner=winner,
            quest_context="tournament_admin_loss",
            reporter=interaction.user,
            tournament_name=tournament_name,
            announce_publicly=True,
        )

    @app_commands.command(
        name="tournament_revert_result",
        description="(Admin) Reopen the most recent Challonge result between two players.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        loser="Player originally recorded as the loser",
        winner="Player originally recorded as the winner",
    )
    async def tournament_revert_result(
        self,
        interaction: discord.Interaction,
        loser: discord.Member,
        winner: discord.Member,
    ) -> None:
        if loser.id == winner.id:
            await interaction.response.send_message(
                "You must choose two different players.",
                ephemeral=True,
            )
            return
        if loser.bot or winner.bot:
            await interaction.response.send_message(
                "Bots cannot play matches.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            tournaments = await self._fetch_active_tournaments()
        except RuntimeError as exc:
            await interaction.followup.send(
                f"Failed to retrieve active tournaments: {exc}",
                ephemeral=True,
            )
            return

        best_match: tuple[
            float,
            str,
            str,
            str,
            str,
            str,
        ] | None = None

        for tournament in tournaments:
            identifier = self._resolve_tournament_identifier(tournament)
            if not identifier:
                continue

            try:
                detailed = await self._fetch_challonge_tournament(
                    identifier, include_participants=True
                )
            except RuntimeError:
                continue

            participants = self._extract_tournament_participants(detailed)
            if not participants:
                continue

            loser_participant = self._find_matching_participant(participants, loser)
            winner_participant = self._find_matching_participant(participants, winner)
            if loser_participant is None or winner_participant is None:
                continue

            loser_participant_id = loser_participant.get("id")
            winner_participant_id = winner_participant.get("id")
            if loser_participant_id is None or winner_participant_id is None:
                continue

            loser_id_str = str(loser_participant_id)
            winner_id_str = str(winner_participant_id)

            try:
                matches = await self._fetch_challonge_matches(identifier)
            except RuntimeError:
                continue

            for match in matches:
                match_state = (match.get("state") or "").strip().lower()
                if match_state not in {"complete", "awaiting_review"}:
                    continue

                match_winner = match.get("winner_id")
                match_loser = match.get("loser_id")
                if match_winner is None or match_loser is None:
                    continue

                if str(match_winner) != winner_id_str or str(match_loser) != loser_id_str:
                    continue

                match_id = match.get("id")
                if match_id is None:
                    continue

                timestamp = max(
                    _parse_challonge_timestamp(match.get("completed_at")),
                    _parse_challonge_timestamp(match.get("updated_at")),
                    _parse_challonge_timestamp(match.get("started_at")),
                    _parse_challonge_timestamp(match.get("created_at")),
                )

                tournament_name = (
                    detailed.get("name")
                    or tournament.get("name")
                    or str(identifier)
                )

                context = (
                    timestamp,
                    str(identifier),
                    str(match_id),
                    winner_id_str,
                    loser_id_str,
                    str(tournament_name),
                )

                if best_match is None or timestamp > best_match[0]:
                    best_match = context

        if best_match is None:
            await interaction.followup.send(
                f"❌ No completed Challonge match found between {loser.display_name} and {winner.display_name}.",
                ephemeral=True,
            )
            return

        _, tournament_identifier, match_id, winner_id_str, loser_id_str, tournament_name = best_match

        loser_after, winner_after = db_stats_revert_result(
            self.state,
            loser_id=loser.id,
            winner_id=winner.id,
        )

        if loser_after is None or winner_after is None:
            await interaction.followup.send(
                "❌ No recorded result found for that matchup to revert.",
                ephemeral=True,
            )
            return

        reopen_error: RuntimeError | None = None
        try:
            await self._challonge_request(
                "POST",
                f"/tournaments/{tournament_identifier}/matches/{match_id}/reopen.json",
            )
        except RuntimeError as exc:
            reopen_error = exc

        if reopen_error is not None:
            try:
                await self._challonge_request(
                    "POST",
                    f"/tournaments/{tournament_identifier}/matches/{match_id}/reset.json",
                )
            except RuntimeError as reset_exc:
                db_stats_record_loss(
                    self.state,
                    loser_id=loser.id,
                    winner_id=winner.id,
                )

                message = (
                    "Failed to reopen Challonge match and restored the local result. "
                    f"Challonge error: {reset_exc}"
                )
                if reopen_error is not None:
                    message += f" (Reopen error: {reopen_error})"

                await interaction.followup.send(message, ephemeral=True)
                return

            self.logger.warning(
                "Challonge reopen failed for %s (match %s); used reset endpoint instead: %s",
                tournament_identifier,
                match_id,
                reopen_error,
            )

        quests = interaction.client.get_cog("Quests")
        try:
            if quests and getattr(quests, "qm", None):
                await quests.qm.increment(winner.id, "win_3_matches", -1)
                await quests.qm.increment(loser.id, "matches_played", -1)
                await quests.qm.increment(winner.id, "matches_played", -1)
        except Exception:
            self.logger.exception(
                "[tournaments] quest tick error during tournament_revert_result"
            )

        loser_pct = self._win_pct(loser_after)
        winner_pct = self._win_pct(winner_after)

        embed = discord.Embed(
            title="Tournament Result Reverted",
            description=(
                f"Reopened the Challonge result in **{tournament_name}** where "
                f"**{loser.display_name}** lost to **{winner.display_name}**."
            ),
            color=0x2F855A,
        )
        embed.add_field(
            name=f"{loser.display_name} — Record",
            value=(
                f"W: **{loser_after['wins']}**\n"
                f"L: **{loser_after['losses']}**\n"
                f"Win%: **{loser_pct:.1f}%**"
            ),
            inline=True,
        )
        embed.add_field(
            name=f"{winner.display_name} — Record",
            value=(
                f"W: **{winner_after['wins']}**\n"
                f"L: **{winner_after['losses']}**\n"
                f"Win%: **{winner_pct:.1f}%**"
            ),
            inline=True,
        )

        await interaction.followup.send(embed=embed, ephemeral=True)

        if interaction.channel:
            await interaction.channel.send(
                "↩️ Admin reopened a Challonge result"
                f" ({tournament_name}): removed the loss for "
                f"**{loser.display_name}** vs **{winner.display_name}**."
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
        name="tournament_shuffle_seeds",
        description="Shuffle the seeding for a pending single or double elimination tournament.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.describe(tournament_id="The Challonge tournament identifier (slug or ID).")
    async def tournament_shuffle_seeds(
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
        name="tournament_drop",
        description="Drop yourself from a Challonge tournament.",
    )
    @app_commands.guilds(GUILD)
    async def tournament_drop(
        self,
        interaction: discord.Interaction,
    ) -> None:

        await interaction.response.defer(ephemeral=True)

        try:
            tournaments = await self._fetch_active_tournaments(
                allowed_states=DROP_DISCOVERY_TOURNAMENT_STATES
            )
        except RuntimeError as exc:
            await interaction.followup.send(
                f"Failed to load tournament details: {exc}", ephemeral=True
            )
            return

        if not tournaments:
            await interaction.followup.send(
                "I couldn't find any active tournaments.", ephemeral=True
            )
            return

        def _resolve_name(data: dict, fallback: str) -> str:
            if isinstance(data, dict):
                name = data.get("name")
                if isinstance(name, str):
                    cleaned = name.strip()
                    if cleaned:
                        return cleaned
            return fallback

        def _format_list(values: list[str]) -> str:
            return ", ".join(f"**{value}**" for value in values)

        eligible_entries: list[TournamentDropEntry] = []
        manual_entries: list[str] = []
        missing_id_entries: list[str] = []
        seen_identifiers: set[str] = set()

        for candidate in tournaments:
            if not isinstance(candidate, dict):
                continue

            identifier = self._resolve_tournament_identifier(candidate)
            if not identifier or identifier in seen_identifiers:
                continue
            seen_identifiers.add(identifier)

            try:
                detailed = await self._fetch_challonge_tournament(
                    identifier, include_participants=True
                )
            except RuntimeError:
                continue

            if not detailed:
                continue

            participants = self._extract_tournament_participants(detailed)
            if not participants:
                continue

            participant = self._find_matching_participant(
                participants, interaction.user
            )
            if participant is None:
                continue

            normalized_state = (detailed.get("state") or "").strip().lower()
            display_name = _resolve_name(detailed, identifier)

            if normalized_state not in DROP_ELIGIBLE_TOURNAMENT_STATES:
                if display_name not in manual_entries:
                    manual_entries.append(display_name)
                continue

            participant_id = participant.get("id")
            if participant_id is None:
                if display_name not in missing_id_entries:
                    missing_id_entries.append(display_name)
                continue

            eligible_entries.append(
                TournamentDropEntry(
                    identifier=identifier,
                    tournament=detailed,
                    participant_id=str(participant_id),
                )
            )

        if not eligible_entries:
            if manual_entries or missing_id_entries:
                message_parts: list[str] = []
                if manual_entries:
                    message_parts.append(
                        "Drops during an active tournament must be done manually. "
                        "Ensure all remaining matches are reported and ask an admin to drop you from "
                        f"{_format_list(manual_entries)}."
                    )
                if missing_id_entries:
                    message_parts.append(
                        "I couldn't determine your Challonge participant ID for "
                        f"{_format_list(missing_id_entries)}. Please ask an admin for assistance."
                    )
                await interaction.followup.send(
                    "\n\n".join(message_parts),
                    ephemeral=True,
                )
                return

            await interaction.followup.send(
                "I couldn't find any active tournaments you're registered in.",
                ephemeral=True,
            )
            return

        if len(eligible_entries) == 1:
            entry = eligible_entries[0]
            tournament_name = entry.display_name()

            try:
                await self._drop_tournament_participant(
                    entry.identifier, entry.participant_id
                )
            except RuntimeError as exc:
                message = (
                    f"Failed to drop you from **{tournament_name}**: {exc}"
                )
                if manual_entries:
                    message += (
                        "\n\nDrops during an active tournament must be done manually. "
                        "Ensure all remaining matches are reported and ask an admin to drop you from "
                        f"{_format_list(manual_entries)}."
                    )
                if missing_id_entries:
                    message += (
                        "\n\nI couldn't determine your Challonge participant ID for "
                        f"{_format_list(missing_id_entries)}. Please ask an admin for assistance."
                    )
                await interaction.followup.send(message, ephemeral=True)
                return

            message = f"Removed you from **{tournament_name}**."
            if manual_entries:
                message += (
                    "\n\nDrops during an active tournament must be done manually. "
                    "Ensure all remaining matches are reported and ask an admin to drop you from "
                    f"{_format_list(manual_entries)}."
                )
            if missing_id_entries:
                message += (
                    "\n\nI couldn't determine your Challonge participant ID for "
                    f"{_format_list(missing_id_entries)}. Please ask an admin for assistance."
                )
            await interaction.followup.send(message, ephemeral=True)
            return

        notes_sections: list[str] = []
        if manual_entries:
            notes_sections.append(
                "Drops during an active tournament must be done manually. "
                "Ensure all remaining matches are reported and ask an admin to drop you from "
                f"{_format_list(manual_entries)}."
            )
        if missing_id_entries:
            notes_sections.append(
                "I couldn't determine your Challonge participant ID for "
                f"{_format_list(missing_id_entries)}. Please ask an admin for assistance."
            )

        notes_text = "\n\n".join(notes_sections) if notes_sections else None

        view = TournamentDropSelectView(self, eligible_entries, notes=notes_text)
        if not view.options_available():
            await interaction.followup.send(
                "I couldn't find any tournaments you're able to drop from right now.",
                ephemeral=True,
            )
            return

        base_prompt = "Select the tournament you want to drop from:"
        initial_message = (
            f"{base_prompt}\n\n{notes_text}" if notes_text else base_prompt
        )

        view.message = await interaction.followup.send(
            initial_message,
            view=view,
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

        tin_promos_by_id: dict[str, str] = {}
        for tin in (getattr(self.state, "tins_index", {}) or {}).values():
            for card in tin.get("promo_cards") or []:
                name = (card.get("name") or card.get("cardname") or "").strip()
                cid = _normalize_card_id(card.get("id") or card.get("cardid"))
                if cid and name:
                    tin_promos_by_id.setdefault(cid, name)

        for cid in card_ids:
            meta = CardMetadata()
            local_name = find_card_name_by_id(self.state, cid)
            if not local_name:
                local_name = owned_name_by_id.get(cid)
            if not local_name:
                local_name = tin_promos_by_id.get(cid)
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


class TournamentDropSelect(discord.ui.Select):
    def __init__(
        self, view: "TournamentDropSelectView", options: list[discord.SelectOption]
    ) -> None:
        super().__init__(
            placeholder="Select a tournament…",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.parent_view = view

    async def callback(self, interaction: discord.Interaction) -> None:
        selected_value = self.values[0]
        entry = self.parent_view.entry_map.get(selected_value)
        if not entry:
            await interaction.response.send_message(
                "The selected tournament is no longer available.",
                ephemeral=True,
            )
            return

        self.parent_view.disable_all_items()
        self.parent_view.stop()

        tournament_name = entry.display_name()

        try:
            await interaction.response.edit_message(
                content=f"Removing you from **{tournament_name}**…",
                view=self.parent_view,
            )
        except Exception:
            self.parent_view.cog.logger.exception(
                "Failed to update tournament drop selection message"
            )

        async def runner() -> None:
            try:
                await self.parent_view.cog._drop_tournament_participant(
                    entry.identifier, entry.participant_id
                )
            except RuntimeError as exc:
                content = (
                    f"Failed to drop you from **{tournament_name}**: {exc}"
                )
                updated = await self.parent_view.update_message_content(content)
                if not updated:
                    try:
                        await interaction.followup.send(
                            content,
                            ephemeral=True,
                        )
                    except Exception:
                        self.parent_view.cog.logger.exception(
                            "Failed to send tournament drop error message"
                        )
                return
            except Exception:
                self.parent_view.cog.logger.exception(
                    "Unexpected error while dropping tournament participant"
                )
                fallback = (
                    "Failed to drop you from the tournament due to an unexpected error."
                )
                updated = await self.parent_view.update_message_content(fallback)
                if not updated:
                    try:
                        await interaction.followup.send(
                            fallback,
                            ephemeral=True,
                        )
                    except Exception:
                        self.parent_view.cog.logger.exception(
                            "Failed to send tournament drop fallback message"
                        )
                return

            success_message = (
                f"You have been removed from **{tournament_name}**."
            )
            updated = await self.parent_view.update_message_content(success_message)
            if not updated:
                try:
                    await interaction.followup.send(
                        success_message,
                        ephemeral=True,
                    )
                except Exception:
                    self.parent_view.cog.logger.exception(
                        "Failed to send tournament drop confirmation"
                    )

        asyncio.create_task(runner())


class TournamentDropSelectView(discord.ui.View):
    def __init__(
        self,
        cog: "Tournaments",
        entries: list[TournamentDropEntry],
        *,
        notes: str | None = None,
    ):
        super().__init__(timeout=60)
        self.cog = cog
        self.message: discord.Message | None = None
        self.entry_map: dict[str, TournamentDropEntry] = {}
        self.notes = notes

        options: list[discord.SelectOption] = []
        for entry in entries:
            identifier = entry.identifier
            if identifier in self.entry_map:
                continue

            tournament = entry.tournament if isinstance(entry.tournament, dict) else {}
            name = entry.display_name()[:100]
            state = (tournament.get("state") or "").replace("_", " ").title()
            start_at = tournament.get("start_at") or tournament.get("started_at")
            description_parts = []
            if state:
                description_parts.append(state)
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
            self.entry_map[identifier] = entry

            if len(options) >= 25:
                break

        self.select: TournamentDropSelect | None = None
        if options:
            self.select = TournamentDropSelect(self, options)
            self.add_item(self.select)

    def options_available(self) -> bool:
        return bool(self.entry_map)

    def disable_all_items(self) -> None:
        for child in self.children:
            if hasattr(child, "disabled"):
                child.disabled = True

    async def update_message_content(
        self, content: str, *, include_notes: bool = True
    ) -> bool:
        if not self.message:
            return False
        if include_notes and self.notes:
            if content:
                content_to_send = f"{content}\n\n{self.notes}"
            else:
                content_to_send = self.notes
        else:
            content_to_send = content
        try:
            await self.message.edit(content=content_to_send, view=self)
            return True
        except Exception:
            self.cog.logger.exception(
                "Failed to update tournament drop selection message"
            )
            return False

    async def on_timeout(self) -> None:
        if not self.message:
            return
        self.disable_all_items()
        await self.update_message_content(
            "Tournament selection timed out.", include_notes=False
        )


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
            content=f"Preparing standings link for **{tournament_name}**…",
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

    async def _delete_selection_message(
        self, interaction: discord.Interaction
    ) -> None:
        try:
            await interaction.delete_original_response()
        except Exception:
            self.cog.logger.exception(
                "Failed to delete tournament standings selection message"
            )
        finally:
            self.message = None

    async def show_standings(
        self,
        interaction: discord.Interaction,
        tournament: dict,
        identifier: str,
    ) -> None:
        await self._delete_selection_message(interaction)
        resolved_identifier = (
            self.cog._resolve_tournament_identifier(tournament) or identifier
        )
        if not resolved_identifier:
            await interaction.followup.send(
                "I couldn't determine the identifier for that tournament.",
                ephemeral=True,
            )
            return

        raw_url = tournament.get("full_challonge_url") or tournament.get("url")
        standings_url: str | None = None
        if isinstance(raw_url, str) and raw_url.strip():
            candidate = raw_url.strip()
            if candidate.startswith("http://") or candidate.startswith("https://"):
                standings_url = candidate.rstrip("/")
            else:
                standings_url = f"https://challonge.com/{candidate.lstrip('/')}".rstrip("/")
        else:
            if resolved_identifier.startswith("http://") or resolved_identifier.startswith("https://"):
                standings_url = resolved_identifier.rstrip("/")
            else:
                standings_url = f"https://challonge.com/{resolved_identifier}".rstrip("/")

        if not standings_url:
            await interaction.followup.send(
                "I couldn't determine a standings link for that tournament.",
                ephemeral=True,
            )
            return

        if not standings_url.lower().endswith("/standings"):
            standings_url = f"{standings_url}/standings"

        await interaction.followup.send(
            f"Here is a link to this tournament's standings: {standings_url}",
            ephemeral=True,
        )

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