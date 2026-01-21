# cogs/teams.py
import asyncio
import math
import os
import random
from collections import defaultdict
from typing import Iterable

import discord
from discord import app_commands
from discord.ext import commands

from core.db import (
    db_add_cards,
    db_match_log_games_for_set,
    db_team_battleground_totals_ensure,
    db_team_battleground_totals_get,
    db_team_battleground_totals_update,
    db_team_battleground_user_points_clear,
    db_team_battleground_user_points_for_user,
    db_team_battleground_user_points_top,
    db_team_battleground_user_points_update,
    db_team_battleground_totals_clear,
    db_team_tracker_load,
    db_team_tracker_store,
)
from core.state import AppState
from core.constants import (
    CURRENT_ACTIVE_SET,
    DUEL_TEAM_SAME_TEAM_MULTIPLIER,
    DUEL_TEAM_TRANSFER_BASE,
    DUEL_TEAM_TRANSFER_MAX,
    DUEL_TEAM_TRANSFER_MIN,
    DUEL_TEAM_WIN_PCT_MULTIPLIER_MIN,
    DUEL_TEAM_WIN_PCT_MULTIPLIER_MAX,
    TEAM_BATTLEGROUND_MIDPOINT,
    TEAM_BATTLEGROUND_SEGMENT_SIZE,
    TEAM_BATTLEGROUND_START_POINTS,
    TEAM_SETS,
    latest_team_set_id,
)
from core.packs import open_pack_from_csv, open_pack_with_guaranteed_top_from_csv
from core.views import _pack_embed_for_cards

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

TEAM_CHANNEL_NAME = "battleground-⚔️"
TEAM_COLOR_EMOJIS = {
    "fire": "🟥",
    "water": "🟦",
    "wind": "🟩",
    "earth": "🟫",
    "past": "🟨",
    "future": "🟪",
}


async def _clear_channel_messages(channel: discord.TextChannel):
    """Remove all messages from the tracker channel before re-posting."""
    try:
        await channel.purge(limit=1000, check=lambda m: True, bulk=True, reason="Refreshing team tracker")
    except Exception:
        try:
            async for msg in channel.history(limit=200):
                try:
                    await msg.delete()
                except Exception:
                    pass
        except Exception:
            pass

def _get_active_team_set():
    set_id = latest_team_set_id()
    if set_id is None:
        return None, {}
    return set_id, TEAM_SETS.get(set_id, {})


def _get_active_team_names() -> tuple[str, ...]:
    _, cfg = _get_active_team_set()
    return tuple(cfg.get("order") or tuple((cfg.get("teams") or {}).keys()))


async def _resolve_member(interaction: discord.Interaction) -> discord.Member | None:
    if not interaction.guild:
        return None
    member = interaction.guild.get_member(interaction.user.id)
    if member:
        return member
    try:
        return await interaction.guild.fetch_member(interaction.user.id)
    except discord.NotFound:
        return None

class Teams(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: AppState = bot.state
        self._startup_once = False
        self._update_lock = asyncio.Lock()

    async def _team_autocomplete(
        self,
        interaction: discord.Interaction,
        current: str,
    ) -> list[app_commands.Choice[str]]:
        names = _get_active_team_names()
        current_lower = (current or "").lower()
        choices = []
        for name in names:
            if current_lower in name.lower():
                choices.append(app_commands.Choice(name=name, value=name))
        return choices[:25]

    async def _ensure_team_role(self, guild: discord.Guild, member: discord.Member, team_name: str) -> bool:
        try:
            role = discord.utils.get(guild.roles, name=team_name)
            if role is None:
                role = await guild.create_role(name=team_name, reason="Team assignment")
            await member.add_roles(role, reason="Join team")
            return True
        except discord.Forbidden:
            try:
                await member.send(
                    "I couldn't assign your team role due to permissions. "
                    "Please grant me **Manage Roles** and ensure my top role is above the team roles, then try again."
                )
            except Exception:
                pass
        except Exception:
            try:
                await member.send(
                    "Something went wrong while assigning your team role. Please try again shortly."
                )
            except Exception:
                pass
        return False

    async def _remove_conflicting_roles(self, member: discord.Member, keep: str):
        active_names = set(_get_active_team_names())
        to_remove = [role for role in member.roles if role.name in active_names and role.name != keep]
        if not to_remove:
            return
        try:
            await member.remove_roles(*to_remove, reason="Switching active team")
        except Exception:
            pass

    async def _send_pack_batch(self, sender, pack_name: str, per_pack: list[list[dict]], total: int):
        for idx, cards in enumerate(per_pack, start=1):
            content, embeds, files = _pack_embed_for_cards(self.bot, pack_name, cards, idx, total)
            send_kwargs: dict = {"embeds": embeds}
            if content:
                send_kwargs["content"] = content
            if files:
                send_kwargs["files"] = files
            await sender(**send_kwargs)
            if total > 5:
                await asyncio.sleep(0.2)

    async def _grant_team_packs(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        team_name: str,
        pack_name: str,
    ) -> bool:
        START_PACKS = 8
        guaranteed_tops = ["super"] * 6 + ["ultra"] * 2

        per_pack: list[list[dict]] = []
        for top_rarity in guaranteed_tops:
            try:
                cards = open_pack_with_guaranteed_top_from_csv(self.state, pack_name, top_rarity)
            except ValueError:
                cards = open_pack_from_csv(self.state, pack_name, 1)
            per_pack.append(cards)

        flat = [c for cards in per_pack for c in cards]
        db_add_cards(self.state, member.id, flat, pack_name)

        dm_sent = False
        try:
            dm = await member.create_dm()
            await self._send_pack_batch(dm.send, pack_name, per_pack, START_PACKS)
            dm_sent = True
        except Exception:
            channel = interaction.channel
            if channel:
                try:
                    await self._send_pack_batch(channel.send, pack_name, per_pack, START_PACKS)
                except Exception:
                    pass

        summary = (
            f"Welcome to the **{team_name}** team {member.mention}!"
            f" I sent you **{START_PACKS}** pack{'s' if START_PACKS != 1 else ''} of **{pack_name}**"
            f" to get started!{' Results sent via DM.' if dm_sent else ' I couldn’t DM you; posting results here.'}"
        )

        try:
            await interaction.followup.send(summary, ephemeral=False)
        except Exception:
            pass

        return dm_sent

    async def cog_load(self):
        asyncio.create_task(self._startup_task())

    async def _startup_task(self):
        await self.bot.wait_until_ready()
        if self._startup_once:
            return
        self._startup_once = True

        if not GUILD_ID:
            return

        guild = self.bot.get_guild(GUILD_ID)
        if not guild:
            return

        bot_member = guild.get_member(self.bot.user.id) if self.bot.user else None
        if not bot_member and self.bot.user:
            try:
                bot_member = await guild.fetch_member(self.bot.user.id)
            except Exception:
                bot_member = None
        if not bot_member:
            return

        await self._ensure_message_exists(guild)

    async def _ensure_tracker_channel(self, guild: discord.Guild, bot_member: discord.Member) -> discord.TextChannel:
        channel = discord.utils.get(guild.text_channels, name=TEAM_CHANNEL_NAME)
        if channel is None:
            try:
                overwrites = {
                    guild.default_role: discord.PermissionOverwrite(
                        view_channel=True,
                        send_messages=False,
                        send_messages_in_threads=False,
                        create_public_threads=False,
                        create_private_threads=False,
                        add_reactions=False,
                        use_application_commands=False,
                    ),
                    bot_member: discord.PermissionOverwrite(
                        view_channel=True,
                        send_messages=True,
                        send_messages_in_threads=True,
                        embed_links=True,
                        attach_files=True,
                        read_message_history=True,
                        manage_messages=True,
                        use_external_emojis=True,
                    ),
                }
                channel = await guild.create_text_channel(
                    TEAM_CHANNEL_NAME,
                    overwrites=overwrites,
                    reason="Create locked team tracker channel",
                )
            except discord.Forbidden:
                channel = await guild.create_text_channel(
                    TEAM_CHANNEL_NAME,
                    reason="Create team tracker channel",
                )
        else:
            try:
                overwrites = channel.overwrites
                overwrites[guild.default_role] = discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=False,
                    add_reactions=False,
                    create_public_threads=False,
                    create_private_threads=False,
                    send_messages_in_threads=False,
                )
                overwrites[bot_member] = discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=True,
                    embed_links=True,
                    attach_files=True,
                    read_message_history=True,
                    manage_messages=True,
                    use_external_emojis=True,
                    send_messages_in_threads=True,
                )
                await channel.edit(overwrites=overwrites, reason="Lock team tracker channel")
            except discord.Forbidden:
                pass
        return channel

    async def _ensure_message_exists(self, guild: discord.Guild, channel: discord.TextChannel | None = None):
        await self._refresh_tracker(guild, channel=channel)

    async def _get_tracker_channel(self, guild: discord.Guild) -> discord.TextChannel | None:
        channel = discord.utils.get(guild.text_channels, name=TEAM_CHANNEL_NAME)
        if channel:
            return channel

        bot_member = guild.get_member(self.bot.user.id) if self.bot.user else None
        if not bot_member and self.bot.user:
            try:
                bot_member = await guild.fetch_member(self.bot.user.id)
            except Exception:
                bot_member = None

        if bot_member:
            return await self._ensure_tracker_channel(guild, bot_member)
        return None

    async def _refresh_tracker(self, guild: discord.Guild, channel: discord.TextChannel | None = None):
        if not guild:
            return

        info = db_team_tracker_load(self.state, guild.id)
        stored_channel = None
        message = None

        if info:
            stored_channel = guild.get_channel(info["channel_id"])
            if stored_channel is None:
                stored_channel = self.bot.get_channel(info["channel_id"])
            if stored_channel is None:
                try:
                    stored_channel = await guild.fetch_channel(info["channel_id"])
                except Exception:
                    stored_channel = None
            if stored_channel and stored_channel.name != TEAM_CHANNEL_NAME:
                stored_channel = None
            if stored_channel:
                try:
                    message = await stored_channel.fetch_message(info["message_id"])
                except (discord.NotFound, discord.Forbidden):
                    message = None
                except Exception:
                    message = None

        target_channel = stored_channel or channel

        if target_channel is None:
            bot_member = guild.get_member(self.bot.user.id) if self.bot.user else None
            if not bot_member and self.bot.user:
                try:
                    bot_member = await guild.fetch_member(self.bot.user.id)
                except Exception:
                    bot_member = None
            if bot_member:
                target_channel = await self._ensure_tracker_channel(guild, bot_member)

        if target_channel is None:
            return

        embed = await self._build_tracker_embed(guild)

        try:
            if message:
                await message.edit(embed=embed)
                db_team_tracker_store(self.state, guild.id, message.channel.id, message.id)
            else:
                await _clear_channel_messages(target_channel)
                message = await target_channel.send(embed=embed)
                db_team_tracker_store(self.state, guild.id, target_channel.id, message.id)
        except discord.Forbidden:
            pass
        except discord.HTTPException:
            pass

    async def _build_tracker_embed(self, guild: discord.Guild) -> discord.Embed:
        set_id, cfg = _get_active_team_set()
        if set_id:
            self._ensure_battleground_totals(guild, int(set_id))
        totals = db_team_battleground_totals_get(self.state, guild.id, int(set_id or 0))
        combined_totals = {
            team: int(info.get("duel_points", 0)) + int(info.get("bonus_points", 0))
            for team, info in totals.items()
        }
        display_totals = self._round_totals_for_display(combined_totals)
        embed = discord.Embed(
            title="Team Battleground Tracker",
            description=(
                "No active team set found."
                if not set_id
                else "Each column is a **region** (200 units of territory). Each block is a **sector** (40 units of territory)."
            ),
            color=discord.Color.orange(),
        )
        embed.set_footer(
            text=f"Use /join_queue in the duel-arena channel to join the fight for your team!"
        )

        if not set_id:
            return embed

        teams = cfg.get("teams") or {}
        progress_lines = self._format_battleground_progress_lines(
            totals,
            display_totals,
            teams,
        )
        if progress_lines:
            embed.add_field(
                name="Battleground Territory",
                value="\n".join(progress_lines),
                inline=False,
            )

        return embed

    @staticmethod
    def _team_color_block(team_name: str) -> str:
        return TEAM_COLOR_EMOJIS.get(team_name.casefold(), "⬜")

    def _format_battleground_progress_lines(
        self,
        totals: dict[str, dict],
        display_totals: dict[str, int],
        teams: dict[str, dict],
    ) -> list[str]:
        if not totals:
            return []

        team_names = [name for name in _get_active_team_names() if name in totals]
        if len(team_names) < 2:
            return []

        left_team, right_team = team_names[:2]
        def _split_points(value: dict | int | float) -> tuple[int, int]:
            if isinstance(value, dict):
                return int(value.get("duel_points", 0)), int(value.get("bonus_points", 0))
            return int(value or 0), 0

        left_duel, left_bonus = _split_points(totals.get(left_team, {}))
        right_duel, right_bonus = _split_points(totals.get(right_team, {}))
        if left_duel <= 0 and right_duel <= 0 and left_bonus <= 0 and right_bonus <= 0:
            return []

        segment_units = 5
        segment_size = TEAM_BATTLEGROUND_SEGMENT_SIZE
        points_per_unit = segment_size / segment_units
        left_color = self._team_color_block(left_team)
        right_color = self._team_color_block(right_team)
        empty_block = "⬛"

        def _filled_column(units: int, color: str) -> list[str]:
            units = max(0, min(segment_units, units))
            cells = [empty_block] * segment_units
            for idx in range(segment_units - units, segment_units):
                if 0 <= idx < segment_units:
                    cells[idx] = color
            return cells

        def _bonus_columns(bonus_points: int, color: str, side: str) -> list[list[str]]:
            columns: list[list[str]] = []
            remaining = max(0, bonus_points)
            full_columns = remaining // segment_size
            remainder = remaining % segment_size

            full = [_filled_column(segment_units, color) for _ in range(full_columns)]
            partial: list[list[str]] = []
            if remainder > 0:
                units = int(remainder / points_per_unit)
                units = max(0, min(segment_units, units))
                partial.append(_filled_column(units, color))

            if side == "left":
                columns.extend(partial)
                columns.extend(full)
            else:
                columns.extend(full)
                columns.extend(partial)
            return columns

        def _distribute_duel_squares() -> tuple[int, int]:
            total_squares = segment_units * 5
            left_exact = left_duel / points_per_unit if points_per_unit else 0
            right_exact = right_duel / points_per_unit if points_per_unit else 0
            left_floor = int(math.floor(left_exact))
            right_floor = int(math.floor(right_exact))
            remaining = max(0, total_squares - (left_floor + right_floor))
            left_remainder = left_exact - left_floor
            right_remainder = right_exact - right_floor
            if remaining:
                if left_remainder >= right_remainder:
                    left_floor += remaining
                else:
                    right_floor += remaining
            left_floor = max(0, min(total_squares, left_floor))
            right_floor = max(0, min(total_squares - left_floor, right_floor))
            return left_floor, right_floor

        left_squares, right_squares = _distribute_duel_squares()

        base_columns = [[empty_block] * segment_units for _ in range(5)]
        left_remaining = left_squares
        for col_idx in range(5):
            if left_remaining <= 0:
                break
            units = min(segment_units, left_remaining)
            left_remaining -= units
            base_columns[col_idx] = _filled_column(units, left_color)

        right_remaining = right_squares
        for col_idx in range(4, -1, -1):
            if right_remaining <= 0:
                break
            units = min(segment_units, right_remaining)
            right_remaining -= units
            column = base_columns[col_idx]
            for idx in range(0, units):
                if 0 <= idx < segment_units:
                    column[idx] = right_color

        left_bonus_columns = _bonus_columns(left_bonus, left_color, "left")
        right_bonus_columns = _bonus_columns(right_bonus, right_color, "right")

        columns = [*left_bonus_columns, *base_columns, *right_bonus_columns]
        base_middle_index = 2
        left_total = display_totals.get(left_team, left_duel + left_bonus)
        right_total = display_totals.get(right_team, right_duel + right_bonus)
        left_segments = max(0, left_total // segment_size)
        right_segments = max(0, right_total // segment_size)
        lead = right_segments - left_segments
        if lead > 0:
            contested_shift = lead - 1
        elif lead < 0:
            contested_shift = lead + 1
        else:
            contested_shift = 0
        contested_base_index = max(0, min(4, base_middle_index - contested_shift))
        middle_index = len(left_bonus_columns) + base_middle_index
        contested_index = len(left_bonus_columns) + contested_base_index

        def _header_label(value: int) -> str:
            keycap = {
                0: "0️⃣",
                1: "1️⃣",
                2: "2️⃣",
                3: "3️⃣",
                4: "4️⃣",
                5: "5️⃣",
                6: "6️⃣",
                7: "7️⃣",
                8: "8️⃣",
                9: "9️⃣",
                10: "🔟",
            }
            return keycap.get(value, str(value))

        left_info = teams.get(left_team, {})
        right_info = teams.get(right_team, {})
        left_icon = left_info.get("emoji") or left_color
        right_icon = right_info.get("emoji") or right_color
        if right_team.casefold() == "water":
            right_icon = "🌊"

        rows = []
        header_cells = [left_icon, " "]
        for col_idx in range(len(columns)):
            if col_idx == contested_index:
                header_cells.append(" ")
            header_cells.append(_header_label(abs(col_idx - middle_index)))
            if col_idx == contested_index:
                header_cells.append(" ")
        header_cells.extend([" ", right_icon])
        rows.append("".join(header_cells).rstrip())

        rows.append("")
        for row_idx in range(segment_units):
            row_cells = [left_icon, " "]
            for col_idx, column in enumerate(columns):
                if col_idx == contested_index:
                    row_cells.append(" ")
                row_cells.append(column[row_idx])
                if col_idx == contested_index:
                    row_cells.append(" ")
            row_cells.extend([" ", right_icon])
            rows.append("".join(row_cells).rstrip())

        lines = rows + ["", "", f"**Total Territory Controlled**"]
        for team in (left_team, right_team):
            info = teams.get(team, {})
            title = info.get("display") or team
            emoji = info.get("emoji", "")
            shown_points = display_totals.get(team, 0)
            color = self._team_color_block(team)
            lines.append(f"{color} {title} {emoji}: {shown_points:,}")

        return lines
    
    async def _format_leaderboard(
        self,
        guild: discord.Guild,
        rows: Iterable[tuple[int, int]],
    ) -> str:
        entries = list(rows)
        if not entries:
            return "_No contributors yet._"

        lines = []
        for idx, (user_id, points) in enumerate(entries, start=1):
            member = guild.get_member(user_id)
            if member is None:
                try:
                    member = await guild.fetch_member(user_id)
                except Exception:
                    member = None
            display = member.display_name if member else f"<@{user_id}>"
            rounded_points = self._round_nearest(points)
            lines.append(f"{idx}. {display} — territory claimed: **{rounded_points:,}**")
        return "\n".join(lines)
    
    def _round_totals_for_display(self, totals: dict[str, int | float]) -> dict[str, int]:
        if not totals:
            return {}

        float_totals = {team: float(value) for team, value in totals.items()}
        if all(value.is_integer() for value in float_totals.values()):
            return {team: int(value) for team, value in float_totals.items()}

        highest_team, _ = max(float_totals.items(), key=lambda item: item[1])
        rounded: dict[str, int] = {}
        for team, value in float_totals.items():
            if value.is_integer():
                rounded[team] = int(value)
            elif team == highest_team:
                rounded[team] = math.ceil(value)
            else:
                rounded[team] = math.floor(value)
        return rounded

    @staticmethod
    def _round_nearest(value: int | float) -> int:
        return int(math.floor(float(value) + 0.5))

    def _resolve_member_team(self, member: discord.Member) -> str | None:
        active_names = set(_get_active_team_names())
        for role in member.roles:
            if role.name in active_names:
                return role.name
        return None

    def _ensure_battleground_totals(self, guild: discord.Guild, set_id: int) -> None:
        db_team_battleground_totals_ensure(
            self.state,
            guild.id,
            int(set_id),
            _get_active_team_names(),
            TEAM_BATTLEGROUND_START_POINTS,
        )

    @staticmethod
    def _skill_multiplier(winner_stats: dict, loser_stats: dict) -> float:
        winner_wins = int(winner_stats.get("wins", 0) or 0)
        winner_losses = int(winner_stats.get("losses", 0) or 0)
        loser_wins = int(loser_stats.get("wins", 0) or 0)
        loser_losses = int(loser_stats.get("losses", 0) or 0)
        skill_diff = (winner_wins - winner_losses) - (loser_wins - loser_losses)
        raw_multiplier = 2 / (1 + math.pow(10, skill_diff / 65))
        return max(
            DUEL_TEAM_WIN_PCT_MULTIPLIER_MIN,
            min(DUEL_TEAM_WIN_PCT_MULTIPLIER_MAX, raw_multiplier),
        )

    def _activity_multiplier(
        self,
        guild: discord.Guild,
        set_id: int,
        winner_team: str,
        loser_team: str,
    ) -> float:
        if winner_team == loser_team:
            return 1.0

        games_by_user = db_match_log_games_for_set(self.state, set_id)
        team_games: defaultdict[str, int] = defaultdict(int)
        active_names = set(_get_active_team_names())
        for member in guild.members:
            team_name = self._resolve_member_team(member)
            if not team_name or team_name not in active_names:
                continue
            team_games[team_name] += int(games_by_user.get(member.id, 0))

        winner_games = team_games.get(winner_team, 0)
        loser_games = team_games.get(loser_team, 0)
        raw_multiplier = (loser_games + 20) / (winner_games + 20)
        return max(2 / 3, min(3 / 2, raw_multiplier))

    @staticmethod
    def _segments_owned(points: int) -> int:
        threshold = TEAM_BATTLEGROUND_MIDPOINT + (TEAM_BATTLEGROUND_SEGMENT_SIZE // 2)
        if points < threshold:
            return 0
        return 1 + math.floor((points - threshold) / TEAM_BATTLEGROUND_SEGMENT_SIZE)

    def _segment_advantage_multiplier(self, winner_points: int, loser_points: int) -> float:
        winner_segments = self._segments_owned(winner_points)
        loser_segments = self._segments_owned(loser_points)
        if winner_segments == loser_segments:
            return 1.0
        lead = abs(winner_segments - loser_segments)
        if winner_segments > loser_segments:
            numerator = max(1, 11 - lead)
        else:
            numerator = 11 + lead
        return numerator / 11
    

    @staticmethod
    def _sector_claim_message(
        *,
        winner_name: str,
        team_name: str,
        before_total: int,
        after_total: int,
    ) -> str | None:
        sector_size = TEAM_BATTLEGROUND_SEGMENT_SIZE // 5
        if sector_size <= 0:
            return None
        if (after_total // sector_size) > (before_total // sector_size):
            color = Teams._team_color_block(team_name)
            return f"{winner_name} claimed a sector {color} for the {team_name} team!"
        return None

    def _calculate_transfer_points(
        self,
        *,
        base_multiplier: float,
        same_team: bool,
    ) -> int:
        multiplier = base_multiplier
        if same_team:
            multiplier *= DUEL_TEAM_SAME_TEAM_MULTIPLIER

        raw_points = DUEL_TEAM_TRANSFER_BASE * multiplier
        points = int(round(raw_points))
        return max(DUEL_TEAM_TRANSFER_MIN, min(DUEL_TEAM_TRANSFER_MAX, points))

    async def apply_duel_result(
        self,
        guild: discord.Guild,
        *,
        winner: discord.Member,
        loser: discord.Member,
        winner_stats: dict,
        loser_stats: dict,
        refresh_tracker: bool = True,
    ) -> tuple[int, dict[str, str]]:
        set_id, _ = _get_active_team_set()
        if not set_id:
            return 0, {"reason": "No active team set configured."}

        winner_team = self._resolve_member_team(winner)
        loser_team = self._resolve_member_team(loser)
        if not winner_team or not loser_team:
            return 0, {"reason": "Missing team role for one or more players."}

        active_names = set(_get_active_team_names())
        if winner_team not in active_names or loser_team not in active_names:
            return 0, {"reason": "Team roles are not part of the active set."}
        
        same_team = winner_team == loser_team
        transfer_loser_team = loser_team
        if same_team:
            opposing_team = next((name for name in active_names if name != winner_team), None)
            if not opposing_team:
                return 0, {"reason": "No opposing team configured for the active set."}
            transfer_loser_team = opposing_team

        self._ensure_battleground_totals(guild, int(set_id))

        totals = db_team_battleground_totals_get(self.state, guild.id, int(set_id))
        winner_points = int(totals.get(winner_team, {}).get("duel_points", TEAM_BATTLEGROUND_START_POINTS))
        loser_points = int(
            totals.get(transfer_loser_team, {}).get("duel_points", TEAM_BATTLEGROUND_START_POINTS)
        )

        skill_multiplier = 1.0 if same_team else self._skill_multiplier(winner_stats, loser_stats)
        activity_multiplier = self._activity_multiplier(guild, int(set_id), winner_team, transfer_loser_team)
        segment_multiplier = self._segment_advantage_multiplier(winner_points, loser_points)
        transfer_points = self._calculate_transfer_points(
            base_multiplier=skill_multiplier * activity_multiplier * segment_multiplier,
            same_team=same_team,
        )

        moved_points = min(transfer_points, loser_points)

        if moved_points > 0:
            db_team_battleground_totals_update(
                self.state,
                guild.id,
                int(set_id),
                winner_team,
                duel_delta=moved_points,
            )

            db_team_battleground_totals_update(
                self.state,
                guild.id,
                int(set_id),
                transfer_loser_team,
                duel_delta=-moved_points,
            )

        if moved_points > 0:
            db_team_battleground_user_points_update(
                self.state,
                guild.id,
                int(set_id),
                winner.id,
                winner_team,
                earned_delta=moved_points,
                net_delta=moved_points,
            )
            db_team_battleground_user_points_update(
                self.state,
                guild.id,
                int(set_id),
                loser.id,
                loser_team,
                earned_delta=0,
                net_delta=0 if same_team else -moved_points,
            )
        if refresh_tracker:
            await self._ensure_message_exists(guild)

        updated_totals = db_team_battleground_totals_get(self.state, guild.id, int(set_id))
        def _total_for(team: str) -> int:
            info = updated_totals.get(team, {})
            return int(info.get("duel_points", 0)) + int(info.get("bonus_points", 0))

        winner_total = _total_for(winner_team)
        sector_message = self._sector_claim_message(
            winner_name=winner.display_name,
            team_name=winner_team,
            before_total=max(0, winner_total - moved_points),
            after_total=winner_total,
        )

        return moved_points, {
            "winner_team": winner_team,
            "loser_team": transfer_loser_team,
            "same_team": "yes" if same_team else "no",
            "winner_total": winner_total,
            "loser_total": _total_for(transfer_loser_team),
            "sector_message": sector_message,
        }

    async def split_duel_team_points(
        self, guild: discord.Guild
    ) -> tuple[bool, str | None, discord.Embed | None]:
        if not guild:
            return False, "This command can only be used in a server.", None

        return (
            False,
            "Battleground team territory is updated automatically per match; no split is required.",
            None,
        )

    @app_commands.command(name="team_award", description="(Admin) Award territory to a team member")
    @app_commands.describe(member="Member to award territory to", points="Amount of territory to award")
    @app_commands.guilds(GUILD)
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def team_award(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        points: app_commands.Range[int, 1, 1_000_000],
    ):
        if not interaction.guild:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        set_id, _ = _get_active_team_set()
        if not set_id:
            await interaction.followup.send(
                "No active team set is configured for awarding territory.",
                ephemeral=True,
            )
            return

        team_name = self._resolve_member_team(member)
        if not team_name:
            await interaction.followup.send(
                "That member does not have a team role for the current set assigned.",
                ephemeral=True,
            )
            return

        self._ensure_battleground_totals(interaction.guild, int(set_id))

        totals = db_team_battleground_totals_update(
            self.state,
            interaction.guild.id,
            int(set_id),
            team_name,
            bonus_delta=int(points),
        )
        db_team_battleground_user_points_update(
            self.state,
            interaction.guild.id,
            int(set_id),
            member.id,
            team_name,
            earned_delta=int(points),
            net_delta=0,
            bonus_delta=int(points),
        )

        await self._ensure_message_exists(interaction.guild)

        total_points = int(totals.get("duel_points", 0)) + int(totals.get("bonus_points", 0))
        message = (
            f"{member.display_name} claimed **{int(points):,}** units of territory for the {team_name} team. "
            f"Territory controlled: **{total_points:,}**."
        )
        await interaction.followup.send(
            message,
            ephemeral=True,
        )

    @app_commands.command(
        name="team_simulate_matches",
        description="(Admin) Simulate a batch of battleground matches.",
    )
    @app_commands.describe(
        matches="Number of simulated matches to run",
        favored_team="Team more likely to win",
        favored_win_rate="Chance that the favored team wins (0.0 to 1.0)",
        seed="Optional random seed for reproducibility",
    )
    @app_commands.autocomplete(favored_team=_team_autocomplete)
    @app_commands.guilds(GUILD)
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def team_simulate_matches(
        self,
        interaction: discord.Interaction,
        matches: app_commands.Range[int, 1, 1_000],
        favored_team: str,
        favored_win_rate: app_commands.Range[float, 0.0, 1.0] = 0.5,
        seed: int | None = None,
    ):
        if not interaction.guild:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        guild = interaction.guild
        set_id, _ = _get_active_team_set()
        if not set_id:
            await interaction.followup.send("No active team set is configured.", ephemeral=True)
            return

        active_names = list(_get_active_team_names())
        if favored_team not in active_names:
            await interaction.followup.send("Favored team must be part of the active team set.", ephemeral=True)
            return

        opposing_team = next((name for name in active_names if name != favored_team), None)
        if not opposing_team:
            await interaction.followup.send("No opposing team found for the active team set.", ephemeral=True)
            return

        team_members: dict[str, list[discord.Member]] = {
            team: [member for member in guild.members if self._resolve_member_team(member) == team]
            for team in (favored_team, opposing_team)
        }
        if not team_members[favored_team] or not team_members[opposing_team]:
            await interaction.followup.send(
                "Both teams need at least one member to simulate matches.",
                ephemeral=True,
            )
            return

        favored_member = team_members[favored_team][0]
        opposing_member = team_members[opposing_team][0]

        rng = random.Random(seed)
        total_moved = 0
        favored_wins = 0

        for _ in range(int(matches)):
            favored_won = rng.random() < float(favored_win_rate)
            winner = favored_member if favored_won else opposing_member
            loser = opposing_member if favored_won else favored_member
            if favored_won:
                favored_wins += 1

            moved, _ = await self.apply_duel_result(
                guild,
                winner=winner,
                loser=loser,
                winner_stats={"wins": 10, "losses": 2},
                loser_stats={"wins": 2, "losses": 10},
                refresh_tracker=False,
            )
            total_moved += moved

        await self._ensure_message_exists(guild)

        await interaction.followup.send(
            (
                f"Simulated **{int(matches):,}** matches with **{favored_team}** favored "
                f"({favored_wins:,} wins, {int(matches) - favored_wins:,} losses). "
                f"Total territory moved: **{total_moved:,}**."
            ),
            ephemeral=True,
        )

    @app_commands.command(
        name="team_transfer_points",
        description="(Admin) Transfer duel territory between teams without assigning member territory.",
    )
    @app_commands.describe(
        from_team="Team losing territory",
        to_team="Team gaining territory",
        points="Amount of territory to transfer",
        set_id="Team set number to adjust (defaults to the active set)",
    )
    @app_commands.autocomplete(from_team=_team_autocomplete, to_team=_team_autocomplete)
    @app_commands.guilds(GUILD)
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def team_transfer_points(
        self,
        interaction: discord.Interaction,
        from_team: str,
        to_team: str,
        points: app_commands.Range[int, 1, 1_000_000],
        set_id: app_commands.Range[int, 1, 9999] | None = None,
    ):
        if not interaction.guild:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        if from_team == to_team:
            await interaction.followup.send("Source and destination teams must be different.", ephemeral=True)
            return

        active_set_id, _ = _get_active_team_set()
        resolved_set_id = int(set_id or active_set_id or 0)
        if not resolved_set_id:
            await interaction.followup.send("No active team set is configured.", ephemeral=True)
            return

        active_names = set(_get_active_team_names())
        if from_team not in active_names or to_team not in active_names:
            await interaction.followup.send(
                "Both teams must be part of the active team set.",
                ephemeral=True,
            )
            return

        self._ensure_battleground_totals(interaction.guild, resolved_set_id)
        totals = db_team_battleground_totals_get(self.state, interaction.guild.id, resolved_set_id)
        from_points = int(totals.get(from_team, {}).get("duel_points", 0))
        moved_points = min(int(points), from_points)
        if moved_points <= 0:
            await interaction.followup.send(
                f"Team **{from_team}** does not have any duel territory to transfer.",
                ephemeral=True,
            )
            return

        db_team_battleground_totals_update(
            self.state,
            interaction.guild.id,
            resolved_set_id,
            from_team,
            duel_delta=-moved_points,
        )
        db_team_battleground_totals_update(
            self.state,
            interaction.guild.id,
            resolved_set_id,
            to_team,
            duel_delta=moved_points,
        )
        await self._ensure_message_exists(interaction.guild)

        updated_totals = db_team_battleground_totals_get(
            self.state,
            interaction.guild.id,
            resolved_set_id,
        )
        to_total = int(updated_totals.get(to_team, {}).get("duel_points", 0)) + int(
            updated_totals.get(to_team, {}).get("bonus_points", 0)
        )
        message = (
            f"{interaction.user.display_name} claimed **{moved_points:,}** units of territory "
            f"for the {to_team} team (set **{resolved_set_id}**). Territory controlled: **{to_total:,}**."
        )
        await interaction.followup.send(
            message,
            ephemeral=True,
        )

    @app_commands.command(
        name="team_award_transfer",
        description="(Admin) Award territory to a member and transfer it from another team.",
    )
    @app_commands.describe(
        member="Member receiving territory",
        points="Amount of territory to award and transfer",
        from_team="Team losing territory (defaults to the opposing team when possible)",
    )
    @app_commands.autocomplete(from_team=_team_autocomplete)
    @app_commands.guilds(GUILD)
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def team_award_transfer(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        points: app_commands.Range[int, 1, 1_000_000],
        from_team: str | None = None,
    ):
        if not interaction.guild:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        set_id, _ = _get_active_team_set()
        if not set_id:
            await interaction.followup.send("No active team set is configured.", ephemeral=True)
            return

        team_name = self._resolve_member_team(member)
        if not team_name:
            await interaction.followup.send(
                "That member does not have a team role for the current set assigned.",
                ephemeral=True,
            )
            return

        active_names = [name for name in _get_active_team_names() if name]
        transfer_from = (from_team or "").strip()
        if not transfer_from:
            other_names = [name for name in active_names if name != team_name]
            transfer_from = other_names[0] if len(other_names) == 1 else ""
        if not transfer_from or transfer_from == team_name:
            await interaction.followup.send(
                "Please specify a valid opposing team to transfer territory from.",
                ephemeral=True,
            )
            return
        if transfer_from not in active_names:
            await interaction.followup.send(
                "The source team must be part of the active team set.",
                ephemeral=True,
            )
            return

        self._ensure_battleground_totals(interaction.guild, int(set_id))
        totals = db_team_battleground_totals_get(self.state, interaction.guild.id, int(set_id))
        from_points = int(totals.get(transfer_from, {}).get("duel_points", 0))
        moved_points = min(int(points), from_points)
        if moved_points <= 0:
            await interaction.followup.send(
                f"Team **{transfer_from}** does not have any duel territory to transfer.",
                ephemeral=True,
            )
            return

        db_team_battleground_totals_update(
            self.state,
            interaction.guild.id,
            int(set_id),
            team_name,
            duel_delta=moved_points,
        )
        db_team_battleground_totals_update(
            self.state,
            interaction.guild.id,
            int(set_id),
            transfer_from,
            duel_delta=-moved_points,
        )
        db_team_battleground_user_points_update(
            self.state,
            interaction.guild.id,
            int(set_id),
            member.id,
            team_name,
            earned_delta=moved_points,
            net_delta=moved_points,
        )
        await self._ensure_message_exists(interaction.guild)

        updated_totals = db_team_battleground_totals_get(
            self.state,
            interaction.guild.id,
            int(set_id),
        )
        team_total = int(updated_totals.get(team_name, {}).get("duel_points", 0)) + int(
            updated_totals.get(team_name, {}).get("bonus_points", 0)
        )
        message = (
            f"{member.display_name} claimed **{moved_points:,}** units of territory for the {team_name} team "
            f"(transferred from {transfer_from}). Territory controlled: **{team_total:,}**."
        )
        await interaction.followup.send(
            message,
            ephemeral=True,
        )

    @app_commands.command(
        name="team_split_points",
        description="(Admin) Split duel team territory based on wins for the active set.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def team_split_points(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        success, content, embed = await self.split_duel_team_points(interaction.guild)
        if not success:
            await interaction.followup.send(content or "No duel wins recorded.", ephemeral=True)
            return

        await interaction.followup.send(
            embed=embed,
            content=content,
            ephemeral=True
        )

    @app_commands.command(
        name="team_reset_points",
        description="(Admin) Reset battleground team territory for a set.",
    )
    @app_commands.describe(
        set_id="Team set number to clear battleground territory for",
        member="Optional member to target; clears all for the set when omitted",
    )
    @app_commands.guilds(GUILD)
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def team_reset_points(
        self,
        interaction: discord.Interaction,
        set_id: app_commands.Range[int, 1, 9999],
        member: discord.Member | None = None,
    ):
        if not interaction.guild:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        guild = interaction.guild
        if member:
            entries = db_team_battleground_user_points_for_user(
                self.state, guild.id, int(set_id), member.id
            )
            entry = next(iter(entries.values()), None)
            if not entry:
                await interaction.followup.send(
                    "No stored battleground territory was found for that member in this set.",
                    ephemeral=True,
                )
                return

            team_name = str(entry.get("team") or "")
            net_points = int(entry.get("net_points") or 0)
            bonus_points = int(entry.get("bonus_points") or 0)

            if team_name:
                db_team_battleground_totals_update(
                    self.state,
                    guild.id,
                    int(set_id),
                    team_name,
                    duel_delta=-net_points,
                    bonus_delta=-bonus_points,
                )

            cleared_rows = db_team_battleground_user_points_clear(
                self.state,
                guild.id,
                int(set_id),
                member.id,
            )
            
            await self._ensure_message_exists(guild)

            embed = discord.Embed(
                title="Team Territory Reset",
                description=(
                    f"Cleared battleground territory for set **{int(set_id)}**"
                    f" for {member.mention}"
                ),
                color=discord.Color.red(),
            )
            embed.add_field(
                name="Net territory removed",
                value=f"**{net_points:,} territory**",
                inline=True,
            )
            embed.add_field(
                name="Bonus territory removed",
                value=f"**{bonus_points:,} territory**",
                inline=True,
            )

            await interaction.followup.send(
                embed=embed,
                content=(
                    f"Cleared battleground territory entries for **{cleared_rows}** member"
                    f"{'s' if cleared_rows != 1 else ''}."
                ),
                ephemeral=True,
            )
            return

        cleared_totals = db_team_battleground_totals_clear(
            self.state,
            guild.id,
            int(set_id),
        )
        cleared_members = db_team_battleground_user_points_clear(
            self.state,
            guild.id,
            int(set_id),
        )

        await self._ensure_message_exists(guild)

        embed = discord.Embed(
            title="Team Territory Reset",
            description=f"Cleared battleground territory for set **{int(set_id)}**",
            color=discord.Color.red(),
        )
        embed.add_field(name="Teams cleared", value=f"**{cleared_totals}**", inline=True)
        embed.add_field(name="Members cleared", value=f"**{cleared_members}**", inline=True)

        await interaction.followup.send(
            embed=embed,
            content=(
                f"Cleared battleground territory for **{cleared_members}** member"
                f"{'s' if cleared_members != 1 else ''}."
            ),
            ephemeral=True,
        )

    @app_commands.command(
        name="team_clear_member_points",
        description="(Admin) Clear battleground member territory without changing team totals.",
    )
    @app_commands.describe(
        set_id="Team set number to clear member territory for",
        member="Optional member to target; clears all members when omitted",
    )
    @app_commands.guilds(GUILD)
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def team_clear_member_points(
        self,
        interaction: discord.Interaction,
        set_id: app_commands.Range[int, 1, 9999],
        member: discord.Member | None = None,
    ):
        if not interaction.guild:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        cleared_rows = db_team_battleground_user_points_clear(
            self.state,
            interaction.guild.id,
            int(set_id),
            member.id if member else None,
        )
        await self._ensure_message_exists(interaction.guild)

        target = member.mention if member else "all members"
        await interaction.followup.send(
            f"Cleared battleground member territory for {target} in set **{int(set_id)}** "
            f"({cleared_rows} record{'s' if cleared_rows != 1 else ''}).",
            ephemeral=True,
        )

    @app_commands.command(name="join_team", description="Join the latest set's team and receive matching packs")
    @app_commands.describe(team="The team you want to join")
    @app_commands.autocomplete(team=_team_autocomplete)
    @app_commands.guilds(GUILD)
    @app_commands.guild_only()
    async def join_team(self, interaction: discord.Interaction, team: str):
        if not interaction.guild:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return

        member = await _resolve_member(interaction)
        if not member:
            await interaction.response.send_message(
                "Could not resolve your member record. Please try again shortly.",
                ephemeral=True,
            )
            return

        set_id, cfg = _get_active_team_set()
        teams = cfg.get("teams") or {}
        if not set_id or not teams:
            await interaction.response.send_message(
                "No active team set is configured right now.",
                ephemeral=True,
            )
            return

        active_names = set(_get_active_team_names())
        existing_team = next((role.name for role in member.roles if role.name in active_names), None)
        if existing_team:
            await interaction.response.send_message(
                f"You have already joined **{existing_team}** for the current team set.",
                ephemeral=True,
            )
            return

        lookup = {name.lower(): name for name in _get_active_team_names() if name in teams}
        chosen = lookup.get(team.lower()) if team else None
        if not chosen:
            available = ", ".join(lookup.values()) or "No teams available"
            await interaction.response.send_message(
                f"Please choose one of the current teams: {available}.",
                ephemeral=True,
            )
            return

        if any(role.name == chosen for role in member.roles):
            await interaction.response.send_message(
                f"You are already on **{chosen}**.",
                ephemeral=True,
            )
            return

        pack_name = (teams.get(chosen) or {}).get("pack")
        if not pack_name:
            await interaction.response.send_message(
                "Could not find the pack linked to that team. Please contact an admin.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        await self._remove_conflicting_roles(member, chosen)
        role_ok = await self._ensure_team_role(interaction.guild, member, chosen)
        if not role_ok:
            await interaction.followup.send(
                "I couldn't assign your team role. Please check my permissions and try again.",
                ephemeral=True,
            )
            return

        await self._ensure_message_exists(interaction.guild)
        await self._grant_team_packs(interaction, member, chosen, pack_name)

async def setup(bot: commands.Bot):
    await bot.add_cog(Teams(bot))

    active_set = latest_team_set_id()
    if active_set is None or active_set < 2:
        try:
            bot.tree.remove_command(
                "join_team", type=discord.AppCommandType.chat_input, guild=GUILD
            )
        except Exception:
            pass
