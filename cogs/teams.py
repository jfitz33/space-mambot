# cogs/teams.py
import asyncio
import os
from typing import Iterable

import discord
from discord import app_commands
from discord.ext import commands

from core.db import (
    db_add_cards,
    db_team_points_add,
    db_team_points_totals,
    db_team_points_top,
    db_team_tracker_load,
    db_team_tracker_store,
)
from core.state import AppState
from core.constants import TEAM_ROLE_NAMES, TEAM_SETS, latest_team_set_id
from core.packs import open_pack_from_csv, open_pack_with_guaranteed_top_from_csv
from core.views import _pack_embed_for_cards

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

TEAM_CHANNEL_NAME = "team-points-tracker"


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

        channel = await self._ensure_tracker_channel(guild, bot_member)
        await self._ensure_message_exists(guild, channel)

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
        async with self._update_lock:
            await self._refresh_tracker(guild, channel=channel)

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
        totals = db_team_points_totals(self.state, guild.id)
        embed = discord.Embed(
            title="Team Points Tracker",
            description=(
                "Top contributors for the current team set." if set_id else "No active team set found."
            ),
            color=discord.Color.orange(),
        )

        if not set_id:
            return embed

        teams = cfg.get("teams") or {}
        for team in _get_active_team_names():
            info = teams.get(team, {})
            title = info.get("display") or team
            emoji = info.get("emoji", "")
            total_points = totals.get(team, 0)
            rows = db_team_points_top(self.state, guild.id, team, limit=3)
            value = await self._format_leaderboard(guild, rows)
            name = f"{title} {emoji} — {total_points:,} pts"
            embed.add_field(name=name, value=value, inline=True)

        if len(embed.fields) == 1:
            embed.add_field(name="\u200b", value="\u200b", inline=True)

        return embed

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
            lines.append(f"{idx}. {display} — **{points:,}**")
        return "\n".join(lines)

    def _resolve_member_team(self, member: discord.Member) -> str | None:
        active_names = set(_get_active_team_names())
        for role in member.roles:
            if role.name in active_names:
                return role.name
        return None

    @app_commands.command(name="team_award", description="(Admin) Award points to a team member")
    @app_commands.describe(member="Member to award points to", points="Number of points to award")
    @app_commands.guilds(GUILD)
    @app_commands.guild_only()
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

        team_name = self._resolve_member_team(member)
        if not team_name:
            await interaction.followup.send(
                "That member does not have a team role for the current set assigned.",
                ephemeral=True,
            )
            return

        new_total = db_team_points_add(
            self.state,
            interaction.guild.id,
            member.id,
            team_name,
            int(points),
        )

        await self._ensure_message_exists(interaction.guild)

        await interaction.followup.send(
            f"Awarded **{int(points):,}** points to {member.mention} on **{team_name}**. "
            f"They now have **{new_total:,}** points.",
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