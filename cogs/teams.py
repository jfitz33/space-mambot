# cogs/teams.py
import asyncio
import os
from typing import Iterable

import discord
from discord import app_commands
from discord.ext import commands

from core.db import (
    db_team_points_add,
    db_team_points_totals,
    db_team_points_top,
    db_team_tracker_load,
    db_team_tracker_store,
)
from core.state import AppState
from core.constants import TEAM_ROLE_NAMES

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

TEAM_CHANNEL_NAME = "team-points-tracker"
TEAM_EMOJIS = {
    "Fire": "🔥",
    "Water": "💧",
}
TEAM_DISPLAY_NAMES = {
    "Fire": "Team Fire",
    "Water": "Team Water",
}
TEAM_ORDER: tuple[str, ...] = ("Fire", "Water")


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


class Teams(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: AppState = bot.state
        self._startup_once = False
        self._update_lock = asyncio.Lock()

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
        totals = db_team_points_totals(self.state, guild.id)
        embed = discord.Embed(
            title="Team Points Tracker",
            description="Top contributors for each team.",
            color=discord.Color.orange(),
        )

        for team in TEAM_ORDER:
            title = TEAM_DISPLAY_NAMES.get(team, team)
            emoji = TEAM_EMOJIS.get(team, "")
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
        for role in member.roles:
            if role.name in TEAM_ROLE_NAMES:
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
                "That member does not have a team role assigned.",
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


async def setup(bot: commands.Bot):
    await bot.add_cog(Teams(bot))