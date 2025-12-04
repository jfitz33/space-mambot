import asyncio
import os
from dataclasses import dataclass
from datetime import datetime, timedelta

import discord
from discord import app_commands
from discord.ext import commands

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

DUEL_CHANNEL_NAME = "duel-arena"
STALE_WAIT = timedelta(minutes=10)
CONFIRM_TIMEOUT_SECONDS = 120


@dataclass
class QueueEntry:
    user_id: int
    joined_at: datetime


@dataclass
class PendingConfirmation:
    challenger_id: int
    channel_id: int | None
    view: discord.ui.View


class QueueConfirmationView(discord.ui.View):
    def __init__(self, cog: "DuelQueue", waiting_user_id: int, challenger_id: int, channel_id: int | None):
        super().__init__(timeout=CONFIRM_TIMEOUT_SECONDS)
        self.cog = cog
        self.waiting_user_id = waiting_user_id
        self.challenger_id = challenger_id
        self.channel_id = channel_id
        self.message: discord.Message | None = None

    async def on_timeout(self):
        try:
            if self.message:
                for child in self.children:
                    child.disabled = True
                await self.message.edit(view=self)
        except Exception:
            pass
        await self.cog.handle_confirmation_timeout(self.waiting_user_id, self.challenger_id, self.channel_id)

    async def _ack(self, interaction: discord.Interaction, content: str):
        for child in self.children:
            child.disabled = True
        try:
            await interaction.response.edit_message(content=content, view=self)
        except discord.InteractionResponded:
            if interaction.message:
                await interaction.message.edit(content=content, view=self)

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.success)
    async def yes(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.waiting_user_id:
            return await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)

        await self._ack(interaction, "Got it, pairing you now…")
        await self.cog.handle_confirmation_response(self.waiting_user_id, self.challenger_id, self.channel_id, accepted=True)
        self.stop()

    @discord.ui.button(label="No", style=discord.ButtonStyle.danger)
    async def no(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.waiting_user_id:
            return await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)

        await self._ack(interaction, "No worries, removing you from the queue.")
        await self.cog.handle_confirmation_response(self.waiting_user_id, self.challenger_id, self.channel_id, accepted=False)
        self.stop()


class DuelQueue(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.queue: list[QueueEntry] = []
        self.active_pairs: dict[int, int] = {}
        self.pending_confirmations: dict[int, PendingConfirmation] = {}
        self.lock = asyncio.Lock()
        self.guild_id = GUILD_ID

    def _in_duel_channel(self, interaction: discord.Interaction) -> bool:
        channel = interaction.channel
        return bool(channel and getattr(channel, "name", "") == DUEL_CHANNEL_NAME)

    def _find_queue_entry(self, user_id: int) -> QueueEntry | None:
        for entry in self.queue:
            if entry.user_id == user_id:
                return entry
        return None

    def _remove_from_queue(self, user_id: int) -> bool:
        for idx, entry in enumerate(self.queue):
            if entry.user_id == user_id:
                self.queue.pop(idx)
                return True
        return False

    def _resolve_duel_channel(self, channel_id: int | None = None, guild: discord.Guild | None = None) -> discord.TextChannel | None:
        if channel_id:
            channel = self.bot.get_channel(channel_id)
            if isinstance(channel, discord.TextChannel):
                return channel
        target_guild = guild or (self.bot.get_guild(self.guild_id) if self.guild_id else None)
        if target_guild:
            for chan in target_guild.text_channels:
                if chan.name == DUEL_CHANNEL_NAME:
                    return chan
        return None

    async def _announce_pair(self, user1_id: int, user2_id: int, channel: discord.TextChannel | None):
        if not channel:
            return
        user1 = self.bot.get_user(user1_id)
        user2 = self.bot.get_user(user2_id)
        mention1 = user1.mention if user1 else f"<@{user1_id}>"
        mention2 = user2.mention if user2 else f"<@{user2_id}>"
        await channel.send(f"{mention1} is paired vs {mention2}. Good luck duelists!")

    async def _request_confirmation(self, waiting_user_id: int, challenger_id: int, channel_id: int | None, guild: discord.Guild | None):
        member: discord.abc.User | None = None
        if guild:
            member = guild.get_member(waiting_user_id)
        if member is None:
            member = self.bot.get_user(waiting_user_id)

        view = QueueConfirmationView(self, waiting_user_id, challenger_id, channel_id)

        try:
            message = await member.send("I found you an opponent! Do you still wish to play?", view=view) if member else None
        except discord.Forbidden:
            message = None

        if message is None:
            async with self.lock:
                self.pending_confirmations.pop(waiting_user_id, None)
                self._remove_from_queue(waiting_user_id)
            channel = self._resolve_duel_channel(channel_id, guild)
            await self._process_queue(channel, guild)
            return

        view.message = message
        async with self.lock:
            self.pending_confirmations[waiting_user_id] = PendingConfirmation(
                challenger_id=challenger_id,
                channel_id=channel_id,
                view=view,
            )

    async def _process_queue(self, channel: discord.abc.Messageable | None, guild: discord.Guild | None):
        while True:
            async with self.lock:
                if len(self.queue) < 2:
                    return

                waiting = self.queue[0]
                challenger = self.queue[1]

                if waiting.user_id in self.pending_confirmations:
                    return

                age = datetime.utcnow() - waiting.joined_at
                if age > STALE_WAIT:
                    channel_id = channel.id if isinstance(channel, discord.TextChannel) else None
                    await self._request_confirmation(waiting.user_id, challenger.user_id, channel_id, guild)
                    return

                user1_id, user2_id = waiting.user_id, challenger.user_id
                self.queue = self.queue[2:]
                self.active_pairs[user1_id] = user2_id
                self.active_pairs[user2_id] = user1_id

            await self._announce_pair(user1_id, user2_id, channel if isinstance(channel, discord.TextChannel) else None)

    async def handle_confirmation_response(self, waiting_user_id: int, challenger_id: int, channel_id: int | None, *, accepted: bool):
        async with self.lock:
            self.pending_confirmations.pop(waiting_user_id, None)
            waiting_removed = self._remove_from_queue(waiting_user_id)
            challenger_removed = self._remove_from_queue(challenger_id) if accepted else False

            if accepted and waiting_removed and challenger_removed:
                self.active_pairs[waiting_user_id] = challenger_id
                self.active_pairs[challenger_id] = waiting_user_id
                pair_ids = (waiting_user_id, challenger_id)
            else:
                pair_ids = None

        guild = self.bot.get_guild(self.guild_id)
        channel = self._resolve_duel_channel(channel_id, guild)

        if accepted:
            if pair_ids:
                await self._announce_pair(pair_ids[0], pair_ids[1], channel)
            else:
                user = self.bot.get_user(waiting_user_id)
                if user:
                    await user.send("Unable to pair you—the opponent is no longer in the queue.")
            await self._process_queue(channel, guild)
            return

        user = self.bot.get_user(waiting_user_id)
        if user:
            await user.send("Removed you from the queue.")
        await self._process_queue(channel, guild)

    async def handle_confirmation_timeout(self, waiting_user_id: int, challenger_id: int, channel_id: int | None):
        async with self.lock:
            self.pending_confirmations.pop(waiting_user_id, None)
            removed = self._remove_from_queue(waiting_user_id)

        user = self.bot.get_user(waiting_user_id)
        if removed and user:
            try:
                await user.send("Confirmation timed out, removing you from the queue")
            except Exception:
                pass

        guild = self.bot.get_guild(self.guild_id)
        channel = self._resolve_duel_channel(channel_id, guild)
        await self._process_queue(channel, guild)

    async def _cancel_pending_for_user(self, user_id: int):
        async with self.lock:
            ctx = self.pending_confirmations.pop(user_id, None)
        if ctx and isinstance(ctx.view, QueueConfirmationView):
            ctx.view.stop()
            try:
                if ctx.view.message:
                    for child in ctx.view.children:
                        child.disabled = True
                    await ctx.view.message.edit(content="Removed from the queue.", view=ctx.view)
            except Exception:
                pass

    @app_commands.command(name="join_queue", description="Join the rated duel queue.")
    @app_commands.guilds(GUILD)
    async def join_queue(self, interaction: discord.Interaction):
        if not self._in_duel_channel(interaction):
            await interaction.response.send_message("This command can only be used in #duel-arena.", ephemeral=True)
            return

        async with self.lock:
            if interaction.user.id in self.active_pairs:
                await interaction.response.send_message(
                    "You are already paired for a match. Please report your result before rejoining.",
                    ephemeral=True,
                )
                return

            if self._find_queue_entry(interaction.user.id):
                await interaction.response.send_message("You are already in the queue.", ephemeral=True)
                return

            self.queue.append(QueueEntry(user_id=interaction.user.id, joined_at=datetime.utcnow()))

        await interaction.response.send_message("added you to the queue", ephemeral=True)
        if interaction.channel:
            await interaction.channel.send("Someone joined the queue")

        await self._process_queue(interaction.channel, interaction.guild)

    @app_commands.command(name="leave_queue", description="Leave the rated duel queue.")
    @app_commands.guilds(GUILD)
    async def leave_queue(self, interaction: discord.Interaction):
        if not self._in_duel_channel(interaction):
            await interaction.response.send_message("This command can only be used in #duel-arena.", ephemeral=True)
            return

        await self._cancel_pending_for_user(interaction.user.id)

        async with self.lock:
            removed = self._remove_from_queue(interaction.user.id)

        if not removed:
            await interaction.response.send_message("You are not currently in the queue.", ephemeral=True)
            return

        await interaction.response.send_message("removed you from the queue", ephemeral=True)
        if interaction.channel:
            await interaction.channel.send("Someone left the queue")

        await self._process_queue(interaction.channel, interaction.guild)

    async def clear_pairing(self, user_a_id: int, user_b_id: int):
        async with self.lock:
            changed = False
            if self.active_pairs.get(user_a_id) == user_b_id:
                self.active_pairs.pop(user_a_id, None)
                changed = True
            if self.active_pairs.get(user_b_id) == user_a_id:
                self.active_pairs.pop(user_b_id, None)
                changed = True
        return changed


async def setup(bot: commands.Bot):
    await bot.add_cog(DuelQueue(bot))