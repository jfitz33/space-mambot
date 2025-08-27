import os
import random
import discord
from discord.ext import commands
from discord import app_commands

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

BOOP_LINES = [
    "Hey that hurt! 😖",
    "Ive been booped! 😮",
    "Awww you little rascal you 😆",
    "Certified boop moment™",
    "Boop achieved. Systems nominal. ✅",
    "Did datgingah put you up to this?",
    "✨ *boop intensifies* ✨",
    "Howdy friend 🤠",
    "Do you mind? I was busy being a bot and stuff 😒",
    "55 BURGERS 55 FRIES 🍔🍟",
    "Sorry not in the booping mood today 😔",
]

class Boop(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="boop", description="Give the bot a lil boop on the snoot")
    @app_commands.guilds(GUILD)
    async def boop(self, interaction: discord.Interaction):
        bot_user = interaction.client.user
        if bot_user is None:
            await interaction.response.send_message("Uh oh, I misplaced my face. Try again?", ephemeral=True)
            return

        line = random.choice(BOOP_LINES)
        avatar_url = bot_user.display_avatar.url

        embed = discord.Embed(
            title="Boop!",
            description=line,
            color=0x2b6cb0,
        )
        embed.set_image(url=avatar_url)

        await interaction.response.send_message(embed=embed)

async def setup(bot: commands.Bot):
    await bot.add_cog(Boop(bot))
