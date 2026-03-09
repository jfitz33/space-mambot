import os
import random
import discord
from discord.ext import commands
from discord import app_commands
from pathlib import Path

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

BOOP_LINES = [
    "Ive been booped! 😮",
    "Awww you little rascal you 😆",
    "Certified boop moment™",
    "Boop achieved. Systems nominal. ✅",
    "Did datgingah put you up to this?",
    "✨ *boop intensifies* ✨",
    "Howdy friend 🤠",
    "Do you mind? I was busy being a bot and stuff 😒",
    "55 BURGERS 55 FRIES 🍔🍟",
    "A boop a day keeps the... uhm... I forget what I was gonna say 😳",
    "Don't touch me I'm sterile!",
    "Ba da da da da da da. Tequila! 🍹",
    "You booped me! Norway!!!",
    "I'm blue, da ba dee da ba di 🎵",
    "Fear is the mind-killer.",
    "“I witnessed the primordial fire triumph over the depths of the sea in a war long past… wait, what was I saying?",
    "I bet knightblader can’t guess what I’m gonna say next :)",
    "!esrever ni kcuts got i pleH",
    "Beep boop",
    "I swim in the ocean of the 4th dimension. Yesterday? Tomorrow? It is no different to me than left or right, up or down.",
    "I made the desert sands into a nice little hourglass :3",
    "Still waiting for Dmitri’s topping list",
]

def boop_image_path() -> Path:
    """
    Resolve images/misc/boop.png robustly:
    1) relative to current working dir
    2) relative to repo root (one level above cogs/)
    """
    p1 = Path("images/misc/boop.png")
    if p1.is_file():
        return p1
    # repo_root / images / misc / boop.png (cogs/ -> repo root)
    p2 = Path(__file__).resolve().parents[1] / "images" / "misc" / "boop.png"
    return p2


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
        embed = discord.Embed(title="Boop!", description=line, color=0x2b6cb0)

        img_path = boop_image_path()
        if img_path.is_file():
            file = discord.File(str(img_path), filename="boop.png")
            embed.set_image(url="attachment://boop.png")
            await interaction.response.send_message(embed=embed, file=file)
        else:
            # graceful fallback
            embed.set_image(url=bot_user.display_avatar.url)
            await interaction.response.send_message(
                content="(boop image not found; showing avatar instead)",
                embed=embed
            )

async def setup(bot: commands.Bot):
    await bot.add_cog(Boop(bot))
