import os
import random
import discord
from discord.ext import commands
from discord import app_commands
from pathlib import Path

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
    "A boop a day keeps the... uhm... I forget what I was gonna say 😳",
    "Don't touch me I'm sterile!",
    "Ba da da da da da da. Tequila! 🍹",
    "You booped me! Norway!!!",
    "Pssst, I'm secretly rooting for the fire team. Don't tell anyone!",
    "Pssst, I'm secretly rooting for the water team. Don't tell anyone!",
    "I'm blue, da ba dee da ba di 🎵",
    "Fear is the mind-killer.",
    "“I witnessed the primordial fire triumph over the depths of the sea in a war long past… wait, what was I saying?",
    "I bet knightblader can’t guess what I’m gonna say next :)",
    "Wind team has nothing on the desert sands, if you ask me 😏",
    "Earth team has nothing on the arid peaks, if you ask me 😏",
    "Oh no, a sandstorm! I wonder who’s duel caused that :o",
    "“I hope there’s an oasis somewhere close by, this battleground drying my scales out :(",
    "HEY! Boop ME, not the cactus!",
    "These desert sands… much like the sea beds of the past and the sands of.. Huh.",
    "Oh, I love that movie! Gone with the, uh, what was it again?",
    "Skies are clear today folks soooooo that should lead to clear flying. If you could uhhhhhhh just put your tray tables up, and uhhhhhh sit in the full upright position... we'll be airborne soon. Thank you as always for flying Space Mambo",
    "I tried to boop a tornado and it was mean :(",
    "Still waiting for Dmitri’s topping list"
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
