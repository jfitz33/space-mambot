import discord, os
from discord.ext import commands
from discord import app_commands
from core.packs import load_packs_from_csv
from core.tins import load_tins_from_json
from core.cards_shop import reset_shop_index, ensure_shop_index

# Set guild ID for development
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

class System(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

async def setup(bot: commands.Bot):
    await bot.add_cog(System(bot))
