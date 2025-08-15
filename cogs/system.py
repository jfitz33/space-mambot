import discord, os
from discord.ext import commands
from discord import app_commands
from core.packs import load_packs_from_csv

# Set guild ID for development
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

class System(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="ping", description="Bot up?")
    @app_commands.guilds(GUILD)
    async def ping(self, interaction: discord.Interaction):
        await interaction.response.send_message("Pong!", ephemeral=True)

    @app_commands.command(name="reload_data", description="Reload CSV packs from disk")
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def reload_data(self, interaction: discord.Interaction):
        try:
            load_packs_from_csv(self.bot.state)
            await interaction.response.send_message("CSV packs reloaded.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"Reload failed: {e}", ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(System(bot))
