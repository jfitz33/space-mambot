import discord, os
from discord.ext import commands
from discord import app_commands
from core.views import PacksSelectView
from core.constants import BOX_COST, PACK_COST

# Set guild ID for development
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None
MAX_PACKS = 10
MIN_PACKS = 1
PACKS_IN_BOX = 24

class Packs(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state = bot.state

    @app_commands.command(name="packlist", description="List available pack types")
    @app_commands.guilds(GUILD)
    async def packlist(self, interaction: discord.Interaction):
        names = sorted((self.bot.state.packs_index or {}).keys())
        if not names:
            await interaction.response.send_message("No packs found. Load CSVs and /reload_data.", ephemeral=True); return
        desc = "\n".join(f"• `{n}`" for n in names[:25])
        await interaction.response.send_message(embed=discord.Embed(title="Available Packs", description=desc, color=0x2b6cb0), ephemeral=True)

    @app_commands.command(name="pack", description="Open packs via dropdown")
    @app_commands.guilds(GUILD)
    @app_commands.describe(amount="How many packs (1-10)")
    async def pack(self, interaction: discord.Interaction, amount: app_commands.Range[int,MIN_PACKS,MAX_PACKS]=1):
        if not self.bot.state.packs_index:
            await interaction.response.send_message("No packs found. Load CSVs and /reload_data.", ephemeral=True); return
        view = PacksSelectView(self.bot.state, requester=interaction.user, amount=amount)
        await interaction.response.send_message("Pick a pack from the dropdown:", view=view, ephemeral=True)

    @app_commands.command(name="box", description=f"Open a sealed box (24 packs; costs **{BOX_COST}** mambucks).")
    @app_commands.guilds(GUILD)
    async def box(self, interaction: discord.Interaction):
        import inspect
        print("PacksSelectView from:", PacksSelectView.__module__)
        print("Ctor:", inspect.signature(PacksSelectView.__init__))
        view = PacksSelectView(self.bot.state, requester=interaction.user, amount=PACKS_IN_BOX, mode="box")
        await interaction.response.send_message(
            "Pick a pack set for your **box**:", view=view, ephemeral=True
        )

async def setup(bot: commands.Bot):
    await bot.add_cog(Packs(bot))
