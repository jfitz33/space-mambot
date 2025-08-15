import io, csv, discord, os
from discord.ext import commands
from discord import app_commands
from core.db import db_get_collection
from core.views import CollectionPaginator

# Set guild ID for development
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

class Collection(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="collection", description="View a paginated collection")
    @app_commands.guilds(GUILD)
    @app_commands.describe(user="User to view (optional)")
    async def collection(self, interaction: discord.Interaction, user: discord.User=None):
        target = user or interaction.user
        rows = db_get_collection(self.bot.state, target.id)
        if not rows:
            await interaction.response.send_message(f"{target.mention} has no cards.", ephemeral=True); return

        paginator = CollectionPaginator(interaction.user, target, rows, page_size=20)
        await interaction.response.send_message(embed=paginator._embed(), view=paginator, ephemeral=True)

    @app_commands.command(name="export_collection", description="Export collection CSV for site import")
    @app_commands.guilds(GUILD)
    @app_commands.describe(user="User to export (optional)")
    async def export_collection(self, interaction: discord.Interaction, user: discord.User=None):
        target = user or interaction.user
        rows = db_get_collection(self.bot.state, target.id)
        if not rows:
            await interaction.response.send_message(f"{target.mention} has no cards.", ephemeral=True); return

        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(["cardname","cardq","cardrarity","card_edition","cardset","cardcode","cardid","print_id"])
        for (name, qty, rarity, cset, code, cid) in rows:
            w.writerow([name, qty, rarity, "1st Edition", cset, code, cid, ""])
        buf.seek(0)
        file = discord.File(fp=io.BytesIO(buf.getvalue().encode("utf-8")), filename=f"{target.id}_collection.csv")
        await interaction.response.send_message(content=f"Export for {target.mention}", file=file, ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(Collection(bot))
