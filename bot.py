# bot.py
import os
import discord
from discord.ext import commands
from discord import app_commands
from dotenv import load_dotenv

from core.state import AppState
from core.db import db_init, db_init_trades
from core.packs import load_packs_from_csv

load_dotenv()
TOKEN    = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
DEV_FORCE_CLEAN = os.getenv("DEV_FORCE_CLEAN", "0") == "1"

# Use default intents (message_content not needed for slash cmds, but default avoids warnings)
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

bot.state = AppState(db_path="collections.sqlite3", packs_dir="packs_csv")

COGS = ["cogs.system", "cogs.packs", "cogs.collection", "cogs.admin"]

@bot.event
async def on_ready():
    # 1) Core init
    db_init(bot.state)
    db_init_trades(bot.state)
    load_packs_from_csv(bot.state)

    # 2) Load cogs BEFORE syncing
    for ext in COGS:
        try:
            await bot.load_extension(ext)
            print(f"[cogs] loaded {ext}")
        except Exception as e:
            print(f"[cogs] FAILED {ext}: {e}")

    # 3) (Optional during dev) clear any stale commands, then guild-sync
    if DEV_FORCE_CLEAN:
        try:
            print("[sync] clearing GLOBAL commands…")
            tree.clear_commands(guild=None)
            await tree.sync(guild=None)
            print("[sync] GLOBAL cleared")
        except Exception as e:
            print("[sync] global clear failed:", e)

        if GUILD_ID:
            try:
                print(f"[sync] clearing GUILD {GUILD_ID} commands…")
                tree.clear_commands(guild=discord.Object(id=GUILD_ID))
                await tree.sync(guild=discord.Object(id=GUILD_ID))
                print("[sync] GUILD cleared")
            except Exception as e:
                print("[sync] guild clear failed:", e)

    # 4) Final sync to your dev guild for instant availability
    if GUILD_ID:
        await tree.sync(guild=discord.Object(id=GUILD_ID))
        cmds = await tree.fetch_commands(guild=discord.Object(id=GUILD_ID))
        print("[sync] guild commands:", [f"{c.name} ({c.type})" for c in cmds], "count:", len(cmds))
        print(f"Slash commands synced to guild {GUILD_ID}")
    else:
        await tree.sync()
        print("Slash commands globally synced (may take a while)")

    # 5) Quick visibility: list guilds
    print("In guilds:", [g.id for g in bot.guilds])
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")
    
if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("DISCORD_TOKEN missing in .env")
    bot.run(TOKEN)
