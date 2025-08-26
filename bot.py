# bot.py
import os
import discord
from discord.ext import commands
from discord import app_commands
from dotenv import load_dotenv

from core.state import AppState
from core.db import db_init, db_init_trades, db_init_wallet, db_wallet_migrate_to_mambucks_and_shards_per_set
from core.packs import load_packs_from_csv
from core.starters import load_starters_from_csv
from core.cards_shop import ensure_shop_index
from core.images import ensure_rarity_emojis
from core.art_import import download_high_rarity_art_from_state
from core.quests.schema import db_init_quests, db_seed_example_quests

load_dotenv()
TOKEN    = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
ART_IMPORT = int(os.getenv("ART_IMPORT", "0") or 0)
DEV_FORCE_CLEAN = os.getenv("DEV_FORCE_CLEAN", "0") == "1"

# Use default intents (message_content not needed for slash cmds, but default avoids warnings)
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

bot.state = AppState(db_path="collections.sqlite3", packs_dir="packs_csv")

# Set to track live views to properly enforce timeouts
setattr(bot.state, "live_views", set())

COGS = ["cogs.system", "cogs.packs", "cogs.collection", "cogs.admin", "cogs.trade", "cogs.start", "cogs.wallet", "cogs.cards_shop", "cogs.wheel", "cogs.quests"]

@bot.event
async def on_ready():
    # 1) Core init
    db_init(bot.state)
    db_init_trades(bot.state)
    await db_init_quests(bot.state)
    await db_seed_example_quests(bot.state)
    load_packs_from_csv(bot.state)
    bot.state.starters_dir = "starters_csv"  # put your starter CSVs here
    load_starters_from_csv(bot.state)
    db_init_wallet(bot.state)
    await db_wallet_migrate_to_mambucks_and_shards_per_set(bot.state)
    print("Wallet migration complete: mambucks=pack currency, shards per set ready")

    # Cache rarity emoji IDs (auto-creates from /images/rarity_logos if missing)
    try:
        gids = [GUILD_ID] if GUILD_ID else [g.id for g in bot.guilds]
        await ensure_rarity_emojis(bot, guild_ids=gids, create_if_missing=True, verbose=True)
        print("[rarity] cached emoji IDs:", getattr(bot.state, "rarity_emoji_ids", {}))
    except Exception as e:
        print("[rarity] setup skipped:", e)

    # After import of cards, check for improper entries and purge incorrect ones
    for attr in ("_shop_print_by_key", "_shop_sig_to_set"):
        if hasattr(bot.state, attr):
            delattr(bot.state, attr)
    ensure_shop_index(bot.state)

    # If Art Import env var set to 1, download card images (super and higher)
    if (ART_IMPORT == 1):
        download_high_rarity_art_from_state(bot.state)

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
