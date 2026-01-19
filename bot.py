# bot.py
import os, asyncio
import discord
import logging
from discord.ext import commands
from discord import app_commands
from dotenv import load_dotenv
from pathlib import Path

from core.state import AppState
from core.db import (
    db_init,
    db_init_trades,
    db_init_wallet,
    db_wallet_migrate_to_mambucks_and_shards_per_set,
    db_init_user_stats,
    db_init_user_set_wins,
    db_init_shard_overrides,
    db_init_daily_sales,
    db_init_wheel_tokens,
)
from core.packs import load_packs_from_csv
from core.starters import load_starters_from_csv
from core.cards_shop import ensure_shop_index, reset_shop_index
from core.images import ensure_rarity_emojis
from core.art_import import download_cardpool_art_from_state
from core.quests.schema import db_init_quests, db_seed_quests_from_json
from core.tins import load_tins_from_json
from core.pack_rewards import PackRewardHelper
from core.constants import TEAM_ROLE_NAMES, TEAM_SETS

logging.basicConfig(
    level=logging.INFO,  # keep most modules at INFO
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logging.getLogger("cogs.tournaments").setLevel(logging.DEBUG)

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")
TOKEN    = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
ART_IMPORT = int(os.getenv("ART_IMPORT", "0") or 0)
DEV_FORCE_CLEAN = os.getenv("DEV_FORCE_CLEAN", "0") == "1"

DB_PATH   = os.getenv("DB_PATH", "collections.sqlite3")
PACKS_DIR = os.getenv("PACKS_DIR", "packs_csv")

# make relative paths project-relative
if not os.path.isabs(DB_PATH):
    DB_PATH = str((BASE_DIR / DB_PATH).resolve())
if not os.path.isabs(PACKS_DIR):
    PACKS_DIR = str((BASE_DIR / PACKS_DIR).resolve())

class MamboCommandTree(app_commands.CommandTree):
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await starter_role_gate(interaction)


class MamboBot(commands.Bot):
    async def setup_hook(self) -> None:
        # Register prefix checks here; slash checks are handled by MamboCommandTree.
        self.add_check(starter_role_gate_prefix)

# Use default intents (message_content not needed for slash cmds, but default avoids warnings)
intents = discord.Intents.default()
intents.members = True           # needed for role.members to populate
bot = MamboBot(command_prefix="!", intents=intents, tree_cls=MamboCommandTree)
tree = bot.tree

bot.state = AppState(db_path="collections.sqlite3", packs_dir="packs_csv")
bot.state.banlist_path = str((BASE_DIR / "data" / "banlist.json").resolve())
bot.state.tins_path = str((BASE_DIR / "data" / "tins.json").resolve())

STARTER_ROLES = TEAM_ROLE_NAMES
TEAM_DISPLAY_ROLE_NAMES = {
    team_cfg.get("display")
    for set_cfg in TEAM_SETS.values()
    for team_cfg in set_cfg.get("teams", {}).values()
    if team_cfg.get("display")
}


def _member_has_starter_role(member: discord.Member | None) -> bool:
    if not member:
        return False
    return any(
        role.name in STARTER_ROLES or role.name in TEAM_DISPLAY_ROLE_NAMES
        for role in getattr(member, "roles", [])
    )


async def _resolve_member(guild: discord.Guild, user: discord.abc.User) -> discord.Member | None:
    member: discord.Member | None = user if isinstance(user, discord.Member) else None
    if member:
        return member

    # Prefer cache when intents are available.
    member = guild.get_member(user.id)
    if member:
        return member

    # Fallback to API; handle missing intent/permissions gracefully by returning None.
    try:
        return await guild.fetch_member(user.id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return None


async def starter_role_gate(interaction: discord.Interaction) -> bool:
    # Allow DM commands to proceed; adjust to False if you want to block DMs.
    if not interaction.guild:
        return True

    # Admins bypass the gate.
    if interaction.user.guild_permissions.administrator:
        return True

    # Let /start through so users can claim their starter role.
    cmd = interaction.command
    if isinstance(cmd, app_commands.Command) and cmd.name == "start":
        return True

    # Check for required starter roles (e.g., Fire/Water) sourced from constants.
    member = await _resolve_member(interaction.guild, interaction.user)
    if _member_has_starter_role(member):
        return True

    message = "Please run `/start` first to receive your starter role (Fire or Water)."
    try:
        await interaction.response.send_message(message, ephemeral=True)
    except discord.InteractionResponded:
        await interaction.followup.send(message, ephemeral=True)
    return False


async def starter_role_gate_prefix(ctx: commands.Context) -> bool:
    # Allow DMs to continue for legacy/prefix commands if any exist.
    if not ctx.guild:
        return True

    # Admins bypass the gate.
    if ctx.author.guild_permissions.administrator:
        return True

    # Reuse the same member/role logic as the slash gate.
    member = await _resolve_member(ctx.guild, ctx.author)
    if _member_has_starter_role(member):
        return True

    await ctx.send("Please run `/start` first to receive your starter role (Fire or Water).", delete_after=10)
    return False

def _select_quests_path(base_dir: Path) -> Path:
    """Choose quests.json for normal mode or quests_week1.json for launch week."""

    # Allow an explicit toggle via env for quick rollout/rollback.
    week1_enabled = os.getenv("DAILY_DUEL_WEEK1_ENABLE", "1") == "1"

    filename = "quests_week1.json" if week1_enabled else "quests.json"
    return (base_dir / "data" / filename).resolve()

# Set to track live views to properly enforce timeouts
setattr(bot.state, "live_views", set())

COGS = ["cogs.system", "cogs.packs", "cogs.collection", 
        "cogs.admin", "cogs.trade", "cogs.start", "cogs.wallet", 
        "cogs.cards_shop", "cogs.gamba", "cogs.quests",
        "cogs.stats", "cogs.boop", "cogs.shop_sim", "cogs.sales",
        "cogs.gamba_chips", "cogs.tournaments", "cogs.teams",
        "cogs.daily_rewards", "cogs.duel_queue", "cogs.timer"]

@bot.event
async def on_member_join(member: discord.Member):
    if GUILD_ID and member.guild.id != GUILD_ID:
        return

    channel = discord.utils.get(member.guild.text_channels, name="welcome")
    if not channel:
        logging.warning("[welcome] #welcome channel not found in guild %s", member.guild.id)
        return

    fire_emoji = discord.utils.get(member.guild.emojis, name="Smug")
    water_emoji = discord.utils.get(member.guild.emojis, name="Mampray")

    message = (
        f"Hi {member.mention}! Welcome to the Nemesis Format server. Rules and info can be found in the appropriate channels. \n"
        f"To begin, you'll need to join the Fire {fire_emoji or ':Smug:'} or Water {water_emoji or ':Mampray:'} teams, and get your starting card pool. "
        "To help you decide you can look at the cards in each pack in ⁠card-pool and the banlist in ⁠banlist. "
        "Use /start command to choose your team. Once you've done so, you'll be dm'd a set of packs by me!"
    )

    try:
        await channel.send(message)
    except discord.Forbidden:
        logging.warning("[welcome] Missing permissions to send message in #welcome for guild %s", member.guild.id)
    except discord.HTTPException as exc:
        logging.warning("[welcome] Failed to send welcome message in guild %s: %s", member.guild.id, exc)
        
@bot.event
async def on_ready():
    # 1) Core init
    db_init(bot.state)
    db_init_trades(bot.state)
    await db_init_quests(bot.state)
    quests_json_path = _select_quests_path(BASE_DIR)
    bot.state.quests_json_path = str(quests_json_path)
    await db_seed_quests_from_json(bot.state, str(quests_json_path), deactivate_missing=True)
    db_init_user_stats(bot.state)
    db_init_user_set_wins(bot.state)
    load_packs_from_csv(bot.state)
    bot.state.starters_dir = "starters_csv"  # put your starter CSVs here
    bot.state.shop = PackRewardHelper(bot.state, bot)
    load_starters_from_csv(bot.state)
    load_tins_from_json(bot.state, bot.state.tins_path)
    db_init_wallet(bot.state)
    db_init_wheel_tokens(bot.state)
    await db_wallet_migrate_to_mambucks_and_shards_per_set(bot.state)
    await asyncio.to_thread(db_init_shard_overrides, bot.state)
    await asyncio.to_thread(db_init_daily_sales, bot.state)

    # Cache rarity emoji IDs (auto-creates from /images/rarity_logos if missing)
    try:
        gids = [GUILD_ID] if GUILD_ID else [g.id for g in bot.guilds]
        await ensure_rarity_emojis(bot, guild_ids=gids, create_if_missing=True, verbose=True, refresh=False)
        print("[rarity] cached emoji IDs:", getattr(bot.state, "rarity_emoji_ids", {}))
    except Exception as e:
        print("[rarity] setup skipped:", e)

    # After import of cards, check for improper entries and purge incorrect ones
    reset_shop_index(bot.state)
    ensure_shop_index(bot.state)

    # If Art Import env var set to 1, download card images (super and higher)
    async def prefetch_art():
        try:
            await asyncio.to_thread(download_cardpool_art_from_state, bot.state)
        except Exception as e:
            print("[art] prefetch failed (continuing without art):", e)
    
    if (ART_IMPORT == 1):
        await prefetch_art()

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
