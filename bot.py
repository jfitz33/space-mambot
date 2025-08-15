# bot.py
import os
import io
import csv
import json
import random
import sqlite3
from collections import defaultdict, Counter
from typing import Dict, List, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

# =========================
# Config & constants
# =========================

# === CSV packs ===
PACKS_CSV_DIR = "packs_csv"

# In-memory:
PACKS_INDEX = {}  # pack_name -> { "by_rarity": {rarity: [ {name,rarity,card_code,card_id,weight}, ... ] } }
RARITY_ORDER = ["secret","ultra","super","rare","uncommon","common"]

DB_PATH    = "collections.sqlite3"
RARITY_ORDER = ["secret", "ultra", "super", "rare", "uncommon", "common"]

load_dotenv()
TOKEN    = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD    = discord.Object(id=GUILD_ID) if GUILD_ID else None

# During dev, set DEV_FORCE_CLEAN=1 in .env to purge global commands and sync only to your guild
DEV_FORCE_CLEAN = os.getenv("DEV_FORCE_CLEAN", "0") == "1"

intents = discord.Intents.none()
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

# In-memory datasets
cards_list: List[dict] = []
packs: Dict[str, dict] = {}
cards_by_rarity: Dict[str, List[dict]] = {}

# =========================
# Database layer
# =========================
def db_init():
    """
    Store each user's collection with the exact fields you requested:
      user_id, card_name, card_qty, card_rarity, card_set, card_code, card_id

    Primary key is (user_id, card_name, card_rarity, card_set, card_code, card_id)
    so the same card can exist in different sets or codes without clobbering.
    """
    with sqlite3.connect(DB_PATH) as conn, conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS user_collection (
            user_id     TEXT NOT NULL,
            card_name   TEXT NOT NULL,
            card_qty    INTEGER NOT NULL DEFAULT 0,
            card_rarity TEXT NOT NULL,
            card_set    TEXT NOT NULL,
            card_code   TEXT,
            card_id     TEXT,
            PRIMARY KEY (user_id, card_name, card_rarity, card_set, card_code, card_id)
        );
        """)

# ---- DB helpers ----
def db_add_cards(user_id: int, items: List[dict], pack_name: str):
    """
    Upsert a batch of pulled cards into the user's collection.
    Each item dict must contain: name, rarity, (optional) card_code, card_id.
    We snapshot the pack_name into card_set.
    """
    user_id_s = str(user_id)
    # compress identical rows (same fields) to a qty sum
    key_fields = ("name", "rarity", "card_code", "card_id")
    counter = Counter()
    for it in items:
        key = (it.get("name",""), it.get("rarity","").lower(), it.get("card_code",""), it.get("card_id",""))
        counter[key] += 1

    with sqlite3.connect(DB_PATH) as conn, conn:
        for (name, rarity, card_code, card_id), add_qty in counter.items():
            conn.execute("""
            INSERT INTO user_collection (user_id, card_name, card_qty, card_rarity, card_set, card_code, card_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, card_name, card_rarity, card_set, card_code, card_id)
            DO UPDATE SET card_qty = card_qty + excluded.card_qty;
            """, (user_id_s, name, add_qty, rarity, pack_name, card_code, card_id))

def db_clear_collection(user_id: int) -> int:
    with sqlite3.connect(DB_PATH) as conn, conn:
        cur = conn.execute("DELETE FROM user_collection WHERE user_id = ?", (str(user_id),))
        return cur.rowcount

def db_get_collection(user_id: int):
    """
    Return rows sorted by rarity (custom order), then name. Each row is:
      (card_name, card_qty, card_rarity, card_set, card_code, card_id)
    """
    user_id_s = str(user_id)
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("""
        SELECT card_name, card_qty, card_rarity, card_set, COALESCE(card_code,''), COALESCE(card_id,'')
        FROM user_collection
        WHERE user_id = ?
        ORDER BY
          CASE LOWER(card_rarity)
            WHEN 'secret' THEN 1
            WHEN 'ultra'  THEN 2
            WHEN 'super'  THEN 3
            WHEN 'rare'   THEN 4
            WHEN 'uncommon' THEN 5
            WHEN 'common' THEN 6
            ELSE 999
          END,
          card_name COLLATE NOCASE ASC,
          card_set COLLATE NOCASE ASC;
        """, (user_id_s,))
        return c.fetchall()

# (Optional) ensure the table exists if you don't already do so at startup
def db_init_user_collection_if_needed():
    with sqlite3.connect(DB_PATH) as conn, conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS user_collection (
            user_id     TEXT NOT NULL,
            card_name   TEXT NOT NULL,
            card_qty    INTEGER NOT NULL DEFAULT 0,
            card_rarity TEXT NOT NULL,
            card_set    TEXT NOT NULL,
            card_code   TEXT,
            card_id     TEXT,
            PRIMARY KEY (user_id, card_name, card_rarity, card_set, card_code, card_id)
        );
        """)

def db_admin_add_card(user_id: int, *, name: str, rarity: str, card_set: str, card_code: str, card_id: str, qty: int) -> int:
    rarity = (rarity or "").strip().lower()
    user_s = str(user_id)
    qty = max(1, int(qty))
    with sqlite3.connect(DB_PATH) as conn, conn:
        conn.execute("""
            INSERT INTO user_collection (user_id, card_name, card_qty, card_rarity, card_set, card_code, card_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, card_name, card_rarity, card_set, card_code, card_id)
            DO UPDATE SET card_qty = card_qty + excluded.card_qty;
        """, (user_s, name, qty, rarity, card_set, card_code or "", card_id or ""))
        cur = conn.execute("""
            SELECT card_qty FROM user_collection
            WHERE user_id=? AND card_name=? AND card_rarity=? AND card_set=?
              AND COALESCE(card_code,'')=COALESCE(?,'') AND COALESCE(card_id,'')=COALESCE(?,'');
        """, (user_s, name, rarity, card_set, card_code or "", card_id or ""))
        row = cur.fetchone()
        return int(row[0]) if row else qty

def db_admin_remove_card(user_id: int, *, name: str, rarity: str, card_set: str, card_code: str, card_id: str, qty: int) -> Tuple[int, int]:
    rarity = (rarity or "").strip().lower()
    user_s = str(user_id)
    qty = max(1, int(qty))
    with sqlite3.connect(DB_PATH) as conn, conn:
        cur = conn.execute("""
            SELECT card_qty FROM user_collection
            WHERE user_id=? AND card_name=? AND card_rarity=? AND card_set=?
              AND COALESCE(card_code,'')=COALESCE(?,'') AND COALESCE(card_id,'')=COALESCE(?,'');
        """, (user_s, name, rarity, card_set, card_code or "", card_id or ""))
        row = cur.fetchone()
        if not row:
            return (0, 0)
        current = int(row[0])
        new_qty = current - qty
        removed = min(qty, current)
        if new_qty > 0:
            conn.execute("""
                UPDATE user_collection
                SET card_qty = ?
                WHERE user_id=? AND card_name=? AND card_rarity=? AND card_set=?
                  AND COALESCE(card_code,'')=COALESCE(?,'') AND COALESCE(card_id,'')=COALESCE(?,'');
            """, (new_qty, user_s, name, rarity, card_set, card_code or "", card_id or ""))
            return (removed, new_qty)
        else:
            conn.execute("""
                DELETE FROM user_collection
                WHERE user_id=? AND card_name=? AND card_rarity=? AND card_set=?
                  AND COALESCE(card_code,'')=COALESCE(?,'') AND COALESCE(card_id,'')=COALESCE(?,'');
            """, (user_s, name, rarity, card_set, card_code or "", card_id or ""))
            return (removed, 0)

# =========================
# Data loading (JSON)
# =========================
PACKS_CSV_DIR = "packs_csv"
PACKS_INDEX = {}  # pack_name -> {"by_rarity": {rarity: [ {name, rarity, card_code, card_id, weight}, ... ]}}

def load_packs_from_csv(folder_path: str = PACKS_CSV_DIR):
    """
    Reads all .csv files in folder and builds PACKS_INDEX.
    CSV header (case/whitespace-insensitive) must include exactly these columns:
      cardname, cardq, cardrarity, card_edition, cardset, cardcode, cardid, print_id
    """
    global PACKS_INDEX
    PACKS_INDEX = {}

    required = ["cardname","cardq","cardrarity","card_edition","cardset","cardcode","cardid","print_id"]

    for fname in os.listdir(folder_path):
        if not fname.lower().endswith(".csv"):
            continue
        fpath = os.path.join(folder_path, fname)

        # Use utf-8-sig to auto-strip BOM from the first header if present
        with open(fpath, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)

            if not reader.fieldnames:
                raise ValueError(f"{fname}: no header row found")

            # Build a map of normalized header -> original header
            original_headers = reader.fieldnames
            norm_headers = [ (h or "").strip().lower() for h in original_headers ]
            header_map = { nh: oh for nh, oh in zip(norm_headers, original_headers) }

            missing = [h for h in required if h not in header_map]
            if missing:
                # help user see what we actually detected
                found_display = ", ".join(original_headers)
                raise ValueError(
                    f"{fname}: missing required column(s): {missing}. "
                    f"Found columns: [{found_display}]"
                )

            # Parse rows
            temp_index = {}  # pack_name -> defaultdict(list)
            for row in reader:
                # Access using original names via header_map
                get = lambda key: (row.get(header_map[key]) or "").strip()

                pack_name = get("cardset")
                if not pack_name:
                    # fallback to filename base if cardset cell is blank
                    pack_name = os.path.splitext(fname)[0]

                name   = get("cardname")
                rarity = normalize_rarity(get("cardrarity"))
                code   = get("cardcode")
                cid    = get("cardid")

                # quantity as weight
                q_raw = get("cardq")
                try:
                    weight = int(q_raw)
                except:
                    weight = 1
                weight = max(1, weight)

                by_rarity = temp_index.setdefault(pack_name, defaultdict(list))
                by_rarity[rarity].append({
                    "name": name,
                    "rarity": rarity,
                    "card_code": code,
                    "card_id": cid,
                    "weight": weight,
                })

            # Commit packs from this file
            for pack_name, by_rarity in temp_index.items():
                entry = PACKS_INDEX.setdefault(pack_name, {"by_rarity": defaultdict(list)})
                # merge lists per rarity
                for r, items in by_rarity.items():
                    entry["by_rarity"][r].extend(items)

    # finalize: convert defaultdicts to dicts
    for p in PACKS_INDEX.values():
        p["by_rarity"] = {k: v for k, v in p["by_rarity"].items()}

def _weighted_pick(items):
    """
    items: [ { ... , weight:int }, ... ] -> returns one dict by weight.
    """
    weights = [max(1, it["weight"]) for it in items]
    return random.choices(items, weights=weights, k=1)[0]

def _fallback_pool(by_rarity, preferred: list[str]):
    # Return the first non-empty rarity pool from preferred list,
    # else flatten all into one list.
    for r in preferred:
        pool = by_rarity.get(r, [])
        if pool:
            return pool
    # absolute fallback
    flat = []
    for v in by_rarity.values():
        flat.extend(v)
    return flat

RARITY_MAP = {
    "common": "common",
    "rare": "rare",
    "super rare": "super",
    "ultra rare": "ultra",
    "secret rare": "secret",
}
def normalize_rarity(s: str) -> str:
    return RARITY_MAP.get((s or "").strip().lower(), "rare")

# New pack opening function
def open_pack_from_csv(pack_name: str, amount: int = 1) -> list[dict]:
    """
    Returns a list of pulled card dicts:
      { name, rarity, card_code, card_id }
    Pack odds: 7 commons, 1 rare, 1 super-or-better (72% super, 25% ultra, 3% secret).
    """
    if pack_name not in PACKS_INDEX:
        raise ValueError(f"Unknown pack '{pack_name}'. Use /packlist.")

    by_rarity = PACKS_INDEX[pack_name]["by_rarity"]
    pulls = []

    for _ in range(amount):
        # 7 commons
        common_pool = by_rarity.get("common")
        pool = common_pool if common_pool else _fallback_pool(by_rarity, ["uncommon","rare","super","ultra","secret"])
        for _i in range(7):
            item = _weighted_pick(pool)
            pulls.append({"name": item["name"], "rarity": item["rarity"], "card_code": item["card_code"], "card_id": item["card_id"]})

        # 1 rare
        rare_pool = by_rarity.get("rare")
        pool = rare_pool if rare_pool else _fallback_pool(by_rarity, ["super","ultra","secret","uncommon","common"])
        item = _weighted_pick(pool)
        pulls.append({"name": item["name"], "rarity": item["rarity"], "card_code": item["card_code"], "card_id": item["card_id"]})

        # 1 super-or-better: 72% super, 25% ultra, 3% secret
        roll = random.random()
        if roll < 0.03:
            target = "secret"
        elif roll < 0.28:
            target = "ultra"
        else:
            target = "super"
        pref = {"secret":["secret","ultra","super","rare","uncommon","common"],
                "ultra":["ultra","super","rare","uncommon","common","secret"],
                "super":["super","rare","uncommon","common","ultra","secret"]}[target]
        pool = _fallback_pool(by_rarity, pref)
        item = _weighted_pick(pool)
        pulls.append({"name": item["name"], "rarity": item["rarity"], "card_code": item["card_code"], "card_id": item["card_id"]})

    return pulls

# =========================
# UI Helpers: Pagination
# =========================
def build_collection_lines(rows):
    """
    rows -> (card_name, card_qty, card_rarity, card_set, card_code, card_id)
    Build display lines grouped/sorted by rarity -> name.
    """
    rarity_rank = {r:i for i,r in enumerate(RARITY_ORDER)}
    sorted_rows = sorted(
        rows,
        key=lambda r: (rarity_rank.get(r[2].lower(), 999), r[0].lower(), r[3].lower())
    )
    lines = [f"x{qty} — **{name}** *(rarity: {rarity}, set: {cset})*"
             for (name, qty, rarity, cset, _code, _cid) in sorted_rows]
    return lines

def chunk_lines(lines, per_page=15):
    for i in range(0, len(lines), per_page):
        yield lines[i:i+per_page]

from discord.ui import View, button, Button

class CollectionPaginatorView(View):
    def __init__(self, target_user, lines, per_page=15, timeout=120):
        super().__init__(timeout=timeout)
        self.target_user = target_user
        self.lines = lines
        self.pages = list(chunk_lines(lines, per_page=per_page))
        self.total_pages = max(1, len(self.pages))
        self.index = 0
        if self.total_pages == 1:
            for child in self.children:
                if isinstance(child, Button):
                    child.disabled = True

    def _make_embed(self):
        page = self.pages[self.index] if self.pages else []
        body = "\n".join(page) if page else "_No cards yet._"
        footer = f"\n\nPage {self.index+1}/{self.total_pages} • {len(self.lines)} rows"
        title = f"{self.target_user.display_name}'s Collection"
        embed = discord.Embed(title=title, description=body+footer, color=0x38a169)
        return embed

    @button(label="◀️ Prev", style=discord.ButtonStyle.secondary)
    async def prev_page(self, interaction: discord.Interaction, _: Button):
        self.index = (self.index - 1) % self.total_pages
        await interaction.response.edit_message(embed=self._make_embed(), view=self)

    @button(label="Next ▶️", style=discord.ButtonStyle.secondary)
    async def next_page(self, interaction: discord.Interaction, _: Button):
        self.index = (self.index + 1) % self.total_pages
        await interaction.response.edit_message(embed=self._make_embed(), view=self)

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True

# ---------- Autocomplete helpers ----------
def _ac_pack_names(prefix: str) -> List[str]:
    prefix = (prefix or "").lower()
    names = sorted((PACKS_INDEX or {}).keys())
    if not prefix:
        return names[:25]
    return [n for n in names if prefix in n.lower()][:25]

def _ac_card_names_for_set(card_set: str, prefix: str) -> List[str]:
    prefix = (prefix or "").lower()
    candidates = set()
    if card_set and PACKS_INDEX and card_set in PACKS_INDEX:
        by_rarity = PACKS_INDEX[card_set]["by_rarity"]
        for items in by_rarity.values():
            for it in items:
                candidates.add(it["name"])
    else:
        for p in (PACKS_INDEX or {}).values():
            for items in p["by_rarity"].values():
                for it in items:
                    candidates.add(it["name"])
    names = sorted(candidates)
    if not prefix:
        return names[:25]
    return [n for n in names if prefix in n.lower()][:25]

def _read_option(interaction: discord.Interaction, name: str) -> str:
    data = getattr(interaction, "data", {}) or {}
    opts = {opt.get("name"): opt.get("value") for opt in data.get("options", []) if isinstance(opt, dict)}
    return (opts.get(name) or "").strip()

async def ac_card_set(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    return [app_commands.Choice(name=n, value=n) for n in _ac_pack_names(current)]

async def ac_card_name(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    selected_set = _read_option(interaction, "card_set") or _read_option(interaction, "cardset")
    names = _ac_card_names_for_set(selected_set, current)
    return [app_commands.Choice(name=n, value=n) for n in names]

# ---------- Resolver ----------
class ResolveError(Exception):
    pass

def resolve_card_from_pack(card_set: str, card_name: str, card_code: str = "", card_id: str = "") -> dict:
    """
    Return a single item dict from PACKS_INDEX[card_set] for card_name (and optional code/id).
    Item has keys: name, rarity, card_code, card_id, weight, (maybe image_url).
    Raises ResolveError with a clear message if none/ambiguous.
    """
    if not PACKS_INDEX or card_set not in PACKS_INDEX:
        raise ResolveError(f"Set '{card_set}' not found. Reload CSVs or check /packlist.")

    # collect all candidates across rarities within the set
    candidates = []
    by_rarity = PACKS_INDEX[card_set]["by_rarity"]
    for items in by_rarity.values():
        for it in items:
            if it["name"] == card_name:
                candidates.append(it)

    if not candidates:
        raise ResolveError(f"Card '{card_name}' not found in set '{card_set}'.")

    # optional narrowing by code/id
    if card_code:
        candidates = [it for it in candidates if (it.get("card_code") or "") == card_code]
        if not candidates:
            raise ResolveError(f"No '{card_name}' in '{card_set}' with code '{card_code}'.")

    if card_id:
        candidates = [it for it in candidates if (it.get("card_id") or "") == card_id]
        if not candidates:
            raise ResolveError(f"No '{card_name}' in '{card_set}' with id '{card_id}'.")

    # if still multiple, see if they actually differ (e.g., same name appears with multiple rarities/prints)
    distinct = {(it.get("rarity",""), it.get("card_code",""), it.get("card_id","")) for it in candidates}
    if len(distinct) > 1:
        # Ask admin to specify code or id to disambiguate
        raise ResolveError(
            "Multiple prints match that name in this set. Please specify card_code or card_id."
        )

    return candidates[0]

# =========================
# Commands
# =========================
@tree.command(name="packlist", description="List available pack types", guild=GUILD)
async def packlist(interaction: discord.Interaction):
    names = sorted(PACKS_INDEX.keys())
    if not names:
        await interaction.response.send_message("No packs found in packs_csv/.", ephemeral=True)
        return
    desc = "\n".join(f"• `{n}`" for n in names[:25])
    await interaction.response.send_message(
        embed=discord.Embed(title="Available Packs", description=desc, color=0x2b6cb0),
        ephemeral=True
    )


# ----- Dropdown (select) to choose pack -----

# --- imports (near top if not present) ---
from collections import Counter
from discord.ui import View, Select, button, Button

# ---- formatting helpers ----
RARITY_ORDER = ["secret","ultra","super","rare","uncommon","common"]
def _rank(r: str) -> int:
    return {"secret":1,"ultra":2,"super":3,"rare":4,"uncommon":5,"common":6}.get((r or "").lower(), 999)

def format_pack_lines(pulls_for_one_pack: list[dict]) -> list[str]:
    """
    pulls_for_one_pack: list of {name, rarity, card_code, card_id}
    Returns compacted, sorted lines: xN — NAME (rarity)
    """
    counts = Counter((c["name"], c["rarity"]) for c in pulls_for_one_pack)
    lines = [
        f"x{qty} — **{name}** *(rarity: {rarity})*"
        for (name, rarity), qty in sorted(
            counts.items(),
            key=lambda kv: (_rank(kv[0][1]), kv[0][0].lower())
        )
    ]
    return lines

# ---- paginator view: one pack per page ----
class PackResultsPaginator(View):
    def __init__(self, requester: discord.User, pack_name: str, per_pack_pulls: list[list[dict]], timeout: float = 120):
        """
        per_pack_pulls = [ [pack1 cards...], [pack2 cards...], ... ]
        """
        super().__init__(timeout=timeout)
        self.requester = requester
        self.pack_name = pack_name
        self.per_pack_pulls = per_pack_pulls
        self.total = len(per_pack_pulls)
        self.index = 0  # current page (0-based)

    def _embed_for_index(self) -> discord.Embed:
        lines = format_pack_lines(self.per_pack_pulls[self.index])
        body = "\n".join(lines)
        footer = f"\n\nPack {self.index+1} of {self.total}"
        title = f"{self.requester.display_name} opened `{self.pack_name}`"
        return discord.Embed(title=title, description=(body + footer), color=0x2b6cb0)

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True

    @button(label="◀️ Prev", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction: discord.Interaction, _: Button):
        if interaction.user.id != self.requester.id:
            await interaction.response.send_message("Only the requester can use these controls.", ephemeral=True)
            return
        self.index = (self.index - 1) % self.total
        await interaction.response.edit_message(embed=self._embed_for_index(), view=self)

    @button(label="Next ▶️", style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction: discord.Interaction, _: Button):
        if interaction.user.id != self.requester.id:
            await interaction.response.send_message("Only the requester can use these controls.", ephemeral=True)
            return
        self.index = (self.index + 1) % self.total
        await interaction.response.edit_message(embed=self._embed_for_index(), view=self)

# ---- dropdown to choose a pack; now shows paginated per-pack pages ----
class PacksSelect(Select):
    def __init__(self, requester: discord.User, amount: int):
        self.requester = requester
        self.amount = amount

        pack_names = sorted(PACKS_INDEX.keys())[:25]  # up to 25 options
        options = [discord.SelectOption(label=n, description="Open this pack", value=n) for n in pack_names]
        super().__init__(placeholder="Choose a pack…", min_values=1, max_values=1, options=options, disabled=(len(options)==0))

    async def callback(self, interaction: discord.Interaction):
        # Restrict use to the requester
        if interaction.user.id != self.requester.id:
            await interaction.response.send_message("Only the requester can use this menu.", ephemeral=True)
            return

        pack_name = self.values[0]

        # Open packs one-by-one so we can display per-pack pages
        per_pack_pulls: list[list[dict]] = []
        try:
            for _ in range(self.amount):
                per_pack_pulls.append(open_pack_from_csv(pack_name, 1))  # one pack each page
        except Exception as e:
            # Disable UI on failure
            for child in self.view.children: child.disabled = True
            await interaction.response.edit_message(content=f"Failed to open pack: {e}", view=self.view)
            return

        # Persist all pulls (flatten)
        flattened = [c for pack in per_pack_pulls for c in pack]
        db_add_cards(self.requester.id, flattened, pack_name)

        # Switch the original message to the paginator
        paginator = PackResultsPaginator(self.requester, pack_name, per_pack_pulls, timeout=120)
        # Disable the select (we’re done choosing)
        for child in self.view.children: child.disabled = True
        await interaction.response.edit_message(content=None, embed=paginator._embed_for_index(), view=paginator)

class PacksSelectView(View):
    def __init__(self, requester: discord.User, amount: int, timeout: float = 90):
        super().__init__(timeout=timeout)
        self.add_item(PacksSelect(requester, amount))

    async def on_timeout(self):
        for child in self.children: child.disabled = True

@tree.command(name="open_select", description="Open trading card packs via dropdown", guild=GUILD)
@app_commands.describe(amount="How many packs (1-10)")
async def open_select_cmd(interaction: discord.Interaction, amount: app_commands.Range[int, 1, 10] = 1):
    if not PACKS_INDEX:
        await interaction.response.send_message("No packs found. Load CSVs into packs_csv/ then /reload_data.", ephemeral=True)
        return
    view = PacksSelectView(requester=interaction.user, amount=amount, timeout=90)
    await interaction.response.send_message("Pick a pack from the dropdown:", view=view, ephemeral=True)

# --- slash command wrapper ---
@tree.command(name="pack", description="Open trading card packs via dropdown", guild=GUILD)
@app_commands.describe(amount="How many packs (1-10)")
async def pack_cmd(
    interaction: discord.Interaction,
    amount: app_commands.Range[int, 1, 10] = 1
):
    if not PACKS_INDEX:
        await interaction.response.send_message(
            "No packs found. Did you load CSVs into packs_csv/ and /reload_data?",
            ephemeral=True
        )
        return

    view = PacksSelectView(requester=interaction.user, amount=amount, timeout=90)
    await interaction.response.send_message(
        content="Pick a pack from the dropdown:",
        view=view,
        ephemeral=True
    )

# ----- Collection (paginated) -----
@tree.command(name="collection", description="Show your (or someone else's) collection", guild=GUILD)
@app_commands.describe(user="User to view (optional)")
async def collection_cmd(interaction: discord.Interaction, user: discord.User = None):
    target = user or interaction.user
    rows = db_get_collection(target.id)  # (name, qty, rarity, set, code, id)
    if not rows:
        await interaction.response.send_message(f"{target.mention} has no cards yet.", ephemeral=True)
        return
    lines = build_collection_lines(rows)
    view = CollectionPaginatorView(target, lines, per_page=30, timeout=120)
    await interaction.response.send_message(embed=view._make_embed(), view=view, ephemeral=True)

# ----- Export in your exact schema order -----
@tree.command(
    name="export_collection",
    description="Export collection as CSV (site import format)",
    guild=GUILD
)
@app_commands.describe(user="User to export (optional)")
async def export_collection_cmd(interaction: discord.Interaction, user: discord.User = None):
    target = user or interaction.user
    rows = db_get_collection(target.id)
    if not rows:
        await interaction.response.send_message(f"{target.mention} has no cards.", ephemeral=True)
        return

    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    # Updated header for your site
    writer.writerow([
        "cardname",
        "cardq",
        "cardrarity",
        "card_edition",
        "cardset",
        "cardcode",
        "cardid",
        "print_id"
    ])
    for (name, qty, rarity, cset, code, cid) in rows:
        writer.writerow([
            name,
            qty,
            rarity,
            "1st Edition",  # hardcoded
            cset,
            code,
            cid,
            ""               # blank print_id
        ])

    buf.seek(0)
    file = discord.File(
        fp=io.BytesIO(buf.getvalue().encode("utf-8")),
        filename=f"{target.id}_collection.csv"
    )
    await interaction.response.send_message(
        content=f"Export for {target.mention}",
        file=file,
        ephemeral=True
    )

# ----- Admin: clear a user's collection -----
@tree.command(name="clear_collection", description="(Admin) Clear a user's collection", guild=GUILD)
@app_commands.default_permissions(administrator=True)
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(user="User to clear", confirm="Type YES to confirm")
async def clear_collection_cmd(interaction: discord.Interaction, user: discord.User, confirm: str):
    if confirm != "YES":
        await interaction.response.send_message(
            "Type **YES** in the `confirm` field to proceed.", ephemeral=True
        )
        return
    affected = db_clear_collection(user.id)
    await interaction.response.send_message(
        f"✅ Cleared **{affected}** rows from {user.mention}'s collection.", ephemeral=True
    )

@clear_collection_cmd.error
async def clear_collection_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("Admin only.", ephemeral=True)
    else:
        await interaction.response.send_message(f"Error: {error}", ephemeral=True)

# ----- Admin: reload datasets -----
@tree.command(name="reload_data", description="Reload CSV packs from packs_csv/", guild=GUILD)
@app_commands.default_permissions(administrator=True)
@app_commands.checks.has_permissions(administrator=True)
async def reload_data_cmd(interaction: discord.Interaction):
    try:
        load_packs_from_csv(PACKS_CSV_DIR)
        await interaction.response.send_message("CSV packs reloaded.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Reload failed: {e}", ephemeral=True)

@reload_data_cmd.error
async def reload_data_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("Admin only.", ephemeral=True)
    else:
        await interaction.response.send_message(f"Error: {error}", ephemeral=True)

# ---- Admin add/remove commands ----
@tree.command(name="admin_add_card", description="(Admin) Add a card row to a user's collection (rarity auto-resolved from pack)", guild=GUILD)
@app_commands.default_permissions(administrator=True)
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(
    user="User to modify",
    card_set="Set/pack name (autocomplete)",
    card_name="Card name (autocomplete)",
    qty="Quantity to add (default 1)",
    card_code="Card code (optional, narrows match if duplicates exist)",
    card_id="Card id (optional, narrows match if duplicates exist)"
)
@app_commands.autocomplete(card_set=ac_card_set, card_name=ac_card_name)
async def admin_add_card_cmd(
    interaction: discord.Interaction,
    user: discord.User,
    card_set: str,
    card_name: str,
    qty: app_commands.Range[int, 1, 999] = 1,
    card_code: str = "",
    card_id: str = ""
):
    try:
        item = resolve_card_from_pack(card_set, card_name, card_code, card_id)
    except ResolveError as e:
        await interaction.response.send_message(f"❌ {e}", ephemeral=True)
        return

    rarity = item.get("rarity","")
    new_total = db_admin_add_card(
        user.id,
        name=card_name, rarity=rarity, card_set=card_set,
        card_code=item.get("card_code",""), card_id=item.get("card_id",""),
        qty=qty
    )

    # Send ephemeral confirmation to admin
    await interaction.response.send_message(
        f"✅ Added **x{qty}** of **{card_name}** *(rarity: {rarity}, set: {card_set})* "
        f"to {user.mention}. New total for that row: **{new_total}**.",
        ephemeral=True
    )

    # Send public log message
    await interaction.channel.send(
        f"📦 **{interaction.user.display_name}** added x{qty} **{card_name}** "
        f"to **{user.display_name}**'s collection."
    )

@tree.command(name="admin_remove_card", description="(Admin) Remove a card row (rarity auto-resolved from pack)", guild=GUILD)
@app_commands.default_permissions(administrator=True)
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(
    user="User to modify",
    card_set="Set/pack name (autocomplete)",
    card_name="Card name (autocomplete)",
    qty="Quantity to remove (default 1)",
    card_code="Card code (optional; narrows match if duplicates exist)",
    card_id="Card id (optional; narrows match if duplicates exist)"
)
@app_commands.autocomplete(card_set=ac_card_set, card_name=ac_card_name)
async def admin_remove_card_cmd(
    interaction: discord.Interaction,
    user: discord.User,
    card_set: str,
    card_name: str,
    qty: app_commands.Range[int, 1, 999] = 1,
    card_code: str = "",
    card_id: str = ""
):
    try:
        item = resolve_card_from_pack(card_set, card_name, card_code, card_id)
    except ResolveError as e:
        await interaction.response.send_message(f"❌ {e}", ephemeral=True)
        return

    rarity = item.get("rarity","")
    removed, remaining = db_admin_remove_card(
        user.id,
        name=card_name, rarity=rarity, card_set=card_set,
        card_code=item.get("card_code",""), card_id=item.get("card_id",""),
        qty=qty
    )
    if removed == 0:
        await interaction.response.send_message(
            "ℹ️ No matching row for that exact (name, rarity-from-pack, set, code, id).",
            ephemeral=True
        )
        return

    # Ephemeral admin confirmation
    if remaining > 0:
        await interaction.response.send_message(
            f"✅ Removed **x{removed}** of **{card_name}** *(rarity: {rarity}, set: {card_set})* "
            f"from {user.mention}. Remaining: **{remaining}**.",
            ephemeral=True
        )
    else:
        await interaction.response.send_message(
            f"✅ Removed **x{removed}**; that row is now gone from {user.mention}'s collection.",
            ephemeral=True
        )

    # Public log message
    await interaction.channel.send(
        f"🗑 **{interaction.user.display_name}** removed x{removed} **{card_name}** "
        f"from **{user.display_name}**'s collection."
    )

# =========================
# Startup & sync
# =========================
did_initial_sync = False

@bot.event
async def on_ready():
    global did_initial_sync
    if did_initial_sync:
        return
    did_initial_sync = True

    db_init()
    load_packs_from_csv(PACKS_CSV_DIR)

    print(f"Logged in as {bot.user} (ID: {bot.user.id})")
    print(f"GUILD_ID={GUILD_ID} DEV_FORCE_CLEAN={DEV_FORCE_CLEAN}")

    try:
        # Optional dev-only cleanup of global commands
        if DEV_FORCE_CLEAN:
            try:
                print("Clearing GLOBAL app commands…")
                tree.clear_commands(guild=None)
                await tree.sync(guild=None)
                print("Global commands cleared.")
            except Exception as e:
                print("Global clear failed:", e)

        if GUILD:
            await tree.sync(guild=GUILD)
            print(f"Slash commands synced to guild {GUILD_ID}")
        else:
            await tree.sync()
            print("Slash commands globally synced (may take time).")
    except Exception as e:
        print("Command sync failed:", e)

if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("DISCORD_TOKEN missing in .env")
    bot.run(TOKEN)
