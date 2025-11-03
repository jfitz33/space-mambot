# cogs/collection.py
import io, csv, os, discord
from collections import defaultdict
from typing import Dict, Iterable, List, Tuple, Any
from discord.ext import commands
from discord import app_commands

from core.db import db_get_collection

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

# ---------- Rarity (trimmed + Starlight) ----------
RARITY_ORDER = ["COMMON", "RARE", "SUPER RARE", "ULTRA RARE", "SECRET RARE", "STARLIGHT RARE"]
RARITY_ALIASES = {
    "C": "COMMON", "COMMON": "COMMON",
    "R": "RARE", "RARE": "RARE",
    "SR": "SUPER RARE", "SUPER": "SUPER RARE", "SUPER RARE": "SUPER RARE",
    "UR": "ULTRA RARE", "ULTRA": "ULTRA RARE", "ULTRA RARE": "ULTRA RARE",
    "SCR": "SECRET RARE", "SEC": "SECRET RARE", "SECRET": "SECRET RARE", "SECRET RARE": "SECRET RARE",
    "SLR": "STARLIGHT RARE", "STARLIGHT": "STARLIGHT RARE", "STARLIGHT RARE": "STARLIGHT RARE",
}
def normalize_rarity(raw: str) -> str:
    key = (raw or "").strip().upper()
    key = key.replace("SUPERRARE","SUPER RARE").replace("ULTRARARE","ULTRA RARE").replace("SECRETRARE","SECRET RARE")
    return RARITY_ALIASES.get(key, "SECRET RARE")
def rarity_bucket_index(raw: str) -> int:
    return RARITY_ORDER.index(normalize_rarity(raw))

# canonical rarity -> key used in bot.state.rarity_emoji_ids
RARITY_TO_IDKEY = {
    "COMMON": "common",
    "RARE": "rare",
    "SUPER RARE": "super",
    "ULTRA RARE": "ultra",
    "SECRET RARE": "secret",
    "STARLIGHT RARE": "secret",  # reuse secret badge per your decision
}
async def build_badge_tokens_from_state(bot: commands.Bot, state: Any) -> Dict[str, str]:
    """
    Uses IDs stored by ensure_rarity_emojis in bot.state.rarity_emoji_ids and
    returns {'COMMON': '<:rar_common:123...>', ...}. Falls back to short text if unresolved.
    """
    fallback = {"COMMON":"C","RARE":"R","SUPER RARE":"SR","ULTRA RARE":"UR","SECRET RARE":"SCR","STARLIGHT RARE":"SCR"}
    idmap = getattr(state, "rarity_emoji_ids", None)
    if not isinstance(idmap, dict) or not idmap:
        return fallback

    async def tok(id_val) -> str:
        if not id_val:
            return ""
        try:
            eid = int(id_val)
        except Exception:
            return ""
        e = bot.get_emoji(eid)
        if not e:
            try:
                e = await bot.fetch_emoji(eid)
            except Exception:
                return ""
        return str(e) if e else ""

    tokens: Dict[str, str] = {}
    for bucket, key in RARITY_TO_IDKEY.items():
        tokens[bucket] = await tok(idmap.get(key)) or fallback[bucket]
    return tokens

# ---------- Optional: pretty headers via your pack/starter indexes ----------
def resolve_set_header(state: Any, cset: str) -> str:
    label = str(cset or "Unknown")
    for attr in ("packs_index", "starters_index"):
        mapping = getattr(state, attr, None)
        if isinstance(mapping, dict):
            meta = mapping.get(label)
            if isinstance(meta, dict):
                name = meta.get("name")
                if isinstance(name, str) and name.strip():
                    return name.strip()
    return label

# ---------- Build lines and embed descriptions ----------
def group_and_format_rows(
    rows: Iterable[tuple],
    state: Any,
    badge_tokens: Dict[str, str],
) -> List[Tuple[str, List[str]]]:
    """
    rows: (name, qty, rarity, cset, code, cid)
    -> [(header, ["<badge> <qty>x - <card_name>", ...]), ...]
    """
    groups: Dict[str, List[Tuple[str, int, str]]] = defaultdict(list)
    for (name, qty, rarity, cset, _code, _cid) in rows:
        header = resolve_set_header(state, cset)
        groups[header].append((str(name or "").strip(), int(qty or 0), str(rarity or "").strip()))

    sections: List[Tuple[str, List[str]]] = []
    for header in sorted(groups.keys(), key=lambda s: s.lower()):
        cards = groups[header]
        cards.sort(key=lambda t: (rarity_bucket_index(t[2]), t[0].lower()))
        lines: List[str] = []
        for name, qty, rarity in cards:
            if qty <= 0 or not name:
                continue
            bucket = normalize_rarity(rarity)
            badge  = badge_tokens.get(bucket, "")
            lines.append(f"{badge} {qty}x - {name}".strip())
        if lines:
            sections.append((header, lines))
    return sections

def sections_to_embed_descriptions(sections: List[Tuple[str, List[str]]], per_embed_limit: int = 4096) -> List[str]:
    """
    Turns sections into <=4096-char description blocks, never splitting a row.
    Continuations auto-label the header with (cont.).
    """
    blocks: List[str] = []
    cur_lines: List[str] = []
    cur_len = 0

    def flush():
        nonlocal cur_lines, cur_len
        if cur_lines:
            blocks.append("\n".join(cur_lines).rstrip())
            cur_lines, cur_len = [], 0

    for header, lines in sections:
        i = 0
        printed_once = False
        while i < len(lines):
            hdr = f"**{header}**" if not printed_once else f"**{header} (cont.)**"
            if not cur_lines:
                cur_lines.append(hdr)
                cur_len += len(hdr) + 1
                printed_once = True

            line = lines[i]
            need = len(line) + 1
            if cur_len + need > per_embed_limit:
                flush()
                continue
            cur_lines.append(line)
            cur_len += need
            i += 1
        flush()
    return [b for b in blocks if b.strip()]

# ---------- Cog ----------
class Collection(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="collection",
        description="DMs you a grouped list of a user's collection by pack/starter."
    )
    @app_commands.guilds(GUILD)
    @app_commands.describe(user="User to view (optional)")
    async def collection(self, interaction: discord.Interaction, user: discord.User = None):
        await interaction.response.defer(ephemeral=True)

        target = user or interaction.user

        # 1) Load rows
        try:
            rows = db_get_collection(self.bot.state, target.id)
        except Exception as e:
            await interaction.edit_original_response(content=f"Couldn't load collection: `{e}`")
            return

        if not rows:
            await interaction.edit_original_response(content=f"{target.mention} has no cards.")
            return

        try:
            # 2) Build rarity badge tokens from IDs cached by ensure_rarity_emojis(...)
            badges = await build_badge_tokens_from_state(self.bot, self.bot.state)

            # 3) Group, sort, and format rows -> sections
            sections = group_and_format_rows(rows, self.bot.state, badges)
            if not sections:
                await interaction.edit_original_response(content=f"{target.mention} has no cards.")
                return

            # 4) Turn sections into 3,900-char embed descriptions (no row splits)
            descs = sections_to_embed_descriptions(sections, per_embed_limit=3900)

            # 5) DM embeds (one embed per message); enforce 6,000-char total/embed safety
            dm_recipient = interaction.user  # change to `target` if you want to DM the owner instead
            dm = await dm_recipient.create_dm()

            title_text = f"Collection for {getattr(target, 'display_name', target.name)}"
            first_sent = False

            for desc in descs:
                # Build the embed (title only on the first one)
                embed = discord.Embed(description=desc)
                if not first_sent:
                    embed.title = title_text
                    first_sent = True

                # Safety: if somehow over 6000 including title, split further on line breaks
                total_len = len(embed.title or "") + len(embed.description or "")
                if total_len <= 6000:
                    await dm.send(embed=embed)
                else:
                    lines = (embed.description or "").splitlines()
                    head = embed.title or ""
                    buf, blen = [], len(head)

                    def make_embed(title_text: str, body_lines: list[str]) -> discord.Embed:
                        return discord.Embed(
                            title=title_text if title_text else discord.Embed.Empty,
                            description="\n".join(body_lines)
                        )

                    for line in lines:
                        if blen + len(line) + 1 > 3900:
                            await dm.send(embed=make_embed(head, buf))
                            head = ""  # only the first chunk carries the title
                            buf, blen = [line], len(line) + 1
                        else:
                            buf.append(line)
                            blen += len(line) + 1

                    if buf:
                        await dm.send(embed=make_embed(head, buf))

            # 6) Replace the “thinking” placeholder with the final status
            await interaction.edit_original_response(
                content=f"Sent you a DM with {target.mention}'s collection. 📬"
            )

        except discord.Forbidden:
            await interaction.edit_original_response(
                content="I couldn't DM you (your DMs might be closed). Please enable DMs from server members and try again."
            )
        except Exception as e:
            await interaction.edit_original_response(
                content=f"Something went wrong sending the DM: `{e}`"
            )

    @app_commands.command(name="export_collection", description="Export collection CSV for site import")
    @app_commands.guilds(GUILD)
    async def export_collection(self, interaction: discord.Interaction):
        target = interaction.user
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
