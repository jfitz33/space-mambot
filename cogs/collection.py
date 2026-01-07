# cogs/collection.py
import io, csv, os, discord
from collections import defaultdict
from typing import Dict, Iterable, List, Tuple, Any
from discord.ext import commands
from discord import app_commands

from core.db import db_binder_list, db_get_collection, _normalize_card_identity
from core.constants import CURRENT_ACTIVE_SET, PACKS_BY_SET, set_id_for_pack
from core.constants import PACKS_BY_SET, set_id_for_pack

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

STARTER_DECK_CHOICE_VALUE = 0


def _build_set_choices(max_set_id: int) -> List[app_commands.Choice[int]]:
    choices: List[app_commands.Choice[int]] = [
        app_commands.Choice(name="Starter decks", value=STARTER_DECK_CHOICE_VALUE)
    ]

    for set_id in sorted(PACKS_BY_SET):
        if set_id > max_set_id:
            continue
        choices.append(app_commands.Choice(name=f"Set {set_id}", value=set_id))

    return choices


SET_CHOICES: List[app_commands.Choice[int]] = _build_set_choices(CURRENT_ACTIVE_SET)

def set_id_for_source(state: Any, set_name: str) -> int | None:
    """Return the set ID for a pack/tin name."""

    sid = set_id_for_pack(set_name)
    if sid is not None:
        return sid

    tins_index = getattr(state, "tins_index", None) or {}
    tin = tins_index.get(set_name)
    if isinstance(tin, dict):
        pack_set_ids = {set_id_for_pack(pack) for pack in tin.get("packs") or []}
        pack_set_ids.discard(None)
        if len(pack_set_ids) == 1:
            return pack_set_ids.pop()

    return None


def section_kind(state: Any, set_name: str) -> str:
    tins_index = getattr(state, "tins_index", None) or {}
    starters_index = getattr(state, "starters_index", None) or {}
    if set_name in starters_index:
        return "starter"
    if set_name in tins_index:
        return "tin"
    if set_id_for_pack(set_name) is not None:
        return "pack"
    return "other"

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
    groups: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"cards": []})
    for (name, qty, rarity, cset, _code, _cid) in rows:
        header = resolve_set_header(state, cset)
        group = groups[header]
        group.setdefault("set_id", set_id_for_source(state, cset))
        group.setdefault("kind", section_kind(state, cset))
        group["cards"].append((str(name or "").strip(), int(qty or 0), str(rarity or "").strip()))

    def sort_key(item: Tuple[str, Dict[str, Any]]):
        header, meta = item
        set_id = meta.get("set_id")
        kind = meta.get("kind")
        set_rank = -1 if kind == "starter" else set_id if set_id is not None else float("inf")
        kind_rank = (
            0 if kind == "starter"
            else 1 if kind == "pack"
            else 2 if kind == "tin"
            else 3
        )
        return (set_rank, kind_rank, header.lower())

    sections: List[Tuple[str, List[str]]] = []
    for header, meta in sorted(groups.items(), key=sort_key):
        cards = meta["cards"]
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
        description="View a list of all cards in your collection, filterable by set number"
    )
    @app_commands.guilds(GUILD)
    @app_commands.describe(
        set_number="Restrict results to a specific set (optional)",
        include_binder="Include cards stored in your binder (default: true)",
    )
    @app_commands.choices(set_number=SET_CHOICES)
    async def collection(
        self,
        interaction: discord.Interaction,
        set_number: app_commands.Choice[int] = None,
        include_binder: bool = True,
    ):
        await interaction.response.defer(ephemeral=True)

        target = interaction.user

        # 1) Load rows
        try:
            rows = db_get_collection(self.bot.state, target.id)
        except Exception as e:
            await interaction.edit_original_response(content=f"Couldn't load collection: `{e}`")
            return

        if not rows:
            await interaction.edit_original_response(content=f"{target.mention} has no cards.")
            return
        
        selected_set_id = set_number.value if set_number else None
        if selected_set_id == STARTER_DECK_CHOICE_VALUE:
            rows = [row for row in rows if section_kind(self.bot.state, row[3]) == "starter"]
            if not rows:
                await interaction.edit_original_response(
                    content=f"{target.mention} has no starter decks."
                )
                return
        elif selected_set_id is not None:
            rows = [row for row in rows if set_id_for_source(self.bot.state, row[3]) == selected_set_id]
            if not rows:
                await interaction.edit_original_response(
                    content=f"{target.mention} has no cards in Set {selected_set_id}."
                )
                return

        if not include_binder:
            binder_rows = db_binder_list(self.bot.state, target.id)
            binder_qty = defaultdict(int)
            for item in binder_rows:
                name, rarity, cset, code, cid = _normalize_card_identity(item)
                key = (name.lower(), rarity.lower(), cset.lower(), code, cid)
                binder_qty[key] += int(item.get("qty") or 0)

            filtered_rows = []
            for name, qty, rarity, cset, code, cid in rows:
                n_name, n_rarity, n_set, n_code, n_cid = _normalize_card_identity(
                    None, name=name, rarity=rarity, card_set=cset, card_code=code, card_id=cid
                )
                key = (n_name.lower(), n_rarity.lower(), n_set.lower(), n_code, n_cid)
                remaining = int(qty or 0) - binder_qty.get(key, 0)
                if remaining > 0:
                    filtered_rows.append((name, remaining, rarity, cset, code, cid))

            rows = filtered_rows
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
                content="Sent you a DM with your collection. 📬"
            )

        except discord.Forbidden:
            await interaction.edit_original_response(
                content="I couldn't DM you (your DMs might be closed). Please enable DMs from server members and try again."
            )
        except Exception as e:
            await interaction.edit_original_response(
                content=f"Something went wrong sending the DM: `{e}`"
            )

    @app_commands.command(name="export_collection", description="Export collection CSV for import into ygoprodeck")
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
