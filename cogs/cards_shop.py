# cogs/cards_shop.py
import os, discord, textwrap
from typing import List, Dict
from discord.ext import commands
from discord import app_commands

from core.state import AppState
from core.cards_shop import (
    ensure_shop_index,
    find_card_by_print_key,
    card_label,
    get_card_rarity,
    register_print_if_missing,
)
from core.constants import (
    CRAFT_COST_BY_RARITY, SHARD_YIELD_BY_RARITY, set_id_for_pack, 
    RARITY_ORDER, RARITY_ALIASES, FRAGMENTABLE_RARITIES
)
from core.views import (
    ConfirmBuyCardView,
    ConfirmSellCardView,
)
from core.currency import SHARD_SET_NAMES
from core.db import (
    db_collection_list_owned_prints, db_collection_list_for_bulk_fragment, 
    db_shards_add, db_collection_remove_exact_print
)

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

# -------- existing suggestors (reused exactly as-is) --------

def suggest_prints_with_set(state, query: str, limit: int = 25):
    from core.cards_shop import ensure_shop_index, card_label, _sig_for_resolution
    ensure_shop_index(state)
    q_tokens = [t for t in (query or "").lower().split() if t]

    # dedupe by signature (name, rarity, code, id), prefer entries with set
    best_by_sig = {}
    for k, card in state._shop_print_by_key.items():
        name = (card.get("name") or card.get("cardname") or "").strip()
        rarity = (card.get("rarity") or card.get("cardrarity") or "").strip()
        set_ = (card.get("set") or card.get("cardset") or "").strip()
        code = (card.get("code") or card.get("cardcode") or "").strip()
        cid  = (card.get("id") or card.get("cardid") or "").strip()
        hay = f"{name} {set_} {rarity} {code} {cid}".lower()
        if q_tokens and not all(t in hay for t in q_tokens):
            continue
        sig = _sig_for_resolution(name, rarity, code, cid)

        # scoring: prefer has_set, then has_code, then has_id
        score = (1 if set_ else 0, 1 if code else 0, 1 if cid else 0)
        prev = best_by_sig.get(sig)
        if prev is None or score > prev[0]:
            best_by_sig[sig] = (score, k, card)

    # emit choices (set-aware only)
    out = []
    for _, k, card in best_by_sig.values():
        set_present = (card.get("set") or card.get("cardset") or "").strip()
        if not set_present:
            continue
        out.append(app_commands.Choice(name=card_label(card), value=k))
        if len(out) >= limit:
            break
    return out

def _normalize_tokens(q: str) -> List[str]:
    q = (q or "").lower()
    # simple normalize: keep alnum & spaces
    out = []
    cur = []
    for ch in q:
        if ch.isalnum():
            cur.append(ch)
        else:
            if cur:
                out.append("".join(cur)); cur = []
    if cur:
        out.append("".join(cur))
    return [t for t in out if t]

def suggest_owned_prints_relaxed(state, user_id: int, query: str, limit: int = 25) -> List[app_commands.Choice[str]]:
    ensure_shop_index(state)
    tokens = _normalize_tokens(query)
    # pull more rows than we’ll show, to improve chances
    rows = db_collection_list_owned_prints(state, user_id, name_filter=None, limit=1000)

    choices: List[app_commands.Choice[str]] = []
    seen_keys = set()

    for row in rows:
        name = (row.get("name") or "").strip()
        rty  = (row.get("rarity") or "").strip()
        set_ = (row.get("set") or "").strip()  # may be empty in older rows
        code = (row.get("code") or "").strip()
        cid  = (row.get("id") or "").strip()
        qty  = int(row.get("qty") or 0)

        hay = f"{name} {set_} {rty} {code} {cid}".lower()
        if tokens and not all(t in hay for t in tokens):
            continue

        # Build/lookup a proper print key for this owned row
        print_key = register_print_if_missing(state, {
            "cardname":  name,
            "cardrarity": rty,
            "cardset":    set_ or None,
            "cardcode":   code or None,
            "cardid":     cid or None,
        })
        if not print_key or print_key in seen_keys:
            continue
        seen_keys.add(print_key)

        card = find_card_by_print_key(state, print_key)
        if not card:
            continue

        label = card_label(card)
        if qty > 0:
            label = f"{label} ×{qty}"

        choices.append(app_commands.Choice(name=label, value=print_key))
        if len(choices) >= limit:
            break

    return choices

def norm_rarity(s: str) -> str:
    r = (s or "").strip().lower()
    return RARITY_ALIASES.get(r, r)

def shorten(s: str, n: int = 80) -> str:
    return s if len(s) <= n else s[:n-1] + "…"

async def ac_pack_names(interaction: discord.Interaction, current: str):
    state: AppState = interaction.client.state
    q = (current or "").lower()
    names = sorted((state.packs_index or {}).keys())
    out = []
    for n in names:
        if q and q not in n.lower():
            continue
        out.append(app_commands.Choice(name=shorten(n, 100), value=n[:100]))
        if len(out) >= 25:
            break
    return out

async def ac_fragmentable_rarity(interaction: discord.Interaction, current: str):
    q = (current or "").lower()
    out = []
    for r in FRAGMENTABLE_RARITIES:  # starlight excluded here
        label = r.title()
        alias_blob = " ".join(k for k, v in RARITY_ALIASES.items() if v == r)
        if q and (q not in r and q not in label.lower() and q not in alias_blob.lower()):
            continue
        out.append(app_commands.Choice(name=label, value=r))
    return out

class BulkFragmentConfirmView(discord.ui.View):
    def __init__(self, state: AppState, user: discord.Member, plan_rows: List[Dict], pack_name: str, rarity: str, keep: int, total_yield: int, *, timeout: float = 120):
        super().__init__(timeout=timeout)
        self.state = state
        self.user = user
        self.plan_rows = plan_rows
        self.pack_name = pack_name
        self.rarity = rarity
        self.keep = int(keep)
        self.total_yield = int(total_yield)
        self._locked = False

    def shard_label(self) -> str:
        sid = set_id_for_pack(self.pack_name) or 1
        return SHARD_SET_NAMES.get(sid, f"Shards (Set {sid})")

    async def finalize(self, interaction: discord.Interaction, content: str | None = None):
        self.stop()
        for c in self.children:
            c.disabled = True
        try:
            await interaction.response.edit_message(content=content, view=None)
        except discord.InteractionResponded:
            await interaction.message.edit(content=content, view=None)

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.user.id:
            return await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)
        if self._locked:
            try: await interaction.response.defer_update()
            except: pass
            return
        self._locked = True

        sid = set_id_for_pack(self.pack_name) or 1
        yield_per = int(SHARD_YIELD_BY_RARITY.get(self.rarity, 0))
        credited = 0

        try:
            for row in self.plan_rows:
                amt = int(row["to_frag"])
                if amt <= 0:
                    continue
                removed = db_collection_remove_exact_print(
                    self.state, self.user.id,
                    card_name=row["name"],
                    card_rarity=row["rarity"],
                    card_set=row["set"],
                    card_code=row["code"],
                    card_id=row["id"],
                    amount=amt
                )
                if removed > 0:
                    credited += removed * yield_per

            if credited > 0:
                db_shards_add(self.state, self.user.id, sid, credited)

            pretty = self.shard_label()
            await self.finalize(interaction, content=f"✅ Fragmented these cards into **{credited} {pretty}**.")
        except Exception:
            self._locked = False
            raise

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.user.id:
            return await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)
        await self.finalize(interaction, content="Cancelled.")

class CardsShop(commands.Cog):
    """
    Refactor: /craft (was /buy) and /shard (was /sell), shop-only.
    """
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: AppState = bot.state

    # ---------- CRAFT (refactor of BUY; shop-only) ----------
    async def ac_craft(self, interaction: discord.Interaction, current: str):
        # Suggest craftable prints from shop index (set-aware, as before)
        return suggest_prints_with_set(self.state, current)

    @app_commands.command(
        name="craft",
        description="Craft a specific printing using shards"
    )
    @app_commands.guilds(GUILD)
    @app_commands.describe(
        card="Choose the exact printing",
        amount="How many copies (max 3)",
    )
    @app_commands.autocomplete(card=ac_craft)
    async def craft(
        self,
        interaction: discord.Interaction,
        card: str,
        amount: app_commands.Range[int, 1, 3] = 1,
    ):
        c = find_card_by_print_key(self.state, card)
        if not c:
            return await interaction.response.send_message("Card not found.", ephemeral=True)

        set_present = (c.get("set") or c.get("cardset") or "").strip()
        if not set_present:
            return await interaction.response.send_message("This printing is missing a set and can’t be crafted.", ephemeral=True)

        rarity = get_card_rarity(c)
        price_each = CRAFT_COST_BY_RARITY.get(rarity)
        if rarity == "starlight" or price_each is None:
            return await interaction.response.send_message("❌ This printing cannot be crafted.", ephemeral=True)
        total = price_each * amount
        # Reuse your existing confirmation view (performs wallet debit + award)
        view = ConfirmBuyCardView(self.state, requester=interaction.user, print_key=card, amount=amount, total_cost=total)
        return await interaction.response.send_message(
            f"Are you sure you want to **craft** **{amount}× {card_label(c)}** for **{total}** Elemental Shards?",
            view=view,
            ephemeral=True
        )

    # ---------- SHARD (refactor of SELL; shop-only) ----------
    async def ac_shard(self, interaction: discord.Interaction, current: str):
        # Suggest prints the CALLER owns (they're sharding their own cards)
        return suggest_owned_prints_relaxed(self.state, interaction.user.id, current)

    @app_commands.command(
        name="fragment",
        description="Break down a specific printing to receive shards"
    )
    @app_commands.guilds(GUILD)
    @app_commands.describe(
        card="Choose the exact printing you own",
        amount="How many copies (max 10)",
    )
    @app_commands.autocomplete(card=ac_shard)
    async def fragment(
        self,
        interaction: discord.Interaction,
        card: str,
        amount: app_commands.Range[int, 1, 100] = 1,
    ):
        c = find_card_by_print_key(self.state, card)
        if not c:
            return await interaction.response.send_message("Card not found.", ephemeral=True)

        set_present = (c.get("set") or c.get("cardset") or "").strip()
        if not set_present:
            return await interaction.response.send_message("This printing is missing a set and can’t be fragmented.", ephemeral=True)

        rarity = get_card_rarity(c)
        price_each = SHARD_YIELD_BY_RARITY.get(rarity)
        if rarity == "starlight" or price_each is None:
            return await interaction.response.send_message("❌ This printing cannot be crafted.", ephemeral=True)
        total = price_each * amount
        # Reuse your existing confirmation view (performs removal + credit)
        view = ConfirmSellCardView(self.state, requester=interaction.user, print_key=card, amount=amount, total_credit=total)
        return await interaction.response.send_message(
            f"Are you sure you want to **fragment** **{amount}× {card_label(c)}** into **{total}** Elemental Shards?",
            view=view,
            ephemeral=True
        )
    
    @app_commands.command(name="fragment_bulk", description="Fragment many cards at once by pack + rarity, keeping a minimum number of each.")
    @app_commands.guilds(GUILD)
    @app_commands.describe(
        pack_name="Set/pack to filter",
        rarity="Card rarity to fragment",
        keep="Keep at least this many copies per card (default 3)"
    )
    @app_commands.autocomplete(pack_name=ac_pack_names, rarity=ac_fragmentable_rarity)
    async def fragment_bulk(
        self,
        interaction: discord.Interaction,
        pack_name: str,
        rarity: str,
        keep: app_commands.Range[int, 0, 99] = 3,
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        if pack_name not in (self.state.packs_index or {}):
            return await interaction.followup.send("❌ Unknown pack/set.", ephemeral=True)

        r = norm_rarity(rarity)
        if r not in FRAGMENTABLE_RARITIES:
            return await interaction.followup.send("❌ That rarity can’t be fragmented.", ephemeral=True)

        # DB helper gathers rows (exact prints) with qty > keep
        rows = db_collection_list_for_bulk_fragment(self.state, interaction.user.id, pack_name, r, int(keep))
        if not rows:
            return await interaction.followup.send("Nothing to fragment with those filters (or all at/under the keep amount).", ephemeral=True)

        # Compute total yield
        yield_per = int(SHARD_YIELD_BY_RARITY.get(r, 0))
        total_yield = sum(int(x["to_frag"]) * yield_per for x in rows)
        sid = set_id_for_pack(pack_name) or 1
        pretty = SHARD_SET_NAMES.get(sid, f"Shards (Set {sid})")

        # Build preview
        lines = []
        for x in rows:
            keep = min(int(x["qty"]), int(keep))
            lines.append(f"• x{x['to_frag']} {shorten(x['name'], 64)} (keep {keep})")

        preview = "\n".join(lines)
        if len(preview) > 1800:
            preview = preview[:1800] + "\n…"

        view = BulkFragmentConfirmView(self.state, interaction.user, rows, pack_name, r, keep, total_yield)
        content_lines = [
            "Are you sure you want to fragment the following cards?",
            "",
            preview,
            "",
            f"This will yield **{total_yield} {pretty}**."
        ]
        content = "\n".join(content_lines)

        await interaction.followup.send(
            content=content,
            ephemeral=True,
            view=view
        )

async def setup(bot: commands.Bot):
    await bot.add_cog(CardsShop(bot))
