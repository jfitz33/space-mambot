# cogs/cards_shop.py
import os, discord
from typing import List
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
from core.constants import CRAFT_COST_BY_RARITY, SHARD_YIELD_BY_RARITY
from core.views import (
    ConfirmBuyCardView,
    ConfirmSellCardView,
)
from core.db import db_collection_list_owned_prints

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

async def setup(bot: commands.Bot):
    await bot.add_cog(CardsShop(bot))
