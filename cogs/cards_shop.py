# cogs/cards_shop.py (replace the command bodies + autocomplete hooks)
import os, discord
from typing import List, Optional
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
from core.constants import BUY_PRICES, SELL_PRICES
from core.views import (
    ConfirmBuyCardView,
    ConfirmSellCardView,
    ConfirmP2PInitiatorView,
)
from core.db import db_collection_list_owned_prints

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

def _suggest_prints_with_set(state, query: str, limit: int = 25):
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

def _suggest_owned_prints(state, user_id: int, query: str, limit: int = 25) -> List[app_commands.Choice[str]]:
    ensure_shop_index(state)
    tokens = [t for t in (query or "").strip().lower().split() if t]
    owned = db_collection_list_owned_prints(state, user_id, name_filter=None, limit=limit*2)
    choices: List[app_commands.Choice[str]] = []
    for row in owned:
        hay = " ".join([row["name"] or "", row["set"] or "", row["rarity"] or "", (row["code"] or ""), (row["id"] or "")]).lower()
        if tokens and not all(tok in hay for tok in tokens):
            continue
        k = register_print_if_missing(state, {
            "cardname": row["name"],
            "cardrarity": row["rarity"],
            "cardset": row["set"],
            "cardcode": row["code"],
            "cardid": row["id"],
        })
        card = find_card_by_print_key(state, k)
        if card:
            choices.append(app_commands.Choice(name=card_label(card), value=k))
            if len(choices) >= limit: break
    return choices

class CardsShop(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: AppState = bot.state

    # ---------- BUY ----------
    async def ac_buy(self, interaction: discord.Interaction, current: str):
        # If a counterparty is specified, suggest prints THEY own; otherwise, from shop index
        try:
            cp = getattr(interaction.namespace, "counterparty", None)
        except AttributeError:
            cp = None
        if isinstance(cp, discord.Member):
            return _suggest_owned_prints(self.state, cp.id, current)
        return _suggest_prints_with_set(self.state, current)

    @app_commands.command(name="buy", description="Buy a printing from the shop, or from another user if specified")
    @app_commands.guilds(GUILD)
    @app_commands.describe(
        card="Choose the exact printing",
        amount="How many copies (max 3)",
        counterparty="(Optional) buy from this user instead of the shop",
        price="(Required if counterparty set) total mambucks you will pay"
    )
    @app_commands.autocomplete(card=ac_buy)
    async def buy(
        self,
        interaction: discord.Interaction,
        card: str,
        amount: app_commands.Range[int, 1, 3] = 1,
        counterparty: Optional[discord.Member] = None,
        price: Optional[app_commands.Range[int, 1, 1_000_000]] = None,
    ):
        c = find_card_by_print_key(self.state, card)
        if not c:
            return await interaction.response.send_message("Card not found.", ephemeral=True)

        set_present = (c.get("set") or c.get("cardset") or "").strip()
        if not set_present:
            return await interaction.response.send_message("This printing is missing a set and can’t be traded.", ephemeral=True)

        if counterparty is None:
            # SHOP path (unchanged)
            rarity = get_card_rarity(c)
            price_each = BUY_PRICES.get(rarity)
            if rarity == "starlight" or price_each is None:
                return await interaction.response.send_message("❌ This printing cannot be bought from the shop.", ephemeral=True)
            total = price_each * amount
            view = ConfirmBuyCardView(self.state, requester=interaction.user, print_key=card, amount=amount, total_cost=total)
            return await interaction.response.send_message(
                f"Are you sure you want to buy **{amount}× {card_label(c)}** for **{total}** mambucks?",
                view=view, ephemeral=True
            )

        # P2P path
        if price is None:
            return await interaction.response.send_message("Please provide a **price** (mambucks) when buying from a user.", ephemeral=True)

        if counterparty.id == interaction.user.id:
            return await interaction.response.send_message("You can’t create a P2P offer with yourself.", ephemeral=True)

        view = ConfirmP2PInitiatorView(
            self.state,
            requester=interaction.user,
            counterparty=counterparty,
            mode="buy",
            print_key=card,
            copies=amount,
            price_mb=int(price),
        )
        return await interaction.response.send_message(
            f"Are you sure you want to **buy** {amount}× {card_label(c)} from {counterparty.mention} for **{int(price)}** mambucks?",
            view=view, ephemeral=True
        )

    # ---------- SELL ----------
    async def ac_sell(self, interaction: discord.Interaction, current: str):
        # Always suggest prints the CALLER owns (they're the seller)
        return _suggest_owned_prints(self.state, interaction.user.id, current)

    @app_commands.command(name="sell", description="Sell a printing to the shop, or to another user if specified")
    @app_commands.guilds(GUILD)
    @app_commands.describe(
        card="Choose the exact printing you own",
        amount="How many copies (max 10)",
        counterparty="(Optional) sell to this user instead of the shop",
        price="(Required if counterparty set) total mambucks you want to receive"
    )
    @app_commands.autocomplete(card=ac_sell)
    async def sell(
        self,
        interaction: discord.Interaction,
        card: str,
        amount: app_commands.Range[int, 1, 10] = 1,
        counterparty: Optional[discord.Member] = None,
        price: Optional[app_commands.Range[int, 1, 1_000_000]] = None,
    ):
        c = find_card_by_print_key(self.state, card)
        if not c:
            return await interaction.response.send_message("Card not found.", ephemeral=True)

        set_present = (c.get("set") or c.get("cardset") or "").strip()
        if not set_present:
            return await interaction.response.send_message("This printing is missing a set and can’t be traded.", ephemeral=True)

        if counterparty is None:
            # SHOP path (unchanged from your functional version)
            rarity = get_card_rarity(c)
            price_each = SELL_PRICES.get(rarity)
            if rarity == "starlight" or price_each is None:
                return await interaction.response.send_message("❌ This printing cannot be sold to the shop.", ephemeral=True)
            total = price_each * amount
            view = ConfirmSellCardView(self.state, requester=interaction.user, print_key=card, amount=amount, total_credit=total)
            return await interaction.response.send_message(
                f"Are you sure you want to sell **{amount}× {card_label(c)}** for **{total}** mambucks?",
                view=view, ephemeral=True
            )

        # P2P path
        if price is None:
            return await interaction.response.send_message("Please provide a **price** (mambucks) when selling to a user.", ephemeral=True)

        if counterparty.id == interaction.user.id:
            return await interaction.response.send_message("You can’t create a P2P offer with yourself.", ephemeral=True)

        view = ConfirmP2PInitiatorView(
            self.state,
            requester=interaction.user,
            counterparty=counterparty,
            mode="sell",
            print_key=card,
            copies=amount,
            price_mb=int(price),
        )
        return await interaction.response.send_message(
            f"Are you sure you want to **sell** {amount}× {card_label(c)} to {counterparty.mention} for **{int(price)}** mambucks?",
            view=view, ephemeral=True
        )

async def setup(bot: commands.Bot):
    await bot.add_cog(CardsShop(bot))
