import os, discord
from typing import List
from discord.ext import commands
from discord import app_commands

from core.state import AppState
from core.cards_shop import ensure_shop_index, find_card_by_print_key, get_card_rarity, card_label, register_print_if_missing
from core.db import db_collection_list_owned_prints
from core.constants import BUY_PRICES, SELL_PRICES
from core.views import ConfirmBuyCardView, ConfirmSellCardView

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

def _suggest_prints(state, query: str, limit: int = 25) -> List[app_commands.Choice[str]]:
    ensure_shop_index(state)
    q = (query or "").strip().lower()
    out: List[app_commands.Choice[str]] = []
    for k, card in getattr(state, "_shop_print_by_key", {}).items():
        text = f"{card.get('name') or ''} {card.get('set') or card.get('cardset') or ''} {card.get('rarity') or card.get('cardrarity') or ''} {card.get('code') or card.get('cardcode') or ''}".lower()
        if not q or all(tok in text for tok in q.split()):
            out.append(app_commands.Choice(name=card_label(card), value=k))
            if len(out) >= limit:
                break
    return out

def _suggest_owned_prints(state, user_id: int, query: str, limit: int = 25) -> List[app_commands.Choice[str]]:
    """
    Autocomplete: show only exact printings the user owns, filtered by query tokens.
    """
    ensure_shop_index(state)
    tokens = [t for t in (query or "").strip().lower().split() if t]
    owned = db_collection_list_owned_prints(state, user_id, name_filter=None, limit=limit * 2)

    choices: List[app_commands.Choice[str]] = []
    for row in owned:
        hay = " ".join([
            row["name"] or "",
            row["set"] or "",
            row["rarity"] or "",
            row.get("code") or "" if isinstance(row, dict) else "",
            row.get("id") or "" if isinstance(row, dict) else "",
        ]).lower()
        if tokens and not all(tok in hay for tok in tokens):
            continue

        # Ensure this exact printing exists in the index; get a print_key back
        print_key = register_print_if_missing(state, {
            "cardname":  row["name"],
            "cardrarity": row["rarity"],
            "cardset":   row["set"],
            "cardcode":  row["code"],
            "cardid":    row["id"],
        })
        card = find_card_by_print_key(state, print_key)
        if not card:
            continue  # extremely unlikely after register_print_if_missing

        choices.append(app_commands.Choice(name=card_label(card), value=print_key))
        if len(choices) >= limit:
            break

    return choices

class Cards_Shop(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: AppState = bot.state

    # ---------- BUY ----------
    async def ac_buy(self, interaction: discord.Interaction, current: str):
        return _suggest_prints(self.state, current)

    @app_commands.command(name="buy", description="Buy a specific printing from the bot")
    @app_commands.guilds(GUILD)
    @app_commands.describe(card="Choose the exact printing", amount="How many to buy (max 3)")
    @app_commands.autocomplete(card=ac_buy)
    async def buy(self, interaction: discord.Interaction, card: str, amount: app_commands.Range[int, 1, 3] = 1):
        c = find_card_by_print_key(self.state, card)
        if not c:
            return await interaction.response.send_message("Card not found.", ephemeral=True)
        rarity = get_card_rarity(c)
        price_each = BUY_PRICES.get(rarity)
        if rarity == "starlight" or price_each is None:
            return await interaction.response.send_message("❌ This printing cannot be bought from the bot.", ephemeral=True)
        total = price_each * amount

        view = ConfirmBuyCardView(self.state, requester=interaction.user, print_key=card, amount=amount, total_cost=total)
        await interaction.response.send_message(
            f"Are you sure you want to buy **{amount}× {card_label(c)}** for **{total}** mambucks?",
            view=view, ephemeral=True
        )

    # ---------- SELL ----------
    async def ac_sell(self, interaction: discord.Interaction, current: str):
        return _suggest_owned_prints(self.state, interaction.user.id, current)

    @app_commands.command(name="sell", description="Sell a specific printing to the bot")
    @app_commands.guilds(GUILD)
    @app_commands.describe(card="Choose the exact printing you own", amount="How many to sell (max 10)")
    @app_commands.autocomplete(card=ac_sell)
    async def sell(self, interaction: discord.Interaction, card: str, amount: app_commands.Range[int, 1, 10] = 1):
        c = find_card_by_print_key(self.state, card)
        if not c:
            return await interaction.response.send_message("Card not found.", ephemeral=True)

        # set must be present (it will be, since we read from inventory)
        set_present = (c.get("set") or c.get("cardset") or "").strip()
        if not set_present:
            return await interaction.response.send_message(
                "This printing is missing a card set in the data and can’t be sold.", ephemeral=True
            )

        rarity = get_card_rarity(c)
        price_each = SELL_PRICES.get(rarity)
        if rarity == "starlight" or price_each is None:
            return await interaction.response.send_message("❌ This printing cannot be sold to the bot.", ephemeral=True)

        total = price_each * amount
        view = ConfirmSellCardView(self.state, requester=interaction.user, print_key=card, amount=amount, total_credit=total)
        await interaction.response.send_message(
            f"Are you sure you want to sell **{amount}× {card_label(c)}** for **{total}** mambucks?",
            view=view,
            ephemeral=True,
        )

async def setup(bot: commands.Bot):
    await bot.add_cog(Cards_Shop(bot))
