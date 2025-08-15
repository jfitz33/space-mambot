# cogs/trade.py
import os
from typing import List, Tuple

import discord
from discord.ext import commands
from discord import app_commands

from core.state import AppState
from core.packs import resolve_card_in_pack
from core.db import (
    db_trade_create, db_trade_get, db_trade_set_receiver_offer,
    db_trade_set_confirm, db_trade_cancel, db_user_has_items,
    db_apply_trade_atomic, db_trade_set_status,
)

# ---- Guild scoping (as requested) ----
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None


# ---------------- Helpers ----------------
def _iter_all_cards(state: AppState):
    """Yield dicts: {pack,name,rarity,card_code,card_id} for every card in all packs."""
    for pack_name, pack in (state.packs_index or {}).items():
        for items in pack["by_rarity"].values():
            for it in items:
                yield {
                    "pack": pack_name,
                    "name": it.get("name", ""),
                    "rarity": it.get("rarity", ""),
                    "card_code": it.get("card_code", "") or "",
                    "card_id": it.get("card_id", "") or "",
                }

def _fmt_item(it: dict) -> str:
    # no code/id in display
    return f"x{it['qty']} {it['name']} ({it['rarity']}, set:{it['card_set']})"

def _trade_embed(t: dict) -> discord.Embed:
    give_str = "\n".join(f"• {_fmt_item(it)}" for it in t.get("give", [])) or "• (nothing)"
    get_str  = "\n".join(f"• {_fmt_item(it)}" for it in t.get("get",  [])) or "• (nothing)"
    status = t["status"]
    checks = []
    if t.get("confirm_proposer"): checks.append("Proposer ✅")
    if t.get("confirm_receiver"): checks.append("Receiver ✅")
    conf_line = f"\n**Confirmations:** {' | '.join(checks) if checks else 'None'}"
    emb = discord.Embed(
        title=f"Trade #{t['trade_id']} — {status.replace('_',' ').title()}",
        description=(
            f"**From:** <@{t['proposer_id']}>\n"
            f"**To:** <@{t['receiver_id']}>\n\n"
            f"**Proposer gives:**\n{give_str}\n\n"
            f"**Receiver gives:**\n{get_str}{conf_line}"
        ),
        color=0x2b6cb0 if status.startswith("await") else (0x38a169 if status=="accepted" else 0xe53e3e)
    )
    return emb

def _parse_card_value(state: AppState, value: str) -> dict:
    """Choice value format: pack|||name|||code|||id -> canonical item dict (no qty)."""
    parts = (value or "").split("|||")
    while len(parts) < 4:
        parts.append("")
    pack, name, code, cid = parts[:4]
    item = resolve_card_in_pack(state, pack, name, code, cid)
    return {
        "name": name,
        "rarity": item.get("rarity", ""),
        "card_set": pack,
        "card_code": item.get("card_code", "") or "",
        "card_id": item.get("card_id", "") or "",
    }

def _collect_items(state: AppState, pairs: List[Tuple[int | None, str | None]]) -> List[dict]:
    """Build a list of items from (qty, card_value) pairs. Skips empty pairs; validates consistency."""
    items: List[dict] = []
    for qty, card in pairs:
        if (qty is None) and (not card):
            continue  # fully empty pair -> ignore
        if (qty is None) or (not card):
            raise ValueError("Each provided pair must have both quantity and card.")
        base = _parse_card_value(state, card)
        items.append({**base, "qty": int(qty)})
    return items


# --------------- Confirm UI ---------------
class TradeConfirmView(discord.ui.View):
    def __init__(self, state: AppState, trade_id: int, timeout: float = 300):
        super().__init__(timeout=timeout)
        self.state = state          # MUST be AppState
        self.trade_id = trade_id

    async def _refresh(self, interaction: discord.Interaction, notice: str | None = None):
        t = db_trade_get(self.state, self.trade_id)
        if not t:
            await interaction.response.send_message("Trade no longer exists.", ephemeral=True)
            return
        if notice:
            await interaction.response.send_message(notice, ephemeral=True)
        await interaction.message.edit(embed=_trade_embed(t), view=self if t["status"].startswith("await") else None)

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm_btn(self, interaction: discord.Interaction, _: discord.ui.Button):
        t = db_trade_get(self.state, self.trade_id)
        if not t:
            await interaction.response.send_message("Trade not found.", ephemeral=True); return
        uid = str(interaction.user.id)
        if uid not in (str(t["proposer_id"]), str(t["receiver_id"])):
            await interaction.response.send_message("Only trade participants can confirm.", ephemeral=True); return
        if t["status"] != "awaiting_confirm":
            await interaction.response.send_message("Trade is not awaiting confirmation.", ephemeral=True); return

        both = db_trade_set_confirm(self.state, self.trade_id, interaction.user.id)
        if both:
            # re-validate and apply atomically
            t = db_trade_get(self.state, self.trade_id)
            ok, msg = db_user_has_items(self.state, t["proposer_id"], t["give"])
            if not ok: await self._refresh(interaction, f"❌ Proposer lacks items: {msg}"); return
            ok, msg = db_user_has_items(self.state, t["receiver_id"], t["get"])
            if not ok: await self._refresh(interaction, f"❌ Receiver lacks items: {msg}"); return
            ok, msg = db_apply_trade_atomic(self.state, t)
            if not ok: await self._refresh(interaction, f"❌ {msg}"); return
            db_trade_set_status(self.state, self.trade_id, "accepted")
            t = db_trade_get(self.state, self.trade_id)
            await interaction.response.send_message("✅ Trade executed.", ephemeral=True)
            await interaction.message.edit(embed=_trade_embed(t), view=None)
            await interaction.channel.send(
                f"🤝 Trade **#{t['trade_id']}** completed: <@{t['proposer_id']}> ⇄ <@{t['receiver_id']}>"
            )
        else:
            await self._refresh(interaction, "✅ Confirmation recorded.")

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel_btn(self, interaction: discord.Interaction, _: discord.ui.Button):
        t = db_trade_get(self.state, self.trade_id)
        if not t:
            await interaction.response.send_message("Trade not found.", ephemeral=True); return

        uid = str(interaction.user.id)
        if uid not in (str(t["proposer_id"]), str(t["receiver_id"])):
            await interaction.response.send_message("Only participants can cancel.", ephemeral=True); return

        if not t["status"].startswith("await"):
            await interaction.response.send_message("Trade is no longer cancelable.", ephemeral=True); return

        # Cancel in DB
        db_trade_cancel(self.state, self.trade_id)

        # Try to delete the public confirmation message
        try:
            if t.get("public_chan_id") and t.get("public_msg_id"):
                ch = interaction.client.get_channel(int(t["public_chan_id"])) or await interaction.client.fetch_channel(int(t["public_chan_id"]))
                msg = await ch.fetch_message(int(t["public_msg_id"]))
                await msg.delete()
            else:
                # Fallback: delete the interaction message if that's the confirm UI
                await interaction.message.delete()
        except discord.Forbidden:
            await interaction.channel.send("⚠️ I don't have permission to delete the confirmation message.")
        except discord.NotFound:
            pass

        # Public cancel notice with display names
        def name_for(user_id: str) -> str:
            try:
                m = interaction.guild.get_member(int(user_id)) if interaction.guild else None
                return m.display_name if m else f"<@{user_id}>"
            except Exception:
                return f"<@{user_id}>"

        await interaction.channel.send(
            f"🛑 trade between **{name_for(t['proposer_id'])}** and **{name_for(t['receiver_id'])}** was cancelled"
        )

        # Ephemeral ack to clicker
        await interaction.response.send_message("🛑 Trade cancelled.", ephemeral=True)


# --------------- Cog ---------------
class Trade(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: AppState = self.bot.state

    # ---- Autocomplete: card fields (shared) ----
    async def ac_card(self, interaction: discord.Interaction, current: str):
        """
        Suggest card names across all packs, disambiguated by pack/rarity.
        Choice.value encodes: pack|||name|||code|||id
        """
        cur = (current or "").lower()
        choices: List[app_commands.Choice[str]] = []
        seen = set()
        for c in _iter_all_cards(self.state):
            if cur and cur not in c["name"].lower():
                continue
            key = (c["pack"], c["name"], c["card_code"], c["card_id"])
            if key in seen:
                continue
            seen.add(key)
            label = f"{c['name']} — set:{c['pack']} [{c['rarity']}]"
            value = f"{c['pack']}|||{c['name']}|||{c['card_code']}|||{c['card_id']}"
            # API limits ~100 chars each
            choices.append(app_commands.Choice(name=label[:100], value=value[:100]))
            if len(choices) >= 25:
                break
        return choices

    # ---------- Commands ----------

    @app_commands.command(name="trade_propose", description="Propose a trade: offer up to 5 items to another user")
    @app_commands.guilds(GUILD)
    @app_commands.describe(
        to_user="User you want to trade with (bots not allowed)",
        quantity1="Qty for card1", card1="Card 1",
        quantity2="Qty for card2 (optional)", card2="Card 2 (optional)",
        quantity3="Qty for card3 (optional)", card3="Card 3 (optional)",
        quantity4="Qty for card4 (optional)", card4="Card 4 (optional)",
        quantity5="Qty for card5 (optional)", card5="Card 5 (optional)",
    )
    @app_commands.autocomplete(card1=ac_card, card2=ac_card, card3=ac_card, card4=ac_card, card5=ac_card)
    async def trade_propose(
        self,
        interaction: discord.Interaction,
        to_user: discord.User,
        quantity1: app_commands.Range[int,1,999],
        card1: str,
        quantity2: app_commands.Range[int,1,999] | None = None,
        card2: str | None = None,
        quantity3: app_commands.Range[int,1,999] | None = None,
        card3: str | None = None,
        quantity4: app_commands.Range[int,1,999] | None = None,
        card4: str | None = None,
        quantity5: app_commands.Range[int,1,999] | None = None,
        card5: str | None = None,
    ):
        if to_user.bot:
            await interaction.response.send_message("❌ You cannot trade with bots.", ephemeral=True); return
        if to_user.id == interaction.user.id:
            await interaction.response.send_message("❌ You can’t trade with yourself.", ephemeral=True); return

        try:
            give_items = _collect_items(self.state, [
                (quantity1, card1),
                (quantity2, card2),
                (quantity3, card3),
                (quantity4, card4),
                (quantity5, card5),
            ])
        except Exception as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True); return

        if not give_items:
            await interaction.response.send_message("❌ Provide at least one (quantity, card) pair.", ephemeral=True); return

        ok, msg = db_user_has_items(self.state, interaction.user.id, give_items)
        if not ok:
            await interaction.response.send_message(f"❌ You don’t have the offered items: {msg}", ephemeral=True); return

        trade_id = db_trade_create(self.state, interaction.user.id, to_user.id, give_items, note="")
        t = db_trade_get(self.state, trade_id)
        await interaction.response.send_message(
            content=(f"{to_user.mention} a trade has been proposed to you by {interaction.user.mention}. "
                     f"Use `/trade_accept trade_id:{trade_id}` to add your items."),
            embed=_trade_embed(t)
        )

    @app_commands.command(name="trade_accept", description="Receiver adds up to 5 items to a pending trade")
    @app_commands.guilds(GUILD)
    @app_commands.describe(
        trade_id="Pending trade ID",
        quantity1="Qty for card1", card1="Card 1",
        quantity2="Qty for card2 (optional)", card2="Card 2 (optional)",
        quantity3="Qty for card3 (optional)", card3="Card 3 (optional)",
        quantity4="Qty for card4 (optional)", card4="Card 4 (optional)",
        quantity5="Qty for card5 (optional)", card5="Card 5 (optional)",
    )
    @app_commands.autocomplete(card1=ac_card, card2=ac_card, card3=ac_card, card4=ac_card, card5=ac_card)
    async def trade_accept(
        self,
        interaction: discord.Interaction,
        trade_id: int,
        quantity1: app_commands.Range[int,1,999],
        card1: str,
        quantity2: app_commands.Range[int,1,999] | None = None,
        card2: str | None = None,
        quantity3: app_commands.Range[int,1,999] | None = None,
        card3: str | None = None,
        quantity4: app_commands.Range[int,1,999] | None = None,
        card4: str | None = None,
        quantity5: app_commands.Range[int,1,999] | None = None,
        card5: str | None = None,
    ):
        t = db_trade_get(self.state, trade_id)
        if not t:
            await interaction.response.send_message("Trade not found.", ephemeral=True); return
        if str(interaction.user.id) != str(t["receiver_id"]):
            await interaction.response.send_message("Only the receiver can add their items.", ephemeral=True); return
        if t["status"] != "awaiting_receiver":
            await interaction.response.send_message("This trade is not awaiting a receiver offer.", ephemeral=True); return

        try:
            get_items = _collect_items(self.state, [
                (quantity1, card1),
                (quantity2, card2),
                (quantity3, card3),
                (quantity4, card4),
                (quantity5, card5),
            ])
        except Exception as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True); return

        if not get_items:
            await interaction.response.send_message("❌ Provide at least one (quantity, card) pair.", ephemeral=True); return

        ok, msg = db_user_has_items(self.state, interaction.user.id, get_items)
        if not ok:
            await interaction.response.send_message(f"❌ You don’t have those items: {msg}", ephemeral=True); return

        db_trade_set_receiver_offer(self.state, trade_id, interaction.user.id, get_items)
        t = db_trade_get(self.state, trade_id)

        # after: db_trade_set_receiver_offer(...); t = db_trade_get(...)

        view = TradeConfirmView(self.bot.state, trade_id)

        # Send the confirmation UI in the channel
        await interaction.response.send_message(
            content=(f"{interaction.user.mention} has added their items to trade **#{trade_id}**.\n"
                    f"**Both players** must now confirm."),
            embed=_trade_embed(t),
            view=view
        )

        # Grab the created message and save channel/message IDs
        msg = await interaction.original_response()
        from core.db import db_trade_store_public_message
        db_trade_store_public_message(self.state, trade_id, chan_id=msg.channel.id, msg_id=msg.id)


    @app_commands.command(name="trade_show", description="Show a trade’s details")
    @app_commands.guilds(GUILD)
    @app_commands.describe(trade_id="Trade ID to view")
    async def trade_show(self, interaction: discord.Interaction, trade_id: int):
        t = db_trade_get(self.state, trade_id)
        if not t:
            await interaction.response.send_message("Trade not found.", ephemeral=True); return
        await interaction.response.send_message(embed=_trade_embed(t), ephemeral=True)

    @app_commands.command(name="trade_confirm", description="Confirm a trade you are part of")
    @app_commands.guilds(GUILD)
    @app_commands.describe(trade_id="Trade ID to confirm")
    async def trade_confirm(self, interaction: discord.Interaction, trade_id: int):
        t = db_trade_get(self.state, trade_id)
        if not t:
            await interaction.response.send_message("Trade not found.", ephemeral=True); return
        if t["status"] != "awaiting_confirm":
            await interaction.response.send_message("Trade is not awaiting confirmation.", ephemeral=True); return
        uid = str(interaction.user.id)
        if uid not in (str(t["proposer_id"]), str(t["receiver_id"])):
            await interaction.response.send_message("Only participants can confirm.", ephemeral=True); return
        both = db_trade_set_confirm(self.state, trade_id, interaction.user.id)
        if not both:
            await interaction.response.send_message("✅ Confirmation recorded. Waiting on the other player.", ephemeral=True)
            return
        t = db_trade_get(self.state, trade_id)
        ok, msg = db_user_has_items(self.state, t["proposer_id"], t["give"])
        if not ok: await interaction.response.send_message(f"❌ Proposer lacks items: {msg}", ephemeral=True); return
        ok, msg = db_user_has_items(self.state, t["receiver_id"], t["get"])
        if not ok: await interaction.response.send_message(f"❌ Receiver lacks items: {msg}", ephemeral=True); return
        ok, msg = db_apply_trade_atomic(self.state, t)
        if not ok: await interaction.response.send_message(f"❌ {msg}", ephemeral=True); return
        db_trade_set_status(self.state, trade_id, "accepted")
        await interaction.response.send_message("✅ Trade executed.", ephemeral=True)
        await interaction.channel.send(
            f"🤝 Trade **#{trade_id}** completed: <@{t['proposer_id']}> ⇄ <@{t['receiver_id']}>"
        )

    @app_commands.command(name="trade_cancel", description="Cancel a pending trade (by ID or your latest)")
    @app_commands.guilds(GUILD)
    @app_commands.describe(trade_id="Trade ID (optional). If omitted, cancels your latest pending trade.")
    async def trade_cancel_cmd(self, interaction: discord.Interaction, trade_id: int | None = None):
        # Resolve which trade to cancel
        t = db_trade_get(self.state, trade_id) if trade_id else db_trade_get_active_for_user(self.state, interaction.user.id)
        if not t:
            await interaction.response.send_message("No pending trade found.", ephemeral=True); return

        if str(interaction.user.id) not in (str(t["proposer_id"]), str(t["receiver_id"])):
            await interaction.response.send_message("Only participants can cancel this trade.", ephemeral=True); return

        if not t["status"].startswith("await"):
            await interaction.response.send_message("Trade is no longer cancelable.", ephemeral=True); return

        # Cancel in DB
        db_trade_cancel(self.state, t["trade_id"])

        # Try to delete the public confirmation message
        try:
            if t.get("public_chan_id") and t.get("public_msg_id"):
                ch = interaction.client.get_channel(int(t["public_chan_id"])) or await interaction.client.fetch_channel(int(t["public_chan_id"]))
                msg = await ch.fetch_message(int(t["public_msg_id"]))
                await msg.delete()
        except discord.Forbidden:
            await interaction.channel.send("⚠️ I don't have permission to delete the confirmation message.")
        except discord.NotFound:
            pass

        # Public cancel notice with display names
        def name_for(user_id: str) -> str:
            try:
                m = interaction.guild.get_member(int(user_id)) if interaction.guild else None
                return m.display_name if m else f"<@{user_id}>"
            except Exception:
                return f"<@{user_id}>"

        # Post in the channel where the command was used (you could also use stored public_chan_id instead)
        await interaction.channel.send(
            f"🛑 trade between **{name_for(t['proposer_id'])}** and **{name_for(t['receiver_id'])}** was cancelled"
        )

        await interaction.response.send_message("🛑 Trade cancelled.", ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(Trade(bot))
