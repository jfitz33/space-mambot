# cogs/trade.py
import os
from typing import List, Tuple, Optional

import discord
from discord.ext import commands
from discord import app_commands

from core.state import AppState
from core.packs import resolve_card_in_pack
from core.db import (
    db_trade_create, db_trade_get, db_trade_set_receiver_offer,
    db_trade_set_confirm, db_trade_cancel, db_user_has_items,
    db_apply_trade_atomic, db_trade_set_status,
    db_trade_get_active_for_user, db_trade_store_public_message,
    db_shards_get,
    db_collection_list_owned_prints,
    db_wishlist_add, db_wishlist_remove, db_wishlist_list, db_wishlist_clear,
    db_wishlist_holders,
    db_binder_add, db_binder_remove, db_binder_list, db_binder_clear,
    db_binder_holders,
)
from core.cards_shop import (
    register_print_if_missing,
    find_card_by_print_key,
    card_label,
    get_card_rarity,
    resolve_card_set,
    ensure_shop_index,
)
from core.currency import shard_set_name, SHARD_SET_NAMES
from cogs.collection import (
    build_badge_tokens_from_state,
    group_and_format_rows,
    sections_to_embed_descriptions,
)

# ---- Guild scoping ----
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None


# ---------------- Formatting helpers ----------------
def _fmt_card_line(it: dict) -> str:
    # expects: {"qty", "name", "rarity", "card_set"}
    return f"x{it['qty']} {it['name']} ({it['rarity']}, set:{it['card_set']})"

def _fmt_item_line(it: dict) -> str:
    if it.get("kind") == "shards":
        pretty = shard_set_name(int(it["set_id"]))
        return f"+{int(it['amount'])} {pretty}"
    # card
    return _fmt_card_line(it)

def _trade_embed(t: dict) -> discord.Embed:
    give_str = "\n".join(f"• {_fmt_item_line(it)}" for it in t.get("give", [])) or "• (nothing)"
    get_str  = "\n".join(f"• {_fmt_item_line(it)}" for it in t.get("get",  [])) or "• (nothing)"
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


# ---------------- Card print-key plumbing ----------------
def _item_dict_from_print_key(state: AppState, key: str) -> dict:
    """
    Convert a shop print_key -> canonical item dict for DB checks:
      { "name","rarity","card_set","card_code","card_id","qty": <later> }
    Raises ValueError if the key cannot be resolved or set missing.
    """
    card = find_card_by_print_key(state, key)
    if not card:
        raise ValueError("Card printing not found.")
    set_name = resolve_card_set(state, card)
    if not set_name:
        raise ValueError("Printing has no set; cannot trade.")
    return {
        "name": (card.get("name") or card.get("cardname") or "").strip(),
        "rarity": (card.get("rarity") or card.get("cardrarity") or "").strip(),
        "card_set": set_name,
        "card_code": (card.get("code") or card.get("cardcode") or "") or "",
        "card_id": (card.get("id") or card.get("cardid") or "") or "",
    }

def _collect_items_from_keys(state: AppState, pairs: List[Tuple[Optional[int], Optional[str]]]) -> List[dict]:
    """
    Build list of CARD items from (qty, print_key) pairs.
    Skips fully-empty rows; errors if one part missing.
    """
    items: List[dict] = []
    for qty, key in pairs:
        if (qty is None) and (not key):
            continue
        if (qty is None) or (not key):
            raise ValueError("Each provided pair must have both quantity and card.")
        base = _item_dict_from_print_key(state, key)
        base["qty"] = int(qty)
        items.append(base)
    return items


# ---------------- Shard helpers ----------------
def _parse_shard_type(raw: Optional[str]) -> Optional[int]:
    if not raw:
        return None
    s = str(raw).strip()
    try:
        return int(s)
    except Exception:
        # allow passing a name like "Frostfire Shards" -> set_id=1 if your helper supports reverse lookup
        # simplest: expect numeric set_id in value; names only for display
        return None

# --------------- Confirm UI ---------------
class TradeConfirmView(discord.ui.View):
    def __init__(self, state: AppState, trade_id: int, timeout: float = 300):
        super().__init__(timeout=timeout)
        self.state = state
        self.trade_id = trade_id

    async def _refresh(self, interaction: discord.Interaction, notice: str | None = None):
        t = db_trade_get(self.state, self.trade_id)
        if not t:
            await interaction.response.send_message("Trade no longer exists.", ephemeral=True)
            return
        if notice:
            await interaction.response.send_message(notice, ephemeral=True)
        await interaction.message.edit(embed=_trade_embed(t), view=self if t["status"].startswith("await") else None)

    async def on_timeout(self):
        # Turn the UI into a cancelled notice if still pending
        try:
            t = db_trade_get(self.state, self.trade_id)
            if t and t["status"].startswith("await"):
                db_trade_cancel(self.state, self.trade_id)
                # If we still have the original message, replace components with text
                try:
                    # Typically we don't have direct message ref here; best-effort:
                    # We cannot fetch interaction.message in on_timeout, so rely on channel storage:
                    # If you saved public message IDs in DB, you could refetch and edit; here we just noop.
                    pass
                except Exception:
                    pass
        except Exception:
            pass

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

        # Ensure at least one side has a CARD (no shards-for-shards)
        def any_card(items: List[dict]) -> bool:
            for it in items or []:
                if it.get("kind") != "shards":
                    return True
            return False
        if not (any_card(t.get("give")) or any_card(t.get("get"))):
            await interaction.response.send_message("❌ At least one side must include a card.", ephemeral=True)
            return

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

        db_trade_cancel(self.state, self.trade_id)

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
        await interaction.response.send_message("🛑 Trade cancelled.", ephemeral=True)


# --------------- Cog ---------------
class Trade(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: AppState = self.bot.state

    # ---- Autocomplete: OWNED prints only (for both proposer & receiver) ----
    async def ac_card_owned(self, interaction: discord.Interaction, current: str):
        """
        Suggest ONLY prints the CALLER owns.
        Choice.value = shop print_key (short, safe for Discord limits)
        """
        cur = (current or "").lower()
        choices: List[app_commands.Choice[str]] = []
        seen = set()

        owned = db_collection_list_owned_prints(self.state, interaction.user.id, name_filter=None, limit=200)
        for row in owned:
            name = (row.get("name") or "").strip()
            pack = (row.get("set") or "").strip()
            rarity = (row.get("rarity") or "").strip()
            code = (row.get("code") or "") or ""
            cid  = (row.get("id") or "") or ""

            if cur and cur not in name.lower():
                continue

            # Register a print_key for this exact row so value is compact and consistent
            key = register_print_if_missing(self.state, {
                "cardname": name,
                "cardrarity": rarity,
                "cardset": pack,
                "cardcode": code,
                "cardid": cid,
            })
            if key in seen:
                continue
            seen.add(key)

            label = f"{name} — set:{pack} [{rarity}]"
            choices.append(app_commands.Choice(name=label[:100], value=key))  # value=print_key
            if len(choices) >= 25:
                break
        return choices
    
    def _known_shard_sets(self) -> list[tuple[int, str]]:
        """
        [(set_id, pretty_name)] for shard types that actually exist now.
        Uses core.currency.SHARD_SET_NAMES so only real types are suggested.
        Preserves insertion order of the dict (e.g., Frostfire first).
        """
        return [(int(sid), name) for sid, name in SHARD_SET_NAMES.items()]
    
    def _suggest_prints_any(self, query: str, limit: int = 25) -> list[app_commands.Choice[str]]:
        ensure_shop_index(self.state)
        tokens = [t for t in (query or "").lower().split() if t]
        seen: dict[tuple[str, str, str, str], tuple[str, dict]] = {}
        for key, card in getattr(self.state, "_shop_print_by_key", {}).items():
            name = (card.get("name") or card.get("cardname") or "").strip()
            rarity = (card.get("rarity") or card.get("cardrarity") or "").strip()
            code = (card.get("code") or card.get("cardcode") or "").strip()
            cid  = (card.get("id") or card.get("cardid") or "").strip()
            set_name = (card.get("set") or card.get("cardset") or "").strip()
            if not name or not set_name:
                continue
            hay = f"{name} {set_name} {rarity} {code} {cid}".lower()
            if tokens and not all(tok in hay for tok in tokens):
                continue
            sig = (name.lower(), rarity.lower(), code.lower(), cid.lower())
            if sig in seen:
                continue
            seen[sig] = (key, card)
        choices: list[app_commands.Choice[str]] = []
        for key, card in sorted(seen.values(), key=lambda item: card_label(item[1]).lower()):
            choices.append(app_commands.Choice(name=card_label(card), value=key))
            if len(choices) >= limit:
                break
        return choices

    def _rows_to_choices(self, rows: List[dict], current: str, qty_label: str) -> list[app_commands.Choice[str]]:
        cur = (current or "").lower()
        choices: list[app_commands.Choice[str]] = []
        for row in rows:
            card = {
                "cardname": row.get("card_name"),
                "cardrarity": row.get("card_rarity"),
                "cardset": row.get("card_set"),
                "cardcode": row.get("card_code"),
                "cardid": row.get("card_id"),
            }
            key = register_print_if_missing(self.state, card)
            rarity = (row.get("card_rarity") or "").upper()
            set_name = row.get("card_set") or "Unknown"
            qty = row.get(qty_label, row.get("qty", 0))
            label = f"{row.get('card_name')} — {set_name} [{rarity}] (x{qty})"
            if cur and cur not in label.lower():
                continue
            choices.append(app_commands.Choice(name=label[:100], value=key))
            if len(choices) >= 25:
                break
        return choices

    async def _build_collection_style_embeds(self, title: str, rows: List[dict], qty_label: str) -> List[discord.Embed]:
        formatted_rows: List[tuple] = []
        for row in rows:
            qty_raw = row.get(qty_label, row.get("qty", 0))
            try:
                qty = int(qty_raw or 0)
            except Exception:
                qty = 0
            formatted_rows.append(
                (
                    row.get("card_name"),
                    qty,
                    row.get("card_rarity"),
                    row.get("card_set"),
                    row.get("card_code"),
                    row.get("card_id"),
                )
            )

        badges = await build_badge_tokens_from_state(self.bot, self.state)
        sections = group_and_format_rows(formatted_rows, self.state, badges)

        if not sections:
            return [discord.Embed(title=title, description="(No entries)")]

        descriptions = sections_to_embed_descriptions(sections, per_embed_limit=3900)
        embeds: List[discord.Embed] = []
        for idx, desc in enumerate(descriptions):
            embed = discord.Embed(description=desc)
            if idx == 0:
                embed.title = title
            else:
                embed.title = f"{title} (cont.)"
            embeds.append(embed)
        return embeds

    async def ac_shard_type(self, interaction: discord.Interaction, current: str):
        """
        Autocomplete shard types by pretty name.
        Choice.value = set_id (as string), Choice.name = pretty.
        """
        q = (current or "").lower()
        choices: list[app_commands.Choice[str]] = []
        for sid, pretty in self._known_shard_sets():
            if q and q not in pretty.lower():
                continue
            choices.append(app_commands.Choice(name=pretty[:100], value=str(sid)))
            if len(choices) >= 25:
                break
        return choices
    
    async def ac_any_print(self, interaction: discord.Interaction, current: str):
        return self._suggest_prints_any(current)

    async def ac_wishlist_entry(self, interaction: discord.Interaction, current: str):
        rows = db_wishlist_list(self.state, interaction.user.id)
        return self._rows_to_choices(rows, current, "qty")

    async def ac_binder_entry(self, interaction: discord.Interaction, current: str):
        rows = db_binder_list(self.state, interaction.user.id)
        return self._rows_to_choices(rows, current, "qty")

    # ---------- Commands ----------

    @app_commands.command(
        name="wishlist_add",
        description="Add a card to your wishlist."
    )
    @app_commands.guilds(GUILD)
    @app_commands.describe(
        name="Card to add",
        copies="Number of copies you want (default 1)",
    )
    @app_commands.autocomplete(name=ac_any_print)
    async def wishlist_add(
        self,
        interaction: discord.Interaction,
        name: str,
        copies: app_commands.Range[int,1,999] = 1,
    ):
        card = find_card_by_print_key(self.state, name)
        if not card:
            await interaction.response.send_message("❌ Card not found.", ephemeral=True)
            return
        try:
            item = _item_dict_from_print_key(self.state, name)
        except ValueError as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)
            return
        total = db_wishlist_add(self.state, interaction.user.id, item, copies)
        await interaction.response.send_message(
            f"✅ Added **x{copies}** {card_label(card)} to your wishlist. Total desired: **{total}**.",
            ephemeral=True,
        )

    @app_commands.command(
        name="wishlist_remove",
        description="Remove a card from your wishlist."
    )
    @app_commands.guilds(GUILD)
    @app_commands.describe(
        name="Card to remove",
        copies="How many copies to remove (default 1)",
    )
    @app_commands.autocomplete(name=ac_wishlist_entry)
    async def wishlist_remove(
        self,
        interaction: discord.Interaction,
        name: str,
        copies: app_commands.Range[int,1,999] = 1,
    ):
        card = find_card_by_print_key(self.state, name)
        if not card:
            await interaction.response.send_message("❌ Card not found in wishlist.", ephemeral=True)
            return
        try:
            item = _item_dict_from_print_key(self.state, name)
        except ValueError:
            await interaction.response.send_message("❌ Unable to resolve that card printing.", ephemeral=True)
            return
        removed, remaining = db_wishlist_remove(self.state, interaction.user.id, item, copies)
        if removed <= 0:
            await interaction.response.send_message("ℹ️ That card isn't on your wishlist.", ephemeral=True)
            return
        await interaction.response.send_message(
            f"✅ Removed **x{removed}** {card_label(card)} from your wishlist. Remaining desired: **{remaining}**.",
            ephemeral=True,
        )

    @app_commands.command(
        name="wishlist_display",
        description="Show a player's wishlist in this channel."
    )
    @app_commands.guilds(GUILD)
    @app_commands.describe(user="Player whose wishlist to show (defaults to you)")
    async def wishlist_display(self, interaction: discord.Interaction, user: Optional[discord.User] = None):
        target = user or interaction.user
        rows = db_wishlist_list(self.state, target.id)
        embeds = await self._build_collection_style_embeds(
            f"{target.display_name}'s Wishlist", rows, "qty"
        )
        await interaction.response.send_message(embeds=embeds[:10])
        for i in range(10, len(embeds), 10):
            await interaction.followup.send(embeds=embeds[i:i+10])

    @app_commands.command(
        name="wishlist_clear",
        description="Clear your wishlist."
    )
    @app_commands.guilds(GUILD)
    async def wishlist_clear(self, interaction: discord.Interaction):
        removed = db_wishlist_clear(self.state, interaction.user.id)
        await interaction.response.send_message(
            f"✅ Cleared your wishlist ({removed} entries removed).",
            ephemeral=True,
        )

    @app_commands.command(
        name="binder_add",
        description="Add card copies from your collection into your binder."
    )
    @app_commands.guilds(GUILD)
    @app_commands.describe(
        name="Card to move into your binder",
        copies="Number of copies to add (default 1)",
    )
    @app_commands.autocomplete(name=ac_card_owned)
    async def binder_add(
        self,
        interaction: discord.Interaction,
        name: str,
        copies: app_commands.Range[int,1,999] = 1,
    ):
        card = find_card_by_print_key(self.state, name)
        if not card:
            await interaction.response.send_message("❌ Card not found.", ephemeral=True)
            return
        try:
            item = _item_dict_from_print_key(self.state, name)
        except ValueError as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)
            return

        existing = 0
        for row in db_binder_list(self.state, interaction.user.id):
            row_key = register_print_if_missing(self.state, {
                "cardname": row.get("card_name"),
                "cardrarity": row.get("card_rarity"),
                "cardset": row.get("card_set"),
                "cardcode": row.get("card_code"),
                "cardid": row.get("card_id"),
            })
            if row_key == name:
                existing = int(row.get("qty", 0))
                break

        check_item = dict(item)
        check_item["qty"] = existing + copies
        ok, msg = db_user_has_items(self.state, interaction.user.id, [check_item])
        if not ok:
            await interaction.response.send_message(
                "❌ You don't have enough copies in your collection to hold that many in your binder.",
                ephemeral=True,
            )
            return

        total = db_binder_add(self.state, interaction.user.id, item, copies)
        await interaction.response.send_message(
            f"✅ Added **x{copies}** {card_label(card)} to your binder. Binder total: **{total}**.",
            ephemeral=True,
        )

    @app_commands.command(
        name="binder_remove",
        description="Remove card copies from your binder."
    )
    @app_commands.guilds(GUILD)
    @app_commands.describe(
        name="Card to remove",
        copies="Number of copies to remove (default 1)",
    )
    @app_commands.autocomplete(name=ac_binder_entry)
    async def binder_remove(
        self,
        interaction: discord.Interaction,
        name: str,
        copies: app_commands.Range[int,1,999] = 1,
    ):
        card = find_card_by_print_key(self.state, name)
        if not card:
            await interaction.response.send_message("❌ Card not found in your binder.", ephemeral=True)
            return
        try:
            item = _item_dict_from_print_key(self.state, name)
        except ValueError:
            await interaction.response.send_message("❌ Unable to resolve that card printing.", ephemeral=True)
            return
        removed, remaining = db_binder_remove(self.state, interaction.user.id, item, copies)
        if removed <= 0:
            await interaction.response.send_message("ℹ️ That card is not in your binder.", ephemeral=True)
            return
        await interaction.response.send_message(
            f"✅ Removed **x{removed}** {card_label(card)} from your binder. Binder total: **{remaining}**.",
            ephemeral=True,
        )

    @app_commands.command(
        name="binder",
        description="Show a player's binder in this channel."
    )
    @app_commands.guilds(GUILD)
    @app_commands.describe(user="Player whose binder to show (defaults to you)")
    async def binder(self, interaction: discord.Interaction, user: Optional[discord.User] = None):
        target = user or interaction.user
        rows = db_binder_list(self.state, target.id)
        embeds = await self._build_collection_style_embeds(
            f"{target.display_name}'s Binder", rows, "qty"
        )
        await interaction.response.send_message(embeds=embeds[:10])
        for i in range(10, len(embeds), 10):
            await interaction.followup.send(embeds=embeds[i:i+10])

    @app_commands.command(
        name="search",
        description="Find players who have or want a specific card.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.describe(name="Card to search for")
    @app_commands.autocomplete(name=ac_any_print)
    async def search(self, interaction: discord.Interaction, name: str):
        card = find_card_by_print_key(self.state, name)
        if not card:
            await interaction.response.send_message("❌ Card not found.", ephemeral=True)
            return
        try:
            identity = _item_dict_from_print_key(self.state, name)
        except ValueError as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)
            return

        binder_holders = db_binder_holders(self.state, identity)
        wishlist_holders = db_wishlist_holders(self.state, identity)

        def format_holder_lines(entries: List[dict]) -> str:
            if not entries:
                return "*(none)*"
            entries_sorted = sorted(entries, key=lambda r: (-int(r.get("qty", 0)), r.get("user_id", "")))
            lines = []
            for entry in entries_sorted[:20]:
                qty = int(entry.get("qty", 0))
                user_id = entry.get("user_id")
                mention = f"<@{user_id}>" if user_id is not None else "Unknown"
                lines.append(f"• x{qty} — {mention}")
            if len(entries_sorted) > 20:
                lines.append(f"…and {len(entries_sorted) - 20} more")
            return "\n".join(lines)

        embed = discord.Embed(
            title=f"Search results: {card_label(card)}",
        )
        embed.add_field(name="In binders", value=format_holder_lines(binder_holders), inline=False)
        embed.add_field(name="On wishlists", value=format_holder_lines(wishlist_holders), inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(
        name="binder_clear",
        description="Empty your binder."
    )
    @app_commands.guilds(GUILD)
    async def binder_clear(self, interaction: discord.Interaction):
        removed = db_binder_clear(self.state, interaction.user.id)
        await interaction.response.send_message(
            f"✅ Cleared your binder ({removed} entries removed).",
            ephemeral=True,
        )

    @app_commands.command(
        name="trade_propose",
        description="Propose a trade: offer up to 5 card items (and optional shards) to another user"
    )
    @app_commands.guilds(GUILD)
    @app_commands.describe(
        to_user="User you want to trade with (bots not allowed)",
        quantity1="Qty for card1 (optional)", card1="Card 1 (optional)",
        quantity2="Qty for card2 (optional)", card2="Card 2 (optional)",
        quantity3="Qty for card3 (optional)", card3="Card 3 (optional)",
        quantity4="Qty for card4 (optional)", card4="Card 4 (optional)",
        quantity5="Qty for card5 (optional)", card5="Card 5 (optional)",
        shard_type="(optional) Shard type to include",
        shard_amount="(optional) Amount of shards to include (>=1)",
    )
    @app_commands.autocomplete(
        card1=ac_card_owned, card2=ac_card_owned, card3=ac_card_owned, card4=ac_card_owned, card5=ac_card_owned,
        shard_type=ac_shard_type
    )
    async def trade_propose(
        self,
        interaction: discord.Interaction,
        to_user: discord.User,
        quantity1: app_commands.Range[int,1,999] | None = None,
        card1: str | None = None,   # print_key
        quantity2: app_commands.Range[int,1,999] | None = None,
        card2: str | None = None,
        quantity3: app_commands.Range[int,1,999] | None = None,
        card3: str | None = None,
        quantity4: app_commands.Range[int,1,999] | None = None,
        card4: str | None = None,
        quantity5: app_commands.Range[int,1,999] | None = None,
        card5: str | None = None,
        shard_type: Optional[str] = None,
        shard_amount: Optional[app_commands.Range[int,1,1_000_000]] = None,
    ):
        if to_user.bot:
            await interaction.response.send_message("❌ You cannot trade with bots.", ephemeral=True); return
        if to_user.id == interaction.user.id:
            await interaction.response.send_message("❌ You can’t trade with yourself.", ephemeral=True); return

        # Build CARD items (can be empty; shards-only allowed at this stage)
        try:
            give_cards = _collect_items_from_keys(self.state, [
                (quantity1, card1),
                (quantity2, card2),
                (quantity3, card3),
                (quantity4, card4),
                (quantity5, card5),
            ])
        except Exception as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True); return

        give_items: List[dict] = list(give_cards)

        # Optional shards for proposer
        sid = _parse_shard_type(shard_type)
        if sid and shard_amount:
            have = db_shards_get(self.state, interaction.user.id, sid)
            if have < int(shard_amount):
                pretty = SHARD_SET_NAMES.get(sid, f"Shards (Set {sid})")
                await interaction.response.send_message(
                    f"❌ You don’t have enough **{pretty}** (have {have}, need {int(shard_amount)}).",
                    ephemeral=True,
                )
                return
            give_items.append({"kind": "shards", "set_id": sid, "amount": int(shard_amount)})

        if not give_items:
            await interaction.response.send_message("❌ Provide at least one item (card or shards).", ephemeral=True); return

        # Verify the proposer actually owns the offered items
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

    @app_commands.command(
        name="trade_accept",
        description="Receiver adds up to 5 items (and optional shards) to a pending trade"
    )
    @app_commands.guilds(GUILD)
    @app_commands.describe(
        trade_id="Pending trade ID",
        quantity1="Qty for card1 (optional)", card1="Card 1 (optional)",
        quantity2="Qty for card2 (optional)", card2="Card 2 (optional)",
        quantity3="Qty for card3 (optional)", card3="Card 3 (optional)",
        quantity4="Qty for card4 (optional)", card4="Card 4 (optional)",
        quantity5="Qty for card5 (optional)", card5="Card 5 (optional)",
        shard_type="(optional) Shard type to include",
        shard_amount="(optional) Amount of shards to include (>=1)",
    )
    @app_commands.autocomplete(
        card1=ac_card_owned, card2=ac_card_owned, card3=ac_card_owned, card4=ac_card_owned, card5=ac_card_owned,
        shard_type=ac_shard_type
    )
    async def trade_accept(
        self,
        interaction: discord.Interaction,
        trade_id: int,
        quantity1: app_commands.Range[int,1,999] | None = None,
        card1: str | None = None,
        quantity2: app_commands.Range[int,1,999] | None = None,
        card2: str | None = None,
        quantity3: app_commands.Range[int,1,999] | None = None,
        card3: str | None = None,
        quantity4: app_commands.Range[int,1,999] | None = None,
        card4: str | None = None,
        quantity5: app_commands.Range[int,1,999] | None = None,
        card5: str | None = None,
        shard_type: Optional[str] = None,
        shard_amount: Optional[app_commands.Range[int,1,1_000_000]] = None,
    ):
        t = db_trade_get(self.state, trade_id)
        if not t:
            await interaction.response.send_message("Trade not found.", ephemeral=True); return
        if str(interaction.user.id) != str(t["receiver_id"]):
            await interaction.response.send_message("Only the receiver can add their items.", ephemeral=True); return
        if t["status"] != "awaiting_receiver":
            await interaction.response.send_message("This trade is not awaiting a receiver offer.", ephemeral=True); return

        try:
            get_cards = _collect_items_from_keys(self.state, [
                (quantity1, card1),
                (quantity2, card2),
                (quantity3, card3),
                (quantity4, card4),
                (quantity5, card5),
            ])
        except Exception as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True); return

        get_items: List[dict] = list(get_cards)

        sid = _parse_shard_type(shard_type)
        if sid and shard_amount:
            have = db_shards_get(self.state, interaction.user.id, sid)
            if have < int(shard_amount):
                pretty = SHARD_SET_NAMES.get(sid, f"Shards (Set {sid})")
                await interaction.response.send_message(
                    f"❌ You don’t have enough **{pretty}** (have {have}, need {int(shard_amount)}).",
                    ephemeral=True,
                )
                return
            get_items.append({"kind": "shards", "set_id": sid, "amount": int(shard_amount)})

        if not get_items:
            await interaction.response.send_message("❌ Provide at least one item (card or shards).", ephemeral=True); return

        # Receiver must own whatever they’re adding
        ok, msg = db_user_has_items(self.state, interaction.user.id, get_items)
        if not ok:
            await interaction.response.send_message(f"❌ You don’t have those items: {msg}", ephemeral=True); return

        db_trade_set_receiver_offer(self.state, trade_id, interaction.user.id, get_items)
        t = db_trade_get(self.state, trade_id)

        view = TradeConfirmView(self.bot.state, trade_id)

        await interaction.response.send_message(
            content=(f"{interaction.user.mention} has added their items to trade **#{trade_id}**.\n"
                     f"**Both players** must now confirm."),
            embed=_trade_embed(t),
            view=view
        )

        # Save channel/message IDs (for potential timeout housekeeping)
        msg = await interaction.original_response()
        db_trade_store_public_message(self.state, trade_id, chan_id=msg.channel.id, msg_id=msg.id)

    @app_commands.command(name="trade_cancel", description="Cancel a pending trade (by ID or your latest)")
    @app_commands.guilds(GUILD)
    @app_commands.describe(trade_id="Trade ID (optional). If omitted, cancels your latest pending trade.")
    async def trade_cancel_cmd(self, interaction: discord.Interaction, trade_id: int | None = None):
        t = db_trade_get(self.state, trade_id) if trade_id else db_trade_get_active_for_user(self.state, interaction.user.id)
        if not t:
            await interaction.response.send_message("No pending trade found.", ephemeral=True); return

        if str(interaction.user.id) not in (str(t["proposer_id"]), str(t["receiver_id"])):
            await interaction.response.send_message("Only participants can cancel this trade.", ephemeral=True); return

        if not t["status"].startswith("await"):
            await interaction.response.send_message("Trade is no longer cancelable.", ephemeral=True); return

        db_trade_cancel(self.state, t["trade_id"])

        def name_for(user_id: str) -> str:
            try:
                m = interaction.guild.get_member(int(user_id)) if interaction.guild else None
                return m.display_name if m else f"<@{user_id}>"
            except Exception:
                return f"<@{user_id}>"

        await interaction.channel.send(
            f"🛑 trade between **{name_for(t['proposer_id'])}** and **{name_for(t['receiver_id'])}** was cancelled"
        )
        await interaction.response.send_message("🛑 Trade cancelled.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(Trade(bot))
