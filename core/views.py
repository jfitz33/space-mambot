from __future__ import annotations
from collections import Counter, defaultdict
from functools import partial

import discord, asyncio
from discord.ui import View, Select, button, Button
from core.packs import RARITY_ORDER, open_pack_from_csv, open_box_from_csv, normalize_rarity
from core.db import (db_add_cards, db_wallet_add, db_wallet_try_spend_mambucks,
                     db_collection_remove_exact_print, _blank_to_none,
                     db_collection_debug_dump, db_shards_add, db_fragment_yield_for_card)
from core.cards_shop import find_card_by_print_key, get_card_rarity, card_label, resolve_card_set
from core.pricing import craft_cost_for_card
from core.images import compose_pack_strip_image, rarity_badge
from core.constants import (
    PACK_COST,
    PACKS_IN_BOX,
    BOX_COST,
    BUNDLES,
    set_id_for_pack,
)
from typing import List, Tuple, Optional, Literal, Any

ORDER = {r: i for i, r in enumerate(RARITY_ORDER)}
BUNDLE_VALUE_PREFIX = "__bundle__"


def _coerce_set_id(value) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if stripped.isdigit():
            return int(stripped)
        lowered = stripped.lower()
        if lowered.startswith("set "):
            remainder = lowered[4:]
            if remainder.isdigit():
                return int(remainder)
        if lowered.startswith("set"):
            remainder = lowered[3:]
            if remainder.isdigit():
                return int(remainder)
        # Fall back to named lookup
        sid = set_id_for_pack(stripped)
        if sid is not None:
            return sid
    return None


def _resolve_pack_set_id(meta: dict, key: str) -> int | None:
    for candidate in (meta.get("set_id"), meta.get("set"), meta.get("set_number")):
        sid = _coerce_set_id(candidate)
        if sid is not None:
            return sid
    for name in (meta.get("set_name"), meta.get("display_name"), key):
        sid = set_id_for_pack(name)
        if sid is not None:
            return sid
    return None

def _rank(r: str) -> int:
    try: return RARITY_ORDER.index((r or "").lower())
    except: return 999

def format_pack_lines(pulls: list[dict]) -> list[str]:
    counts = Counter((c["name"], c["rarity"]) for c in pulls)
    return [f"x{qty} — **{name}** *(rarity: {rarity})*"
            for (name, rarity), qty in sorted(counts.items(), key=lambda kv: (_rank(kv[0][1]), kv[0][0].lower()))]

def _chunk(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def format_collection_lines(rows):
    """
    rows: (name, qty, rarity, set, code, id)
    Display format: **Name** — xQTY *(rarity: <rarity>, set:<set>)*
    """
    def _rank(r: str) -> int:
        from core.packs import RARITY_ORDER
        try: return RARITY_ORDER.index((r or "").lower())
        except: return 999

    sorted_rows = sorted(
        rows,
        key=lambda r: (_rank(r[2]), r[0].lower(), (r[3] or "").lower())  # rarity → name → set
    )

    lines = []
    for (name, qty, rarity, cset, _code, _cid) in sorted_rows:
        pack_tag = f"set:{cset}" if cset else ""
        # no code/id shown
        tail_bits = [f"rarity: {rarity}"]
        if pack_tag:
            tail_bits.append(pack_tag)
        tail = " *(" + ", ".join(tail_bits) + ")*"
        lines.append(f"**{name}** — x{qty}{tail}")
    return lines

def _build_pack_options(
    state,
    *,
    include_bundle: bool = False,
    bundle_registry: dict[str, dict] | None = None,
) -> List[discord.SelectOption]:
    packs_index = state.packs_index or {}
    if not packs_index:
        return [discord.SelectOption(label="No packs available", value="__none__")]

    if bundle_registry is not None:
        bundle_registry.clear()

    grouped: dict[int | None, list[dict[str, Any]]] = defaultdict(list)
    for key, meta in packs_index.items():
        label = (meta.get("display_name") or key).strip()[:100] or key[:100]
        desc = (meta.get("desc") or meta.get("description") or "")[:100] or None
        option = discord.SelectOption(label=label, value=key, description=desc)
        set_id = _resolve_pack_set_id(meta, key)
        grouped[set_id].append({"key": key, "label": label, "option": option})

    for entries in grouped.values():
        entries.sort(key=lambda item: item["label"].casefold())

    considered_set_ids = set(grouped.keys())
    if include_bundle:
        considered_set_ids.update(bundle["set_id"] for bundle in BUNDLES)

    ordered_set_ids = sorted(
        considered_set_ids,
        key=lambda sid: sid if sid is not None else 999,
    )

    options: List[discord.SelectOption] = []
    LIMIT = 25
    for set_id in ordered_set_ids:
        pack_entries = grouped.get(set_id, [])
        for entry in pack_entries:
            if len(options) >= LIMIT:
                break
            options.append(entry["option"])
        if len(options) >= LIMIT:
            break

        if include_bundle:
            for bundle in BUNDLES:
                if bundle["set_id"] != set_id:
                    continue
                if not pack_entries:
                    continue
                if len(options) >= LIMIT:
                    break

                bundle_value = f"{BUNDLE_VALUE_PREFIX}{bundle['id']}"
                pack_labels = [entry["label"] for entry in pack_entries]
                pack_pairs = [(entry["key"], entry["label"]) for entry in pack_entries]
                description = f"Boxes of {', '.join(pack_labels)}"
                if len(description) > 100:
                    description = f"{len(pack_labels)} boxes from Set {set_id}"

                option = discord.SelectOption(
                    label=bundle["name"],
                    value=bundle_value,
                    description=description[:100] or None,
                )
                options.append(option)

                if bundle_registry is not None:
                    bundle_registry[bundle_value] = {
                        "name": bundle["name"],
                        "cost": bundle["cost"],
                        "set_id": set_id,
                        "packs": pack_pairs,
                    }
        if len(options) >= LIMIT:
            break

    if not options:
        return [discord.SelectOption(label="No packs available", value="__none__")]
    return options

def _norm_rarity(r: str) -> str:
    r = (r or "").strip().lower()
    aliases = {
        "sr": "super", "super rare": "super",
        "ur": "ultra", "ultra rare": "ultra",
        "secr": "secret", "secret rare": "secret",
    }
    return aliases.get(r, r)

def _pick_highest_rarity_card(cards: list[dict[str, Any]]) -> dict[str, Any] | None:
    best = None
    best_rank = 10_000
    for c in cards or []:
        raw = c.get("rarity") or c.get("cardrarity") or ""
        rr = _norm_rarity(raw)
        rank = ORDER.get(rr, 9_999)
        if rank < best_rank:
            best, best_rank = c, rank
    return best

def _pack_embed_for_cards(
    emoji_ctx,
    pack_name: str,
    cards: list[dict],
    idx: int,
    total: int,
) -> tuple[list[discord.Embed], list[discord.File]]:
    """Return message content, embeds, and files for a pack pull.

    The embed lists the cards with rarity badges and attaches a composite image
    of the pack's card art beneath the text when possible.
    """
    title = f"{pack_name} — Pack {idx}/{total}" if total > 1 else f"{pack_name} — Pack"
    summary_embed = discord.Embed(title=title, color=0x2b6cb0)

    lines: list[str] = []
    files: list[discord.File] = []

    for card in cards or []:
        name = (card.get("name") or card.get("cardname") or "Unknown").strip() or "Unknown"
        rarity = card.get("rarity") or card.get("cardrarity") or ""
        badge = rarity_badge(emoji_ctx, rarity)
        line = f"{badge} {name}".strip()
        lines.append(line)
    
    summary_embed.description = "\n".join(lines) if lines else "_No cards pulled._"

    embeds = [summary_embed]

    strip_file, missing_art = compose_pack_strip_image(
        pack_name,
        cards,
        pack_index=idx if total > 1 else None,
    )
    if strip_file:
        files.append(strip_file)
        summary_embed.set_image(url=f"attachment://{strip_file.filename}")

    if missing_art:
        summary_embed.set_footer(text="Some cards lacked art and will not appear below.")
    
    return None, embeds, files

class PacksDropdown(discord.ui.Select):
    def __init__(self, parent_view: "PacksSelectView"):
        self.parent_view = parent_view
        options = _build_pack_options(
            parent_view.state,
            include_bundle=parent_view.include_bundle,
            bundle_registry=parent_view.bundle_lookup,
        )
        super().__init__(
            placeholder="Choose a pack…",
            min_values=1,
            max_values=1,
            options=options
        )

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.parent_view.requester.id:
            return await interaction.response.send_message("This menu isn't for you.", ephemeral=True)
        value = self.values[0]
        if value == "__none__":
            return await interaction.response.send_message("No packs are configured.", ephemeral=True)
        # Hand off to the view to branch pack vs box
        await self.parent_view._handle_pack_choice(interaction, value)

class ConfirmSpendView(discord.ui.View):
    def __init__(
        self,
        state,
        requester,
        pack_name,
        amount,
        on_confirm,
        total_cost: int | None = None,
        *,
        timeout: float = 90,
        display_description: str | None = None,
    ):
        super().__init__(timeout=timeout)
        self.state = state
        self.requester = requester
        self.pack_name = pack_name
        self.amount = amount
        self.on_confirm = on_confirm
        self.total_cost = total_cost
        self.display_description = display_description
        self._processing = False

    async def remove_ui(self, interaction: discord.Interaction, content: str | None = None):
        self.stop()
        for item in self.children:
            item.disabled = True
        try:
            await interaction.response.edit_message(content=content, view=None)
        except discord.InteractionResponded:
            await interaction.message.edit(content=content, view=None)
        except Exception:
            pass

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.success)
    async def yes(self, interaction: discord.Interaction, button: discord.ui.Button):
        from core.db import db_wallet_try_spend_mambucks, db_wallet_add
        if interaction.user.id != self.requester.id:
            return await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)

        if self._processing:
            try:
                await interaction.response.defer_update()
            except Exception:
                pass
            return
        self._processing = True

        await self.remove_ui(interaction, content="Processing…")
        try:
            await interaction.followup.defer(ephemeral=True)
        except Exception:
            pass

        total_cost = int(self.total_cost or 0)
        if total_cost <= 0:
            # fallback if you still use PACK_COST * amount pattern elsewhere
            from core.constants import PACK_COST
            total_cost = self.amount * PACK_COST

        after_spend = db_wallet_try_spend_mambucks(self.state, self.requester.id, total_cost)
        if after_spend is None:
            self._processing = False
            desc = self.display_description or f"**{self.amount}** pack(s) of **{self.pack_name}**"
            return await interaction.followup.send(
                f"❌ Not enough **Mambucks** to open {desc}.\n"
                f"Cost: **{total_cost}**.",
                ephemeral=True
            )

        try:
            await self.on_confirm(interaction, self.state, self.requester, self.pack_name, self.amount)
            # (channel could be None for DMs)
            if interaction.channel:
                await interaction.channel.send(
                    f"💰 Remaining balance → **{after_spend['mambucks']}** Mambucks."
                )
        except Exception:
            # refund Mambucks on failure
            db_wallet_add(self.state, self.requester.id, d_mambucks=total_cost)
            await interaction.followup.send("⚠️ Something went wrong opening packs. You were not charged.", ephemeral=True)
            raise
        finally:
            self._processing = False

    @discord.ui.button(label="No", style=discord.ButtonStyle.danger)
    async def no(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.requester.id:
            return await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)
        await self.remove_ui(interaction, content="Cancelled.")


class ConfirmBuyCardView(discord.ui.View):
    def __init__(self, state, requester: discord.Member, print_key: str, amount: int, total_cost: int, *, timeout: float = 90):
        super().__init__(timeout=timeout)
        self.state = state
        self.requester = requester
        self.print_key = print_key
        self.amount = amount
        self.total_cost = total_cost  # recomputed via shard cost
        self._processing = False

    async def remove_ui(self, interaction: discord.Interaction, content: str | None = None):
        self.stop()
        for item in self.children:
            item.disabled = True
        try:
            await interaction.response.edit_message(content=content, view=None)
        except discord.InteractionResponded:
            await interaction.message.edit(content=content, view=None)

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.success)
    async def yes(self, interaction: discord.Interaction, _: discord.ui.Button):
        from core.cards_shop import (
            find_card_by_print_key,
            card_label,
            get_card_rarity,
            resolve_card_set,
            is_starter_card,
            is_starter_set,
        )
        from core.db import db_add_cards, db_shards_get, db_shards_add
        from core.constants import CRAFT_COST_BY_RARITY, set_id_for_pack
        from core.currency import shard_set_name

        if interaction.user.id != self.requester.id:
            return await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)
        if self._processing:
            try:
                await interaction.response.defer_update()
            except Exception:
                pass
            return
        self._processing = True

        await self.remove_ui(interaction, content="Processing craft…")
        try:
            await interaction.followup.defer(ephemeral=True)
        except Exception:
            pass

        card = find_card_by_print_key(self.state, self.print_key)
        if not card:
            self._processing = False
            return await interaction.followup.send("⚠️ Card printing not found.", ephemeral=True)
        
        if is_starter_card(card):
            self._processing = False
            return await interaction.followup.send("❌ Starter deck cards cannot be crafted.", ephemeral=True)

        set_name = resolve_card_set(self.state, card)
        if not set_name:
            self._processing = False
            return await interaction.followup.send(
                "⚠️ This printing is missing a card set in the data, so it can’t be crafted.",
                ephemeral=True
            )
        
        if is_starter_set(set_name):
            self._processing = False
            return await interaction.followup.send("❌ Starter deck cards cannot be crafted.", ephemeral=True)

        cost_each, sale_row = craft_cost_for_card(self.state, card, set_name)
        if cost_each <= 0:
            self._processing = False
            return await interaction.followup.send("❌ This printing cannot be crafted.", ephemeral=True)

        total_cost = cost_each * self.amount

        set_id = set_id_for_pack(set_name) or 1  # default Set 1
        have = db_shards_get(self.state, self.requester.id, set_id)
        if have < total_cost:
            self._processing = False
            pretty = shard_set_name(set_id)
            return await interaction.followup.send(
                f"❌ Not enough {pretty}. Need **{total_cost}**, you have **{have}**.",
                ephemeral=True
            )

        # debit shards (non-atomic by design, you chose this path)
        db_shards_add(self.state, self.requester.id, set_id, -total_cost)

        try:
            db_add_cards(self.state, self.requester.id, [card] * self.amount, set_name)
            after = db_shards_get(self.state, self.requester.id, set_id)
            pretty = shard_set_name(set_id)
            sale_note = ""
            if sale_row:
                sale_note = f" *(on sale −{int(sale_row.get('discount_pct', 0))}%)*"

            await interaction.followup.send(
                f"✅ Crafted **{self.amount}× {card_label(card)}** for **{total_cost}** {pretty}{sale_note}.\n"
                f"**Remaining {pretty}:** {after}",
                ephemeral=True
            )
            if interaction.channel:
                await interaction.channel.send(
                    f"{self.requester.mention} crafted {self.amount} {card.get('name') or 'card'} "
                    f"for {total_cost} {pretty}"
                )
        except Exception:
            # refund shards on failure
            db_shards_add(self.state, self.requester.id, set_id, total_cost)
            await interaction.followup.send("⚠️ Craft failed. You were not charged.", ephemeral=True)
            raise
        finally:
            self._processing = False

    @discord.ui.button(label="No", style=discord.ButtonStyle.danger)
    async def no(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.requester.id:
            return await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)
        await self.remove_ui(interaction, content="Craft cancelled.")


class ConfirmSellCardView(discord.ui.View):
    def __init__(self, state, requester: discord.Member, print_key: str, amount: int, total_credit: int, *, timeout: float = 90):
        super().__init__(timeout=timeout)
        self.state = state
        self.requester = requester
        self.print_key = print_key
        self.amount = amount
        self.total_credit = total_credit
        self._processing = False

    async def remove_ui(self, interaction: discord.Interaction, content: Optional[str] = None):
        self.stop()
        for item in self.children:
            item.disabled = True
        try:
            await interaction.response.edit_message(content=content, view=None)
        except discord.InteractionResponded:
            await interaction.message.edit(content=content, view=None)

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.success)
    async def yes(self, interaction: discord.Interaction, _: discord.ui.Button):
        from core.cards_shop import (
            find_card_by_print_key,
            card_label,
            get_card_rarity,
            resolve_card_set,
            is_starter_card,
            is_starter_set,
        )
        from core.db import db_collection_remove_exact_print, db_shards_add, db_shards_get
        from core.constants import SHARD_YIELD_BY_RARITY, set_id_for_pack
        from core.currency import shard_set_name

        if interaction.user.id != self.requester.id:
            return await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)
        if self._processing:
            try:
                await interaction.response.defer_update()
            except Exception:
                pass
            return
        self._processing = True

        await self.remove_ui(interaction, content="Processing sharding…")
        try:
            await interaction.followup.defer(ephemeral=True)
        except Exception:
            pass

        card = find_card_by_print_key(self.state, self.print_key)
        if not card:
            self._processing = False
            return await interaction.followup.send("⚠️ Card printing not found.", ephemeral=True)
        
        if is_starter_card(card):
            self._processing = False
            return await interaction.followup.send("❌ Starter deck cards cannot be fragmented.", ephemeral=True)

        set_name = resolve_card_set(self.state, card)
        if not set_name:
            self._processing = False
            return await interaction.followup.send(
                "⚠️ This printing is missing a card set in the data, so it can’t be fragmented.",
                ephemeral=True
            )
        
        if is_starter_set(set_name):
            self._processing = False
            return await interaction.followup.send("❌ Starter deck cards cannot be fragmented.", ephemeral=True)

        rarity = (get_card_rarity(card) or "").lower()
        yield_each, ov = db_fragment_yield_for_card(self.state, card, set_name)
        if yield_each is None:
            self._processing = False
            return await interaction.followup.send("❌ This printing cannot be fragmented.", ephemeral=True)

        # remove exact print from collection
        removed = db_collection_remove_exact_print(
            self.state,
            self.requester.id,
            card_name=(card.get("name") or card.get("cardname") or ""),
            card_rarity=(card.get("rarity") or card.get("cardrarity") or ""),
            card_set=set_name,
            card_code=(card.get("code") or card.get("cardcode")),
            card_id=(card.get("id") or card.get("cardid")),
            amount=int(self.amount),
        )
        if removed <= 0:
            self._processing = False
            return await interaction.followup.send("❌ You don’t have the specified copies to shard.", ephemeral=True)

        set_id = set_id_for_pack(set_name) or 1
        credit = removed * yield_each
        db_shards_add(self.state, self.requester.id, set_id, credit)
        after = db_shards_get(self.state, self.requester.id, set_id)
        pretty = shard_set_name(set_id)

        await interaction.followup.send(
            f"🔨 Fragmented **{removed}× {card_label(card)}** into **{credit}** {pretty}.\n"
            f"**Total {pretty}:** {after}",
            ephemeral=True
        )
        self._processing = False

    @discord.ui.button(label="No", style=discord.ButtonStyle.danger)
    async def no(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.requester.id:
            return await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)
        await self.remove_ui(interaction, content="Sale cancelled.")

class ConfirmP2PInitiatorView(discord.ui.View):
    """
    First step: shown only to the initiator. If they confirm, we post a channel message
    pinging the counterparty with a second confirm view.
    """
    def __init__(
        self,
        state,
        *,
        requester: discord.Member,
        counterparty: discord.Member,
        mode: Literal["buy","sell"],   # 'buy' => requester buys; 'sell' => requester sells
        print_key: str,
        copies: int,
        price_mb: int,
        timeout: float = 120
    ):
        super().__init__(timeout=timeout)
        self.state = state
        self.requester = requester
        self.counterparty = counterparty
        self.mode = mode
        self.print_key = print_key
        self.copies = int(copies)
        self.price_mb = int(price_mb)
        self._processing = False

    async def _remove_ui(self, interaction: discord.Interaction, content: Optional[str] = None):
        self.stop()
        for item in self.children: item.disabled = True
        try:
            await interaction.response.edit_message(content=content, view=None)
        except discord.InteractionResponded:
            await interaction.message.edit(content=content, view=None)

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.success)
    async def yes(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.requester.id:
            return await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)
        if self._processing:
            try: await interaction.response.defer_update()
            except: pass
            return
        self._processing = True

        # Resolve card for display; if missing, abort
        card = find_card_by_print_key(self.state, self.print_key)
        if not card:
            await self._remove_ui(interaction, content="⚠️ Card printing not found.")
            return

        # Remove this prompt and tell initiator we sent the offer
        await self._remove_ui(interaction, content="Offer sent for counterparty confirmation…")
        try:
            await interaction.followup.send(
                f"📨 Sent your offer to {self.counterparty.mention}.",
                ephemeral=True
            )
        except: pass

        # Post the counterparty confirmation publicly (so they can click)
        verb = "buy" if self.mode == "buy" else "sell"
        direction = ("from you" if self.mode == "buy" else "to you")
        offer_text = (
            f"{self.counterparty.mention} — {self.requester.mention} wants to **{verb} "
            f"{self.copies}× {card_label(card)}** {direction} for **{self.price_mb}** mambucks.\n"
            f"Do you accept?"
        )
        counter_view = ConfirmP2PCounterpartyView(
            self.state,
            requester=self.requester,
            counterparty=self.counterparty,
            mode=self.mode,
            print_key=self.print_key,
            copies=self.copies,
            price_mb=self.price_mb
        )
        msg = await interaction.channel.send(offer_text, view=counter_view)  # <-- send
        counter_view.message = msg

        # Monitoring message for timeout
        try:
            self.state.live_views.add(counter_view)
        except Exception:
            pass   

    @discord.ui.button(label="No", style=discord.ButtonStyle.danger)
    async def no(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.requester.id:
            return await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)
        await self._remove_ui(interaction, content="Offer cancelled.")

class ConfirmP2PCounterpartyView(discord.ui.View):
    """
    Second step: shown to the counterparty. On Accept, we perform the transfer:
    - Spend mambucks from the buyer
    - Move cards from seller to buyer
    - Credit mambucks to the seller
    """
    def __init__(
        self,
        state,
        *,
        requester: discord.Member,
        counterparty: discord.Member,
        mode: Literal["buy","sell"],
        print_key: str,
        copies: int,
        price_mb: int,
        timeout: float = 90
    ):
        super().__init__(timeout=timeout)
        self.state = state
        self.requester = requester
        self.counterparty = counterparty
        self.mode = mode
        self.print_key = print_key
        self.copies = int(copies)
        self.price_mb = int(price_mb)
        self._processing = False
        self._completed = False
        self.message: discord.Message | None = None 

    def _roles(self):
        # Returns (buyer, seller) based on mode
        if self.mode == "buy":
            return (self.requester, self.counterparty)  # requester buys FROM counterparty
        else:
            return (self.counterparty, self.requester)  # requester sells TO counterparty

    async def _remove_ui(self, interaction: discord.Interaction, content: Optional[str] = None):
        self.stop()
        for item in self.children: item.disabled = True
        try:
            await interaction.response.edit_message(content=content, view=None)
        except discord.InteractionResponded:
            await interaction.message.edit(content=content, view=None)


    async def on_timeout(self):
        """If no one clicked within timeout, replace buttons with a cancel notice."""
        if self._processing or self._completed:
            return
        try:
            if self.message:
                await self.message.edit(
                    content="Sale cancelled, user did not respond in time",
                    view=None
                )
            else:
                # Fallback: best effort disable if we somehow lack the message reference
                for item in self.children:
                    item.disabled = True
        except Exception:
            pass
        finally:
            # Attempt to drop strong ref
            try:
                self.state.live_views.discard(self)
            except Exception:
                pass
            self.stop()
    
    @discord.ui.button(label="Accept", style=discord.ButtonStyle.success)
    async def accept(self, interaction: discord.Interaction, _: discord.ui.Button):
        # Only the counterparty can accept/decline
        if interaction.user.id != self.counterparty.id:
            return await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)
        if self._processing:
            try: await interaction.response.defer_update()
            except: pass
            return
        self._processing = True

        await self._remove_ui(interaction, content="Processing trade…")
        try:
            await interaction.followup.defer(ephemeral=True)
        except: pass

        buyer, seller = self._roles()
        card = find_card_by_print_key(self.state, self.print_key)
        if not card:
            return await interaction.followup.send("⚠️ Card printing not found.", ephemeral=True)

        set_name = (card.get("set") or card.get("cardset") or "").strip()
        if not set_name:
            return await interaction.followup.send("⚠️ Printing has no set; cannot trade.", ephemeral=True)

        # 1) Take payment from buyer (escrow). If insufficient, abort.
        spent = db_wallet_try_spend_mambucks(self.state, buyer.id, self.price_mb)
        if spent is None:
            return await interaction.followup.send(
                f"❌ {buyer.mention} doesn’t have **{self.price_mb}** mambucks.", ephemeral=True
            )

        # 2) Remove cards from seller; if seller lacks copies, refund buyer and abort.
        removed = 0
        sig_code   = _blank_to_none(card.get("code") or card.get("cardcode"))
        sig_id     = _blank_to_none(card.get("id") or card.get("cardid"))
        sig_name = _blank_to_none(card.get("name") or card.get("cardname") or "")
        sig_rarity = _blank_to_none(card.get("rarity") or card.get("cardrarity") or "")
        rows = db_collection_debug_dump(self.state, seller.id, sig_name, sig_rarity, set_name)
        print("DEBUG owned rows:", rows)
        try:
            removed = db_collection_remove_exact_print(
                self.state,
                seller.id,
                card_name=sig_name,
                card_rarity=sig_rarity,
                card_set=set_name,
                card_code=sig_code,
                card_id=sig_id,
                amount=self.copies
            )
            if removed < self.copies:
                # Restore any partial removal (rare, but safe)
                if removed > 0:
                    db_add_cards(self.state, seller.id, [card] * removed, set_name)
                # Refund buyer
                db_wallet_add(self.state, buyer.id, d_mambucks=self.price_mb)
                return await interaction.followup.send(
                    f"❌ Did not find **{self.copies}× {card_label(card)}** in {seller.mention}'s collection.", ephemeral=True
                )

            # 3) Grant cards to buyer
            db_add_cards(self.state, buyer.id, [card] * self.copies, set_name)

            # 4) Credit seller with the mambucks
            db_wallet_add(self.state, seller.id, d_mambucks=self.price_mb)

            # Success messages
            await interaction.followup.send("✅ Trade completed.", ephemeral=True)

            label = card_label(card)  # or use sig_name if you prefer plain name
            if self.mode == "buy":
                # buyer initiated a purchase from the counterparty
                summary = (
                    f"{buyer.mention} bought {self.copies}× {label} "
                    f"from {seller.mention} for {self.price_mb} mambucks"
                )
            else:  # self.mode == "sell"
                # requester sold to the counterparty
                summary = (
                    f"{seller.mention} sold {self.copies}× {label} "
                    f"to {buyer.mention} for {self.price_mb} mambucks"
                )
            await interaction.channel.send(summary)
        except Exception:
            # On any exception, try to refund buyer
            try: db_wallet_add(self.state, buyer.id, d_mambucks=self.price_mb)
            except: pass
            await interaction.followup.send("⚠️ Trade failed; any funds were refunded.", ephemeral=True)
            raise
        finally:
            self._processing = False
            self._completed = True

    @discord.ui.button(label="Decline", style=discord.ButtonStyle.danger)
    async def decline(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.counterparty.id:
            self._completed = True
            return await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)
        await self._remove_ui(interaction, content="Trade declined.")

class PackResultsPaginator(View):
    def __init__(self, requester: discord.User, pack_name: str, per_pack_pulls: list[list[dict]], timeout: float = 120):
        super().__init__(timeout=timeout)
        self.requester = requester
        self.pack_name = pack_name
        self.per_pack_pulls = per_pack_pulls
        self.total = len(per_pack_pulls)
        self.index = 0

    def _embed_for_index(self) -> discord.Embed:
        lines = format_pack_lines(self.per_pack_pulls[self.index])
        body = "\n".join(lines) or "_No cards._"
        footer = f"\n\nPack {self.index+1}/{self.total}"
        return discord.Embed(title=f"{self.requester.display_name} opened {self.total} pack(s) of `{self.pack_name}`",
                             description=body+footer, color=0x2b6cb0)

    async def on_timeout(self):
        for child in self.children: child.disabled = True

    @button(label="◀️ Prev", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction: discord.Interaction, _: Button):
        if interaction.user.id != self.requester.id:
            await interaction.response.send_message("Only the requester can use these controls.", ephemeral=True); return
        self.index = (self.index - 1) % self.total
        await interaction.response.edit_message(embed=self._embed_for_index(), view=self)

    @button(label="Next ▶️", style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction: discord.Interaction, _: Button):
        if interaction.user.id != self.requester.id:
            await interaction.response.send_message("Only the requester can use these controls.", ephemeral=True); return
        self.index = (self.index + 1) % self.total
        await interaction.response.edit_message(embed=self._embed_for_index(), view=self)

class PacksSelectView(discord.ui.View):
    def __init__(self, state, requester: discord.Member, amount: int, mode: Literal["pack","box"]="pack", *, timeout: float=90):
        super().__init__(timeout=timeout)
        self.state = state
        self.requester = requester
        self.amount = amount
        self.mode = mode
        self.include_bundle = mode == "box"
        self.bundle_lookup: dict[str, dict] = {}
        self.add_item(PacksDropdown(self))

    async def _handle_pack_choice(self, interaction: discord.Interaction, pack_name: str):
        bundle_info = self.bundle_lookup.get(pack_name)
        if bundle_info:
            bundle_name = bundle_info.get("name", "Bundle")
            if not self.include_bundle:
                await interaction.response.send_message(
                    f"The {bundle_name} is only available when opening boxes.",
                    ephemeral=True,
                )
                return

            pack_entries = bundle_info.get("packs") or []
            if not pack_entries:
                await interaction.response.send_message(
                    f"No packs are available for the {bundle_name}.",
                    ephemeral=True,
                )
                return

            pack_labels = [label for _key, label in pack_entries]
            preview_names = ", ".join(pack_labels[:10])
            if len(pack_labels) > 10:
                preview_names += ", …"

            confirm_view = ConfirmSpendView(
                state=self.state,
                requester=self.requester,
                pack_name=bundle_name,
                amount=1,
                on_confirm=partial(self._open_bundle, bundle_info=bundle_info),
                total_cost=bundle_info.get("cost", 0),
                display_description=f"**the {bundle_name}**",
            )

            set_id = bundle_info.get("set_id")
            set_text = f"Set {set_id}" if set_id is not None else "this bundle"
            message = (
                f"Open the **{bundle_name}** for **{bundle_info.get('cost', 0)}** Mambucks?\n"
                f"This grants one box ({PACKS_IN_BOX} packs) of each available pack in {set_text} ({len(pack_entries)} total)."
            )
            if preview_names:
                message += f"\nIncludes: {preview_names}"

            await interaction.response.edit_message(content=message, view=confirm_view)
            return

        if self.mode == "box":
            confirm_view = ConfirmSpendView(
                state=self.state,
                requester=self.requester,
                pack_name=pack_name,
                amount=PACKS_IN_BOX,
                on_confirm=self._open_box_and_render,
                total_cost=BOX_COST,
            )
            await interaction.response.edit_message(
                content=(
                    f"Open a **box** of **{pack_name}** for **{BOX_COST}** Mambucks?\n"
                    f"That’s **{PACKS_IN_BOX}** packs with guarantees:\n"
                    f"• Packs 1–18: Super Rare top\n"
                    f"• Packs 19–23: Ultra Rare top\n"
                    f"• Pack 24: Secret Rare top"
                ),
                view=confirm_view
            )
        else:
            total_cost = self.amount * PACK_COST
            confirm_view = ConfirmSpendView(
                state=self.state,
                requester=self.requester,
                pack_name=pack_name,
                amount=self.amount,
                on_confirm=self._open_and_render
            )
            await interaction.response.edit_message(
                content=(f"Are you sure you want to spend **{total_cost}** Mambucks on "
                         f"**{self.amount}** pack(s) of **{pack_name}**?"),
                view=confirm_view
            )

    async def _open_and_render(self, interaction: discord.Interaction, state, requester, pack_name: str, amount: int):
        # ConfirmSpendView has already charged Mambucks and will refund on exceptions.
        per_pack: list[list[dict]] = []
        for _ in range(amount):
            per_pack.append(open_pack_from_csv(state, pack_name, 1))

        # Persist cards
        flat = [c for pack in per_pack for c in pack]
        db_add_cards(state, requester.id, flat, pack_name)

        # Try to DM results, one embed per pack
        dm_sent = False
        try:
            dm = await requester.create_dm()
            for i, cards in enumerate(per_pack, start=1):
                content, embeds, files = _pack_embed_for_cards(interaction.client, pack_name, cards, i, amount)
                send_kwargs: dict = {"embeds": embeds}
                if content:
                    send_kwargs["content"] = content
                if files:
                    send_kwargs["files"] = files
                await dm.send(**send_kwargs)
                # be polite to rate limits if many packs
                if amount > 5:
                    await asyncio.sleep(0.2)
            dm_sent = True
        except Exception:
            dm_sent = False

        # Remove/clear the confirm message if it still exists
        try:
            await interaction.edit_original_response(content=None, view=None)
        except Exception:
            pass

        # Public summary in the channel
        summary = (
            f"{requester.mention} opened **{amount}** pack{'s' if amount != 1 else ''} of **{pack_name}**."
            f"{' Results sent via DM.' if dm_sent else ' I could not DM you; posting results here.'}"
        )

        # Update packs opened counter for quests
        quests_cog = interaction.client.get_cog("Quests")
        if quests_cog:
            await quests_cog.tick_pack_open(user_id=interaction.user.id, amount=amount)

        if dm_sent:
            # Just a tidy, public summary
            await interaction.channel.send(summary)
        else:
            # Fallback: post results in the channel (one embed per pack)
            await interaction.channel.send(summary)
            for i, cards in enumerate(per_pack, start=1):
                content, embeds, files = _pack_embed_for_cards(interaction.client, pack_name, cards, i, amount)
                send_kwargs: dict = {"embeds": embeds}
                if content:
                    send_kwargs["content"] = content
                if files:
                    send_kwargs["files"] = files
                await dm.send(**send_kwargs)
                if amount > 5:
                    await asyncio.sleep(0.2)

    async def _open_box_and_render(
        self,
        interaction: discord.Interaction,
        state,
        requester,
        pack_name: str,
        amount: int | None = None,   # not used; ConfirmSpendView passes PACKS_IN_BOX in .amount
    ):
        # ConfirmSpendView has already charged Mambucks and will refund on exceptions.
        per_pack = open_box_from_csv(state, pack_name)

        # persist the cards
        flat = [c for pack in per_pack for c in pack]
        db_add_cards(state, requester.id, flat, pack_name)

        # DM one message per pack (same as packs flow)
        dm_sent = False
        try:
            dm = await requester.create_dm()
            for i, cards in enumerate(per_pack, start=1):
                content, embeds, files = _pack_embed_for_cards(interaction.client, pack_name, cards, i, amount)
                send_kwargs: dict = {"embeds": embeds}
                if content:
                    send_kwargs["content"] = content
                if files:
                    send_kwargs["files"] = files
                await dm.send(**send_kwargs)
                await asyncio.sleep(0.25)  # gentle on rate limits
            dm_sent = True
        except Exception:
            dm_sent = False

        # remove the confirm message if it still exists
        try:
            await interaction.edit_original_response(content=None, view=None)
        except Exception:
            pass

        # Update packs opened counter for quests (box = 24)
        quests_cog = interaction.client.get_cog("Quests")
        if quests_cog:
            await quests_cog.tick_pack_open(user_id=interaction.user.id, amount=PACKS_IN_BOX)

        # public summary
        summary = (
            f"{requester.mention} opened a **box** (24 packs) of **{pack_name}**."
            f"{' Results sent via DM.' if dm_sent else ' I could not DM you; posting results here.'}"
        )
        await interaction.channel.send(summary)

        # fallback to channel if DMs closed
        if not dm_sent:
            for i, cards in enumerate(per_pack, start=1):
                content, embeds, files = _pack_embed_for_cards(interaction.client, pack_name, cards, i, amount)
                send_kwargs: dict = {"embeds": embeds}
                if content:
                    send_kwargs["content"] = content
                if files:
                    send_kwargs["files"] = files
                await dm.send(**send_kwargs)
                await asyncio.sleep(0.25)

    async def _open_bundle(
        self,
        interaction: discord.Interaction,
        state,
        requester,
        pack_name: str,
        amount: int | None = None,
        *,
        bundle_info: dict,
    ):
        pack_entries: list[tuple[str, str]] = bundle_info.get("packs", [])
        bundle_name = bundle_info.get("name", pack_name)

        if not pack_entries:
            try:
                await interaction.edit_original_response(
                    content=f"No packs are configured for the {bundle_name}.",
                    view=None,
                )
            except Exception:
                pass
            await interaction.followup.send(
                f"No packs are available for the {bundle_name}.",
                ephemeral=True,
            )
            return

        per_pack_results: list[tuple[str, list[list[dict]], str]] = []
        total_packs_opened = 0

        for pack_key, display_name in pack_entries:
            per_pack = open_box_from_csv(state, pack_key)
            per_pack_results.append((display_name, per_pack, pack_key))
            total_packs_opened += len(per_pack)
            flat = [card for pack_cards in per_pack for card in pack_cards]
            db_add_cards(state, requester.id, flat, pack_key)

        dm_sent = False
        try:
            dm = await requester.create_dm()
            for display_name, per_pack, _pack_key in per_pack_results:
                for i, cards in enumerate(per_pack, start=1):
                    content, embeds, files = _pack_embed_for_cards(
                        interaction.client,
                        display_name,
                        cards,
                        i,
                        len(per_pack),
                    )
                    send_kwargs: dict = {"embeds": embeds}
                    if content:
                        send_kwargs["content"] = content
                    if files:
                        send_kwargs["files"] = files
                    await dm.send(**send_kwargs)
                    await asyncio.sleep(0.25)
            dm_sent = True
        except Exception:
            dm_sent = False

        try:
            await interaction.edit_original_response(content=None, view=None)
        except Exception:
            pass

        quests_cog = interaction.client.get_cog("Quests")
        if quests_cog:
            await quests_cog.tick_pack_open(
                user_id=interaction.user.id,
                amount=total_packs_opened,
            )

        pack_labels = [label for _key, label in pack_entries]
        pack_count = len(pack_labels)
        pack_word = "pack" if pack_count == 1 else "packs"
        summary = (
            f"{requester.mention} opened the **{bundle_name}** "
            f"(one box each of {pack_count} {pack_word})."
            f"{' Results sent via DM.' if dm_sent else ' I could not DM you; posting results here.'}"
        )
        if pack_labels:
            summary += f"\nIncludes: {', '.join(pack_labels)}"

        if interaction.channel:
            await interaction.channel.send(summary)

        if not dm_sent and interaction.channel:
            for display_name, per_pack, _pack_key in per_pack_results:
                for i, cards in enumerate(per_pack, start=1):
                    content, embeds, files = _pack_embed_for_cards(
                        interaction.client,
                        display_name,
                        cards,
                        i,
                        len(per_pack),
                    )
                    send_kwargs: dict = {"embeds": embeds}
                    if content:
                        send_kwargs["content"] = content
                    if files:
                        send_kwargs["files"] = files
                    await interaction.channel.send(**send_kwargs)
                    await asyncio.sleep(0.25)

class CollectionPaginator(View):
    def __init__(self, requester: discord.User, target: discord.User, rows: List[Tuple[str,int,str,str,str,str]], page_size: int = 20, timeout: float = 180):
        super().__init__(timeout=timeout)
        self.requester = requester
        self.target = target
        self.lines = format_collection_lines(rows)
        self.pages = _chunk(self.lines, page_size) or [[]]
        self.index = 0
        self.total_qty = sum(q for (_, q, *_rest) in rows)

    def _embed(self) -> discord.Embed:
        body = "\n".join(self.pages[self.index]) if self.pages[self.index] else "_No cards._"
        footer = f"\n\nPage {self.index+1}/{len(self.pages)} • Unique rows: {len(self.lines)} • Total qty: {self.total_qty}"
        return discord.Embed(
            title=f"{self.target.display_name}'s Collection",
            description=body + footer,
            color=0x2b6cb0
        )

    async def on_timeout(self):
        for child in self.children: child.disabled = True

    @button(label="◀️ Prev", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction: discord.Interaction, _: Button):
        if interaction.user.id != self.requester.id:
            await interaction.response.send_message("Only the requester can use these controls.", ephemeral=True); return
        self.index = (self.index - 1) % len(self.pages)
        await interaction.response.edit_message(embed=self._embed(), view=self)

    @button(label="Next ▶️", style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction: discord.Interaction, _: Button):
        if interaction.user.id != self.requester.id:
            await interaction.response.send_message("Only the requester can use these controls.", ephemeral=True); return
        self.index = (self.index + 1) % len(self.pages)
        await interaction.response.edit_message(embed=self._embed(), view=self)
