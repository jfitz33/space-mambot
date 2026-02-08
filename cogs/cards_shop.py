# cogs/cards_shop.py
import asyncio
import os, discord, textwrap
from typing import List, Dict, Optional
import requests
from discord.ext import commands
from discord import app_commands
from core.feature_flags import is_shop_gamba_enabled
from core.state import AppState
from core.cards_shop import (
    ensure_shop_index,
    find_card_by_print_key,
    card_label_with_badge,
    get_card_rarity,
    card_set_name,
    resolve_card_set,
    register_print_if_missing,
    is_starter_card,
    is_starter_set,
    canonicalize_rarity,
    YGOPRO_API_URL,
)
from core.tins import is_tin_promo_print
from core.constants import (
    CRAFT_COST_BY_RARITY, SHARD_YIELD_BY_RARITY, set_id_for_pack, 
    RARITY_ORDER, RARITY_ALIASES, FRAGMENTABLE_RARITIES
)
from core.views import (
    ConfirmBuyCardView,
    ConfirmSellCardView,
)
from core.currency import shard_set_name
from core.images import rarity_badge, card_art_url_for_card, card_art_path_for_card
from core.db import (
    db_collection_list_owned_prints, db_collection_list_for_bulk_fragment, 
    db_shards_add, db_collection_remove_exact_print, db_fragment_yield_for_card,
    db_shards_get
)
from core.pricing import craft_cost_for_card
from core.cards_shop import ensure_shop_index, card_label, _sig_for_resolution

GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None


def suggest_prints_with_set(
    state,
    query: str,
    limit: int = 25,
    *,
    include_starters: bool = False,
    include_tins: bool = False,
):
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
        if not include_starters and is_starter_set(set_):
            continue
        if not include_tins and is_tin_promo_print(state, card, set_name=set_):
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
        if not include_starters and is_starter_set(set_present):
            continue
        if not include_tins and is_tin_promo_print(state, card, set_name=set_present):
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

def suggest_owned_prints_relaxed(
    state, user_id: int, query: str, limit: int = 25, *, include_starters: bool = False
) -> List[app_commands.Choice[str]]:
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
        if not include_starters and is_starter_set(set_):
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
        if not include_starters and is_starter_card(card):
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

def _set_sort_key(set_name: str) -> tuple[int, str]:
    return (set_id_for_pack(set_name) or 9999, (set_name or "").lower())

def _sort_rows_by_set(rows: List[Dict]) -> List[Dict]:
    rarity_rank = {r: i for i, r in enumerate(RARITY_ORDER)}

    return sorted(
        rows,
        key=lambda row: (
            _set_sort_key(row.get("set") or ""),
            rarity_rank.get(row.get("rarity"), len(rarity_rank)),
            (row.get("name") or "").lower(),
        ),
    )

class BulkFragmentConfirmView(discord.ui.View):
    def __init__(self, state: AppState, user: discord.Member, plan_rows: List[Dict], keep: int, total_yield_by_set: Dict[str, int], *, timeout: float = 120):
        # Setting the timeout explicitly keeps discord.py happy when the View is
        # inspected before it is sent (e.g., during followup.send(ephemeral=True)).
        super().__init__(timeout=timeout)
        self.state = state
        self.user = user
        self.plan_rows = plan_rows
        self.keep = int(keep)
        self.total_yield_by_set = {k: int(v) for k, v in (total_yield_by_set or {}).items()}
        self._locked = False

    def _is_requester(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id
    
    async def _show_processing_state(self, interaction: discord.Interaction):
        for child in self.children:
            child.disabled = True
            if isinstance(child, discord.ui.Button) and child.label == "Confirm":
                child.label = "Processing…"

        try:
            if interaction.response.is_done():
                await interaction.followup.edit_message(
                    message_id=self.message.id if self.message else interaction.message.id,
                    view=self,
                )
            else:
                await interaction.response.edit_message(view=self)
        except discord.InteractionResponded:
            await interaction.message.edit(view=self)
        except discord.NotFound:
            await interaction.message.edit(view=self)

    async def _ensure_deferred(self, interaction: discord.Interaction):
        if interaction.response.is_done():
            return
        try:
            await interaction.response.defer(thinking=False)
        except discord.InteractionResponded:
            pass

    def shard_label(self, set_name: str) -> str:
        sid = set_id_for_pack(set_name) or 1
        return shard_set_name(sid)

    def shard_summary(self, totals: Dict[str, int] | None = None) -> str:
        blob = totals or self.total_yield_by_set
        parts = []
        for set_name, amount in sorted(blob.items(), key=lambda item: _set_sort_key(item[0])):
            if amount <= 0:
                continue
            parts.append(f"{amount} {self.shard_label(set_name)}")
        return ", ".join(parts)

    async def finalize(self, interaction: discord.Interaction, content: str | None = None):
        self.stop()
        for c in self.children:
            c.disabled = True
        try:
            if interaction.response.is_done():
                await interaction.followup.edit_message(
                    message_id=self.message.id if self.message else interaction.message.id,
                    content=content,
                    view=None,
                    embeds=[],
                )
            else:
                await interaction.response.edit_message(content=content, view=None, embeds=[])
        except discord.InteractionResponded:
            await interaction.message.edit(content=content, view=None, embeds=[])
        except discord.NotFound:
            await interaction.message.edit(content=content, view=None, embeds=[])

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not self._is_requester(interaction):
            return await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)
        await self._ensure_deferred(interaction)
        if self._locked:
            await self._ensure_deferred(interaction)
            return
        self._locked = True
        await self._show_processing_state(interaction)

        credited_by_set: Dict[str, int] = {}

        try:
            for row in self.plan_rows:
                amt = int(row["to_frag"])
                yield_per = int(row.get("yield_each", 0))
                if amt <= 0:
                    continue
                set_name = row.get("set") or ""
                rarity = row.get("rarity") or ""
                removed = db_collection_remove_exact_print(
                    self.state, self.user.id,
                    card_name=row["name"],
                    card_rarity=rarity,
                    card_set=set_name,
                    card_code=row["code"],
                    card_id=row["id"],
                    amount=amt
                )
                if removed > 0:
                    credited_by_set[set_name] = credited_by_set.get(set_name, 0) + removed * yield_per

            for set_name, total in credited_by_set.items():
                if total <= 0:
                    continue
                sid = set_id_for_pack(set_name) or 1
                db_shards_add(self.state, self.user.id, sid, total)

            shard_blob = self.shard_summary(credited_by_set) or "0 shards"
            await self.finalize(interaction, content=f"✅ Fragmented these cards into **{shard_blob}**.")
        except Exception:
            self._locked = False
            raise

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not self._is_requester(interaction):
            return await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)
        await self._ensure_deferred(interaction)
        await self.finalize(interaction, content="Cancelled.")

class PaginatedBulkFragmentConfirmView(BulkFragmentConfirmView):
    def __init__(
        self,
        state: AppState,
        user: discord.Member,
        plan_rows: List[Dict],
        keep: int,
        total_yield_by_set: Dict[str, int],
        *,
        content: str,
        embeds: list[discord.Embed],
        timeout: float = 120,
    ):
        super().__init__(state, user, plan_rows, keep, total_yield_by_set, timeout=timeout)
        self.embeds = embeds or []
        self.content = content
        self.current_page = 0
        self.message: Optional[discord.Message] = None
        self._sync_page_title()
        if len(self.embeds) <= 1:
            self.next_page.disabled = True
            self.prev_page.disabled = True

    def _page_count(self) -> int:
        return max(len(self.embeds), 1)

    def _sync_page_title(self):
        if len(self.embeds) <= 1:
            return
        total = self._page_count()
        for idx, embed in enumerate(self.embeds, start=1):
            embed.title = f"Cards to fragment ({idx}/{total})"

    def _wrap_index(self, idx: int) -> int:
        total = self._page_count()
        if total <= 1:
            return 0
        return idx % total

    async def _show_page(self, interaction: discord.Interaction, idx: int):
        if self._locked:
            try: await interaction.response.defer_update()
            except: pass
            return
        self.current_page = self._wrap_index(idx)
        embed = self.embeds[self.current_page] if self.embeds else None
        if interaction.response.is_done():
            await interaction.followup.edit_message(
                message_id=self.message.id if self.message else interaction.message.id,
                content=self.content,
                embed=embed,
                view=self,
            )
        else:
            await interaction.response.edit_message(content=self.content, embed=embed, view=self)

    @discord.ui.button(label="◀️ Prev", style=discord.ButtonStyle.secondary, row=1)
    async def prev_page(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not self._is_requester(interaction):
            return await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)
        await self._show_page(interaction, self.current_page - 1)

    @discord.ui.button(label="Next ▶️", style=discord.ButtonStyle.secondary, row=1)
    async def next_page(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not self._is_requester(interaction):
            return await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)
        await self._show_page(interaction, self.current_page + 1)

class CardsShop(commands.Cog):
    """
    Refactor: /craft (was /buy) and /shard (was /sell), shop-only.
    """
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: AppState = bot.state

    async def ac_craft(self, interaction: discord.Interaction, current: str):
        # Suggest craftable prints from shop index (set-aware, as before)
        return suggest_prints_with_set(self.state, current, include_starters=True)

    async def ac_card_lookup(self, interaction: discord.Interaction, current: str):
        # Allow starter cards to be suggested for lookup commands
        return suggest_prints_with_set(self.state, current, include_starters=True, include_tins=True)

    async def _fetch_cardinfo_from_api(self, card: dict) -> Optional[dict]:
        loop = asyncio.get_running_loop()

        def _blocking_lookup() -> Optional[dict]:
            params = {}
            cid = (card.get("id") or card.get("cardid") or card.get("card_id"))
            name = (card.get("name") or card.get("cardname") or "").strip()
            if cid and str(cid).strip().isdigit():
                params["id"] = str(cid).strip()
            elif name:
                params["name"] = name
            else:
                return None

            try:
                resp = requests.get(YGOPRO_API_URL, params=params, timeout=15)
                resp.raise_for_status()
                payload = resp.json()
            except requests.RequestException:
                return None

            data = payload.get("data") or []
            if not data:
                return None

            set_name = card_set_name(card).lower()
            if set_name:
                for entry in data:
                    for card_set in entry.get("card_sets") or []:
                        if (card_set.get("set_name") or "").strip().lower() == set_name:
                            return entry

            return data[0]

        return await loop.run_in_executor(None, _blocking_lookup)

    @staticmethod
    def _extract_card_text(card: dict, api_entry: Optional[dict]) -> str:
        for key in ("desc", "cardtext", "text"):
            val = (card.get(key) or "").strip()
            if val:
                return val

        if api_entry:
            api_text = (api_entry.get("desc") or "").strip()
            if api_text:
                return api_text

        return "No card text available."

    @staticmethod
    def _resolve_set_info(card_set: str, api_entry: Optional[dict]) -> tuple[str, str]:
        pack_name = (card_set or "").strip()
        rarity = ""

        sets = (api_entry or {}).get("card_sets") or []
        if sets:
            target_set = None
            if pack_name:
                for card_set_row in sets:
                    if (card_set_row.get("set_name") or "").strip().lower() == pack_name.lower():
                        target_set = card_set_row
                        break
            if target_set is None:
                target_set = sets[0]

            pack_name = pack_name or (target_set.get("set_name") or "").strip()
            rarity = canonicalize_rarity(target_set.get("set_rarity") or "")

        return pack_name, rarity

    @app_commands.command(
        name="craft",
        description="Craft a specific card using shards"
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
        if is_tin_promo_print(self.state, c, set_name=set_present):
            return await interaction.response.send_message("❌ Tin promo cards cannot be crafted.", ephemeral=True)

        rarity = get_card_rarity(c)
        price_each, sale_row = craft_cost_for_card(self.state, c, set_present)
        if rarity == "starlight" or not price_each:
            return await interaction.response.send_message("❌ This printing cannot be crafted.", ephemeral=True)
        total = price_each * amount
        # Reuse your existing confirmation view (performs wallet debit + award)
        view = ConfirmBuyCardView(self.state, requester=interaction.user, print_key=card, amount=amount, total_cost=total)
        shard_pretty = shard_set_name(set_id_for_pack(set_present) or 1)
        return await interaction.response.send_message(
            f"Are you sure you want to **craft** **{amount}× {card_label_with_badge(self.state, c)}** for **{total}** {shard_pretty}?",
            view=view,
            ephemeral=True
        )

    async def ac_shard(self, interaction: discord.Interaction, current: str):
        # Suggest prints the CALLER owns (they're sharding their own cards)
        return suggest_owned_prints_relaxed(self.state, interaction.user.id, current)

    @app_commands.command(
        name="card",
        description="View card details for a given card name",
    )
    @app_commands.guilds(GUILD)
    @app_commands.describe(
        cardname="Choose the exact printing",
    )
    @app_commands.autocomplete(cardname=ac_card_lookup)
    async def card(self, interaction: discord.Interaction, cardname: str):
        card = find_card_by_print_key(self.state, cardname)
        if not card:
            return await interaction.response.send_message("Card not found.", ephemeral=True)

        await interaction.response.defer()

        api_entry = await self._fetch_cardinfo_from_api(card)
        resolved_set = resolve_card_set(self.state, card) or card_set_name(card)
        is_tin_promo = bool(resolved_set and is_tin_promo_print(self.state, card, set_name=resolved_set))
        if is_tin_promo:
            pack_name = resolved_set
            rarity_from_api = ""
        else:
            pack_name, rarity_from_api = self._resolve_set_info(resolved_set, api_entry)

        rarity = get_card_rarity(card) or rarity_from_api
        badge = rarity_badge(self.state, rarity)
        name = (card.get("name") or card.get("cardname") or "Unknown").strip()
        card_text = self._extract_card_text(card, api_entry)

        image_url = None
        image_file = None

        art_path = card_art_path_for_card(card)
        if art_path and art_path.is_file():
            image_file = discord.File(art_path, filename=art_path.name)
            image_url = f"attachment://{art_path.name}"
        else:
            image_url = card_art_url_for_card(card)
            if not image_url and api_entry:
                images = api_entry.get("card_images") or []
                if images:
                    image_url = images[0].get("image_url") or images[0].get("image_url_small")

        print_label = pack_name or "Unknown set"
        if is_tin_promo:
            print_label = f"{print_label}"

        desc_lines = [
            f"**{name}**",
            f"Print: {badge} {print_label}",
            "",
            card_text,
        ]
        desc_body = "\n".join(desc_lines)

        image_embed = None
        if image_url:
            image_embed = discord.Embed()
            image_embed.set_image(url=image_url)

        info_embed = discord.Embed(description=desc_body)

        embeds = [e for e in (image_embed, info_embed) if e is not None]

        await interaction.followup.send(embeds=embeds, file=image_file)

    @app_commands.command(
        name="fragment",
        description="Break down a card into shards"
    )
    @app_commands.guilds(GUILD)
    @app_commands.describe(
        card="Choose the exact printing you own",
        amount="How many copies (max 100)",
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
        if is_starter_set(set_present):
            return await interaction.response.send_message("❌ Starter deck cards cannot be fragmented.", ephemeral=True)

        rarity = get_card_rarity(c)
        #price_each = SHARD_YIELD_BY_RARITY.get(rarity)
        price_each, ov = db_fragment_yield_for_card(self.state, c, set_present)
        if rarity == "starlight" or price_each is None:
            return await interaction.response.send_message("❌ This printing cannot be crafted.", ephemeral=True)
        total = price_each * amount
        # Reuse your existing confirmation view (performs removal + credit)
        view = ConfirmSellCardView(self.state, requester=interaction.user, print_key=card, amount=amount, total_credit=total)
        shard_pretty = shard_set_name(set_id_for_pack(set_present) or 1)
        return await interaction.response.send_message(
            f"Are you sure you want to **fragment** **{amount}× {card_label_with_badge(self.state, c)}** into **{total}** {shard_pretty}?",
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
        pack_name: Optional[str] = None,
        rarity: Optional[str] = None,
        keep: app_commands.Range[int, 0, 99] = 3,
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        if pack_name and pack_name not in (self.state.packs_index or {}):
            return await interaction.followup.send("❌ Unknown pack/set.", ephemeral=True)

        r = norm_rarity(rarity) if rarity else None
        if r and r not in FRAGMENTABLE_RARITIES:
            return await interaction.followup.send("❌ That rarity can’t be fragmented.", ephemeral=True)

        # Exact-print rows with qty > keep: [{"name","qty","to_frag","code","id"}, ...]
        rows = db_collection_list_for_bulk_fragment(self.state, interaction.user.id, pack_name, r, int(keep))
        filtered_rows = []
        for row in rows:
            row_rarity = norm_rarity(row.get("rarity"))
            if row_rarity not in FRAGMENTABLE_RARITIES:
                continue
            row = {**row, "rarity": row_rarity}
            filtered_rows.append(row)
        rows = _sort_rows_by_set(filtered_rows)
        if not rows:
            return await interaction.followup.send(
                "Nothing to fragment with those filters (or all at/under the keep amount).",
                ephemeral=True
            )

        # --- compute per-print yield (with overrides) and grand total ---
        total_yield_by_set: Dict[str, int] = {}
        preview_lines = []
        keep_floor = int(keep)

        # Pass an enriched list (with yield_each) to the confirm view so we don’t
        # recompute and risk drift between preview and execution.
        enriched_rows = []
        for row in rows:
            # minimal "card" dict for the helper (uses your field names)
            card_min = {
                "name": row["name"],
                "rarity": row.get("rarity"),
                "code": row.get("code"),
                "id": row.get("id"),
            }
            yield_each, ov = db_fragment_yield_for_card(self.state, card_min, set_name=row.get("set"))
            qty_to_frag = int(row["to_frag"])
            set_name = row.get("set") or ""
            total_yield_by_set[set_name] = total_yield_by_set.get(set_name, 0) + qty_to_frag * yield_each

            boost = ""
            if ov is not None and int(ov.get("yield_override", yield_each)) != int(SHARD_YIELD_BY_RARITY.get(r, 0)):
                # keep it short; you can expand this if you store reason/expiry, etc.
                boost = " (override)"

            badge = rarity_badge(self.state, row.get("rarity"))
            pack_suffix = f" [{shorten(set_name, 32)}]" if set_name else ""
            preview_lines.append(
                f"{badge} x{qty_to_frag} {shorten(row['name'], 64)}{pack_suffix}"
            )

            enriched_rows.append({**row, "yield_each": int(yield_each)})

        preview = "\n".join(preview_lines)

        summary_view = BulkFragmentConfirmView(
            self.state,
            interaction.user,
            enriched_rows,           # <-- pass yields to the view
            keep_floor,
            total_yield_by_set,
        )

        shard_breakdown = summary_view.shard_summary() or shard_set_name(set_id_for_pack(pack_name) or 1)

        preview_lines = preview.split("\n") if preview else []
        embeds: list[discord.Embed] = []
        chunk: list[str] = []
        chunk_len = 0
        for line in preview_lines:
            line_len = len(line) + 1  # account for newline
            if chunk and (chunk_len + line_len > 3500 or len(chunk) >= 24):
                embeds.append(discord.Embed(
                    title=f"Cards to fragment ({len(embeds) + 1})",
                    description="\n".join(chunk)
                ))
                chunk = []
                chunk_len = 0
            chunk.append(line)
            chunk_len += line_len
        if chunk:
            embeds.append(discord.Embed(
                title=f"Cards to fragment ({len(embeds) + 1})",
                description="\n".join(chunk)
            ))

        if embeds:
            total_pages = len(embeds)
            for i, embed in enumerate(embeds, start=1):
                if total_pages > 1:
                    embed.title = f"Cards to fragment ({i}/{total_pages})"

        content = "\n".join([
            "Are you sure you want to fragment the following cards?",
            f"This will yield **{shard_breakdown}**.",
        ])
        view = PaginatedBulkFragmentConfirmView(
            self.state,
            interaction.user,
            enriched_rows,
            keep_floor,
            total_yield_by_set,
            content=content,
            embeds=embeds,
        )

        primary_embed = embeds[0] if embeds else None
        message = await interaction.followup.send(
            content=content,
            ephemeral=True,
            view=view,
            embed=primary_embed,
        )
        view.message = message

async def setup(bot: commands.Bot):
    await bot.add_cog(CardsShop(bot))

    for guild in (GUILD, None):
        bot.tree.remove_command(
            "craft",
            type=discord.AppCommandType.chat_input,
            guild=guild,
        )

    if not is_shop_gamba_enabled():
        for cmd_name in ("craft", "fragment", "fragment_bulk"):
            for guild in (GUILD, None):
                bot.tree.remove_command(
                    cmd_name,
                    type=discord.AppCommandType.chat_input,
                    guild=guild,
                )
