from collections import Counter
import discord
from discord.ui import View, Select, button, Button
from core.packs import RARITY_ORDER, open_pack_from_csv
from core.db import db_add_cards, db_wallet_try_spend_fitzcoin, db_wallet_add
from core.state import AppState
from typing import List, Tuple

PACK_COST = 10

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

class ConfirmSpendView(discord.ui.View):
    def __init__(self, state, requester: discord.Member, pack_name: str, amount: int, on_confirm):
        """
        on_confirm: async function(interaction, state, requester, pack_name, amount) -> None
                    Should perform the actual pack opening + rendering (your existing logic).
        """
        super().__init__(timeout=90)
        self.state = state
        self.requester = requester
        self.pack_name = pack_name
        self.amount = amount
        self.on_confirm = on_confirm  # callback to your existing flow

    async def interaction_guard(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.requester.id:
            await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.success)
    async def yes(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.interaction_guard(interaction):
            return

        total_cost = self.amount * PACK_COST
        await interaction.response.defer(ephemeral=True, thinking=True)

        # Try to spend atomically
        after_spend = db_wallet_try_spend_fitzcoin(self.state, self.requester.id, total_cost)
        if after_spend is None:
            # Not enough funds, disable buttons and inform
            self.disable_all_items()
            await interaction.edit_original_response(
                content=(
                    f"❌ Not enough fitzcoin to open **{self.amount}** pack(s) of **{self.pack_name}**.\n"
                    f"Cost: **{total_cost}**, your balance is too low."
                ),
                view=self
            )
            return

        try:
            # Run your existing pack-open + DB write + paginator/render callback
            await self.on_confirm(interaction, self.state, self.requester, self.pack_name, self.amount)

            # Public announcement (optional)
            await interaction.channel.send(
                f"🎉 {self.requester.mention} opened **{self.amount}** pack(s) of **{self.pack_name}**!"
            )
            await interaction.channel.send(
                f"💰 Remaining balance → **{after_spend['fitzcoin']}** fitzcoin, "
                f"**{after_spend['mambucks']}** mambucks."
            )

            # Disable buttons (done)
            self.disable_all_items()
            await interaction.edit_original_response(view=self)

        except Exception:
            # Refund if your open/render fails
            db_wallet_add(self.state, self.requester.id, d_fitzcoin=total_cost)
            self.disable_all_items()
            await interaction.edit_original_response(
                content="⚠️ Something went wrong opening packs. You were not charged.",
                view=self
            )
            raise

    @discord.ui.button(label="No", style=discord.ButtonStyle.danger)
    async def no(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.interaction_guard(interaction):
            return
        self.disable_all_items()
        await interaction.response.edit_message(
            content="Cancelled. No currency spent.",
            view=self
        )

    def disable_all_items(self):
        for item in self.children:
            item.disabled = True

    async def on_timeout(self):
        try:
            self.disable_all_items()
            # Attempt to edit the original message quietly on timeout
            # (wrapped in try/except to avoid errors if message no longer exists)
            # You'll need a reference to the message to reliably edit here; if not tracked,
            # leaving timeout silent is fine.
        except Exception:
            pass

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

class PacksSelect(Select):
    def __init__(self, state: AppState, requester: discord.User, amount: int):
        self.requester = requester
        self.amount = amount
        self.state = state
        names = sorted((state.packs_index or {}).keys())[:25]
        options = [discord.SelectOption(label=n, description="Open this pack", value=n) for n in names]
        super().__init__(placeholder="Choose a pack…", min_values=1, max_values=1, options=options, disabled=(len(options)==0))

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.requester.id:
            await interaction.response.send_message("Only the requester can use this menu.", ephemeral=True); return
        pack_name = self.values[0]
        total_cost = self.amount * PACK_COST
        per_pack: list[list[dict]] = []

        # Build the confirm view, pass a callback that runs your existing open + paginator
        confirm_view = ConfirmSpendView(
            state=self.state,
            requester=self.requester,
            pack_name=pack_name,
            amount=self.amount,
            on_confirm=self._open_and_render  # provided below
        )

        # NOTE: quick edit, no defer needed here
        await interaction.response.edit_message(
            content=(
                f"Are you sure you want to spend **{total_cost}** fitzcoin on "
                f"**{self.amount}** pack(s) of **{pack_name}**?"
            ),
            view=confirm_view
        )


    async def _open_and_render(self, interaction: discord.Interaction, state, requester, pack_name: str, amount: int):
        per_pack: list[list[dict]] = []
        total_cost = self.amount * PACK_COST
        try:
            for _ in range(self.amount):
                per_pack.append(open_pack_from_csv(self.state, pack_name, 1))
        except Exception as e:
            for ch in self.view.children: ch.disabled = True
            db_wallet_add(self.state, self.requester.id, d_fitzcoin=total_cost)
            await interaction.response.edit_message(content=f"Failed to open: {e}", view=self.view)
            return
        flat = [c for pack in per_pack for c in pack]
        db_add_cards(self.state, self.requester.id, flat, pack_name)
        paginator = PackResultsPaginator(self.requester, pack_name, per_pack)
        for ch in self.view.children: ch.disabled = True
        await interaction.edit_original_response(content=None, embed=paginator._embed_for_index(), view=paginator)

class PacksSelectView(View):
    def __init__(self, state: AppState, requester: discord.User, amount: int, timeout: float = 90):
        super().__init__(timeout=timeout)
        self.add_item(PacksSelect(state, requester, amount))

    async def on_timeout(self):
        for child in self.children: child.disabled = True

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
