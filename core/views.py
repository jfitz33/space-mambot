from collections import Counter
import discord
from discord.ui import View, Select, button, Button
from core.packs import RARITY_ORDER, open_pack_from_csv, open_pack_with_guaranteed_top_from_csv
from core.db import db_add_cards, db_wallet_try_spend_fitzcoin, db_wallet_add
from core.state import AppState
from typing import List, Tuple, Optional, Literal

PACK_COST = 10
PACKS_IN_BOX = 24
BOX_COST = 200

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

def _build_pack_options(state) -> List[discord.SelectOption]:
    opts: List[discord.SelectOption] = []
    # assume state.packs_index = { pack_key: {"display_name": "...", "desc": "...", ...}, ... }
    for key, meta in state.packs_index.items():
        label = (meta.get("display_name") or key)[:100]
        desc = (meta.get("desc") or meta.get("description") or "")[:100] or None
        opts.append(discord.SelectOption(label=label, value=key, description=desc))
        if len(opts) >= 25:  # Discord hard limit
            break
    if not opts:
        opts.append(discord.SelectOption(label="No packs available", value="__none__"))
    return opts

class PacksDropdown(discord.ui.Select):
    def __init__(self, parent_view: "PacksSelectView"):
        self.parent_view = parent_view
        options = _build_pack_options(parent_view.state)
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
    def __init__(self, state, requester, pack_name, amount, on_confirm, total_cost: int | None = None, *, timeout: float = 90):
        super().__init__(timeout=timeout)
        self.state = state
        self.requester = requester
        self.pack_name = pack_name
        self.amount = amount
        self.on_confirm = on_confirm
        self.total_cost = total_cost
        self._processing = False

    async def _remove_ui(self, interaction: discord.Interaction, content: str | None = None):
        """Remove the confirm buttons from the message that contains them."""
        self.stop()
        for item in self.children:
            item.disabled = True
        try:
            # This edits the message that the button/select lives on
            await interaction.response.edit_message(content=content, view=None)
        except discord.InteractionResponded:
            # If we already responded/deferred, edit the message object directly
            await interaction.message.edit(content=content, view=None)
        except Exception:
            pass

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.success)
    async def yes(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.requester.id:
            return await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)

        # Re-entrancy guard (ignore spam clicks)
        if self._processing:
            try:
                await interaction.response.defer_update()
            except Exception:
                pass
            return
        self._processing = True

        # 🔒 Immediately remove the buttons so they can’t be clicked again
        await self._remove_ui(interaction, content="Processing…")

        # Acknowledge so we can send followups
        try:
            await interaction.followup.defer(ephemeral=True)
        except Exception:
            pass

        total_cost = self.total_cost if self.total_cost is not None else self.amount * PACK_COST
        after_spend = db_wallet_try_spend_fitzcoin(self.state, self.requester.id, total_cost)
        if after_spend is None:
            self._processing = False
            return await interaction.followup.send(
                f"❌ Not enough fitzcoin to open **{self.amount}** pack(s) of **{self.pack_name}**.\n"
                f"Cost: **{total_cost}**.",
                ephemeral=True
            )

        try:
            # Open + render (your renderer must use followup.send)
            await self.on_confirm(interaction, self.state, self.requester, self.pack_name, self.amount)

            # Public announcements
            await interaction.channel.send(
                f"🎉 {self.requester.mention} opened **{self.amount}** pack(s) of **{self.pack_name}**!"
            )
            await interaction.channel.send(
                f"💰 Remaining balance → **{after_spend['fitzcoin']}** fitzcoin, "
                f"**{after_spend['mambucks']}** mambucks."
            )
        except Exception:
            # Refund on failure
            db_wallet_add(self.state, self.requester.id, d_fitzcoin=total_cost)
            await interaction.followup.send("⚠️ Something went wrong opening packs. You were not charged.", ephemeral=True)
            raise
        finally:
            self._processing = False

    @discord.ui.button(label="No", style=discord.ButtonStyle.danger)
    async def no(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.requester.id:
            return await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)
        # Remove the UI and leave a small notice (or set content=None to blank it)
        await self._remove_ui(interaction, content="Cancelled.")

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
        self.add_item(PacksDropdown(self))

    async def _handle_pack_choice(self, interaction: discord.Interaction, pack_name: str):
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
                    f"Open a **box** of **{pack_name}** for **{BOX_COST}** fitzcoin?\n"
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
                content=(f"Are you sure you want to spend **{total_cost}** fitzcoin on "
                         f"**{self.amount}** pack(s) of **{pack_name}**?"),
                view=confirm_view
            )

    async def _open_and_render(self, interaction, state, requester, pack_name: str, amount: int):
        per_pack: list[list[dict]] = []
        try:
            for _ in range(self.amount):
                per_pack.append(open_pack_from_csv(self.state, pack_name, 1))
        except Exception as e:
            return await interaction.followup.send(f"Failed to open: {e}", ephemeral=True)

        flat = [c for pack in per_pack for c in pack]
        db_add_cards(self.state, self.requester.id, flat, pack_name)

        paginator = PackResultsPaginator(self.requester, pack_name, per_pack)
        await interaction.followup.send(embed=paginator._embed_for_index(), view=paginator, ephemeral=True)



    async def _open_box_and_render(self, interaction, state, requester, pack_name: str, amount: int):
        per_pack: list[list[dict]] = []
        try:
            for _ in range(18):
                per_pack.append(open_pack_with_guaranteed_top_from_csv(self.state, pack_name, "super"))
            for _ in range(5):
                per_pack.append(open_pack_with_guaranteed_top_from_csv(self.state, pack_name, "ultra"))
            per_pack.append(open_pack_with_guaranteed_top_from_csv(self.state, pack_name, "secret"))
        except Exception as e:
            return await interaction.followup.send(f"Failed to open box: {e}", ephemeral=True)

        flat = [c for pack in per_pack for c in pack]
        db_add_cards(self.state, self.requester.id, flat, pack_name)

        paginator = PackResultsPaginator(self.requester, pack_name, per_pack)
        await interaction.followup.send(embed=paginator._embed_for_index(), view=paginator, ephemeral=True)

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
