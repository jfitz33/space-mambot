import discord, os, time
from discord.ext import commands
from discord import app_commands
from typing import List, Literal, Optional

from core.db import (
    db_admin_add_card,
    db_admin_remove_card,
    db_collection_clear,
    db_wallet_set,
    db_wallet_add,
    db_wallet_get,
    db_shards_get,
    db_shards_add,
    db_shard_override_set,
    db_shard_override_clear,
    db_shard_override_list_active,
    db_stats_reset,
    db_stats_record_loss,
    db_stats_revert_result,
    db_team_points_clear,
    db_wheel_tokens_clear,
    db_wishlist_clear,
)
from core.quests.schema import db_reset_all_user_quests
from core.constants import PACKS_BY_SET, TEAM_ROLE_NAMES
from core.currency import shard_set_name  # pretty name per set
from core.cards_shop import find_card_by_print_key, resolve_card_set, card_label

# Set guild ID for development
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

# NEW: currency selector for admin wallet ops
Currency = Literal["mambucks", "shards"]

# NEW: autocomplete for shard_set (int choices based on PACKS_BY_SET keys)
def _available_set_choices() -> List[app_commands.Choice[int]]:
    # Present known set IDs with friendly names
    out: List[app_commands.Choice[int]] = []
    for sid in sorted(PACKS_BY_SET.keys()):
        out.append(app_commands.Choice(name=f"{sid} — {shard_set_name(sid)}", value=int(sid)))
        if len(out) >= 25:
            break
    # Ensure Set 1 exists even if PACKS_BY_SET is empty (safety)
    if not out:
        out.append(app_commands.Choice(name=f"1 — {shard_set_name(1)}", value=1))
    return out

class Admin(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state = bot.state

    async def ac_shard_set(self, interaction: discord.Interaction, current: str):
        # We can optionally filter by `current` if user types a digit; otherwise show all
        choices = _available_set_choices()
        if current:
            cur = (current or "").strip()
            choices = [c for c in choices if cur in c.name or cur == str(c.value)]
        return choices[:25]
    
    # Re-using shop's suggest prints with set function for admin override ac
    async def ac_print(self, interaction: discord.Interaction, current: str):
        from cogs.cards_shop import suggest_prints_with_set  # you already have this
        return suggest_prints_with_set(self.state, current)
    
    def _resolve_print_key(self, print_key: str) -> Optional[dict]:
        """Return a copy of the card metadata for the supplied print key."""
        if not print_key:
            return None
        card = find_card_by_print_key(self.state, print_key)
        if not card:
            return None
        resolved = dict(card)
        set_name = resolve_card_set(self.state, resolved)
        if set_name:
            resolved["set"] = set_name
            resolved.setdefault("cardset", set_name)
        return resolved

    @staticmethod
    def _card_name(card: dict) -> str:
        return (card.get("name") or card.get("cardname") or "").strip()

    @staticmethod
    def _card_rarity(card: dict) -> str:
        return (card.get("rarity") or card.get("cardrarity") or "").strip()

    @staticmethod
    def _card_set(card: dict) -> str:
        return (card.get("set") or card.get("cardset") or "").strip()

    @staticmethod
    def _card_code(card: dict) -> str:
        return (card.get("code") or card.get("cardcode") or "").strip()

    @staticmethod
    def _card_id(card: dict) -> str:
        return (card.get("id") or card.get("cardid") or "").strip()

    @staticmethod
    def _win_pct(stats: dict) -> float:
        games = int(stats.get("games", 0) or 0)
        wins = int(stats.get("wins", 0) or 0)
        return (wins / games * 100.0) if games else 0.0

    @app_commands.command(name="admin_add_card", description="(Admin) Add a card to a user's collection (rarity from pack)")
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        user="User to modify",
        card_name="Card to add (choose the exact printing)",
        qty="Quantity to add (default 1)",
    )
    @app_commands.autocomplete(card_name=ac_print)
    async def admin_add_card(self, interaction: discord.Interaction,
                             user: discord.User, card_name: str,
                             qty: app_commands.Range[int,1,999]=1):
        card = self._resolve_print_key(card_name)
        if not card:
            await interaction.response.send_message("❌ Card not found for that selection.", ephemeral=True)
            return

        name = self._card_name(card)
        rarity = self._card_rarity(card)
        card_set = self._card_set(card)
        if not card_set:
            await interaction.response.send_message("❌ Unable to determine the card's set.", ephemeral=True)
            return
        card_code = self._card_code(card)
        card_id = self._card_id(card)

        new_total = db_admin_add_card(
            self.bot.state,
            user.id,
            name=name,
            rarity=rarity,
            card_set=card_set,
            card_code=card_code,
            card_id=card_id,
            qty=qty,
        )
        display_card = dict(card)
        display_card.setdefault("set", card_set)
        label = card_label(display_card)

        await interaction.response.send_message(
            f"✅ Added **x{qty}** of **{label}** "
            f"to {user.mention}. New total: **{new_total}**.", ephemeral=True)
        await interaction.channel.send(
            f"📦 **{interaction.user.display_name}** added x{qty} **{label}** to **{user.display_name}**'s collection."
        )

    @app_commands.command(name="admin_remove_card", description="(Admin) Remove a card row (rarity from pack)")
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        user="User to modify",
        card_name="Card to remove (choose the exact printing)",
        qty="Quantity to remove (default 1)",
    )
    @app_commands.autocomplete(card_name=ac_print)
    async def admin_remove_card(self, interaction: discord.Interaction,
                                user: discord.User, card_name: str,
                                qty: app_commands.Range[int,1,999]=1):
        card = self._resolve_print_key(card_name)
        if not card:
            await interaction.response.send_message("❌ Card not found for that selection.", ephemeral=True)
            return

        name = self._card_name(card)
        rarity = self._card_rarity(card)
        card_set = self._card_set(card)
        if not card_set:
            await interaction.response.send_message("❌ Unable to determine the card's set.", ephemeral=True)
            return
        card_code = self._card_code(card)
        card_id = self._card_id(card)

        removed, remaining = db_admin_remove_card(
            self.bot.state,
            user.id,
            name=name,
            rarity=rarity,
            card_set=card_set,
            card_code=card_code,
            card_id=card_id,
            qty=qty,
        )
        display_card = dict(card)
        display_card.setdefault("set", card_set)
        label = card_label(display_card)

        if removed == 0:
            await interaction.response.send_message("ℹ️ No matching row for that card.", ephemeral=True); return
        if remaining > 0:
            await interaction.response.send_message(
                f"✅ Removed **x{removed}** of **{label}** "
                f"from {user.mention}. Remaining: **{remaining}**.", ephemeral=True)
        else:
            await interaction.response.send_message(
                f"🗑 **{interaction.user.display_name}** removed x{removed} **{label}** from **{user.display_name}**'s collection.",
                ephemeral=True)
        await interaction.channel.send(
            f"🗑 **{interaction.user.display_name}** removed x{removed} **{label}** from **{user.display_name}**'s collection."
        )

    @app_commands.command(
        name="admin_reset_user",
        description="(Admin) Clear a user's collection and remove their team roles."
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(user="Member to reset", reason="Optional reason")
    async def admin_reset_user(self, interaction: discord.Interaction, user: discord.Member, reason: str | None = None):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("You must be an administrator to use this.", ephemeral=True)
            return

        if user.bot:
            await interaction.response.send_message("You can’t reset a bot account.", ephemeral=True)
            return

        # Clear collection & wishlist data
        deleted = db_collection_clear(self.state, user.id)
        wishlist_removed = db_wishlist_clear(self.state, user.id)

        # Remove quest progress (weekly/daily/etc.)
        quest_rows = await db_reset_all_user_quests(self.state, user.id)

        # Reset wallet balances (capture previous values for messaging)
        wallet_before = db_wallet_get(self.state, user.id)
        db_wallet_set(self.state, user.id, fitzcoin=0, mambucks=0)

        # Zero shards across all known sets
        shard_clears: list[str] = []
        for sid in sorted(PACKS_BY_SET.keys() or [1]):
            before = db_shards_get(self.state, user.id, sid)
            if before:
                db_shards_add(self.state, user.id, sid, -before)
                shard_clears.append(f"{shard_set_name(sid)} ({before})")

        # Reset wheel tokens & win/loss stats
        wheel_tokens_removed = db_wheel_tokens_clear(self.state, user.id)
        stats_reset = db_stats_reset(self.state, user.id)

        # Clear stored team points within this guild (if any)
        team_points_removed = 0
        if interaction.guild:
            team_points_removed = db_team_points_clear(self.state, interaction.guild.id, user.id)

        # Remove team roles (if present)
        removed_roles: list[str] = []
        failed_roles: list[str] = []
        if interaction.guild:
            for role_name in TEAM_ROLE_NAMES:
                role = discord.utils.get(interaction.guild.roles, name=role_name)
                if not role or role not in user.roles:
                    continue
                try:
                    await user.remove_roles(role, reason=reason or "Admin reset user")
                    removed_roles.append(role_name)
                except discord.Forbidden:
                    failed_roles.append(role_name)

        lines = [f"✅ Cleared **{deleted}** collection row(s) for {user.mention}."]
        if wishlist_removed:
            lines.append(f"✅ Cleared wishlist entries (**{wishlist_removed}** removed).")
        if quest_rows:
            lines.append(f"✅ Removed **{quest_rows}** quest progress row(s).")
        if wallet_before:
            mb_before = int(wallet_before.get("mambucks", 0) or 0)
            fz_before = int(wallet_before.get("fitzcoin", 0) or 0)
            if mb_before or fz_before:
                lines.append(
                    "✅ Reset wallet balances ("
                    f"Mambucks {mb_before} → 0; Fitzcoin {fz_before} → 0)."
                )
            else:
                lines.append("ℹ️ Wallet balances were already zero.")
        if shard_clears:
            lines.append("✅ Cleared shards: " + ", ".join(shard_clears))
        else:
            lines.append("ℹ️ No shards to clear.")
        if stats_reset.get("stats_rows") or stats_reset.get("match_rows"):
            lines.append(
                "✅ Reset win/loss record"
                f" (stats rows cleared: {stats_reset['stats_rows']}, matches removed: {stats_reset['match_rows']})."
            )
        else:
            lines.append("ℹ️ No win/loss history found to clear.")
        if wheel_tokens_removed:
            lines.append(f"✅ Removed **{wheel_tokens_removed}** stored wheel token(s).")
        if interaction.guild and team_points_removed:
            lines.append(f"✅ Cleared team points entries ({team_points_removed} row(s)).")
        if removed_roles:
            lines.append("✅ Removed team role(s): " + ", ".join(sorted(removed_roles)))
        missing_roles = [name for name in TEAM_ROLE_NAMES if name not in removed_roles and name not in failed_roles]
        if missing_roles:
            lines.append("ℹ️ Team role(s) not present: " + ", ".join(sorted(missing_roles)))
        if failed_roles:
            lines.append("⚠️ Could not remove team role(s): " + ", ".join(sorted(failed_roles)))
        if not removed_roles and not missing_roles and not failed_roles:
            lines.append("ℹ️ No team roles configured for removal.")
        if reason:
            lines.append(f"📝 Reason: {reason}")

        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @app_commands.command(
        name="admin_report_loss",
        description="(Admin) Record a loss between two players (updates both records).",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        loser="Player who lost the match",
        winner="Player who won the match",
    )
    async def admin_report_loss(
        self,
        interaction: discord.Interaction,
        loser: discord.Member,
        winner: discord.Member,
    ) -> None:
        if loser.id == winner.id:
            await interaction.response.send_message("You must choose two different players.", ephemeral=True)
            return
        if loser.bot or winner.bot:
            await interaction.response.send_message("Bots cannot play matches.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        loser_after, winner_after = db_stats_record_loss(
            self.state,
            loser_id=loser.id,
            winner_id=winner.id,
        )

        quests = interaction.client.get_cog("Quests")
        try:
            if quests and getattr(quests, "qm", None):
                await quests.qm.increment(winner.id, "win_3_matches", 1)
                await quests.qm.increment(loser.id, "matches_played", 1)
                await quests.qm.increment(winner.id, "matches_played", 1)
        except Exception as e:
            print("[admin] quest tick error during admin_report_loss:", e)

        lpct = self._win_pct(loser_after)
        wpct = self._win_pct(winner_after)

        embed = discord.Embed(
            title="Admin Match Recorded",
            description=f"**{loser.display_name}** lost to **{winner.display_name}**.",
            color=0xCC3333,
        )
        embed.add_field(
            name=f"{loser.display_name} — Record",
            value=(
                f"W: **{loser_after['wins']}**\n"
                f"L: **{loser_after['losses']}**\n"
                f"Win%: **{lpct:.1f}%**"
            ),
            inline=True,
        )
        embed.add_field(
            name=f"{winner.display_name} — Record",
            value=(
                f"W: **{winner_after['wins']}**\n"
                f"L: **{winner_after['losses']}**\n"
                f"Win%: **{wpct:.1f}%**"
            ),
            inline=True,
        )

        await interaction.followup.send(embed=embed, ephemeral=True)
        if interaction.channel:
            await interaction.channel.send(
                f"📝 Admin recorded a result: **{loser.display_name}** lost to **{winner.display_name}**."
            )

    @app_commands.command(
        name="admin_revert_result",
        description="(Admin) Revert the most recent recorded result between two players.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        loser="Player originally recorded as the loser",
        winner="Player originally recorded as the winner",
    )
    async def admin_revert_result(
        self,
        interaction: discord.Interaction,
        loser: discord.Member,
        winner: discord.Member,
    ) -> None:
        if loser.id == winner.id:
            await interaction.response.send_message("You must choose two different players.", ephemeral=True)
            return
        if loser.bot or winner.bot:
            await interaction.response.send_message("Bots cannot play matches.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        loser_after, winner_after = db_stats_revert_result(
            self.state,
            loser_id=loser.id,
            winner_id=winner.id,
        )

        if loser_after is None or winner_after is None:
            await interaction.followup.send(
                "❌ No recorded result found for that matchup to revert.",
                ephemeral=True,
            )
            return

        quests = interaction.client.get_cog("Quests")
        try:
            if quests and getattr(quests, "qm", None):
                await quests.qm.increment(winner.id, "win_3_matches", -1)
                await quests.qm.increment(loser.id, "matches_played", -1)
                await quests.qm.increment(winner.id, "matches_played", -1)
        except Exception as e:
            print("[admin] quest tick error during admin_revert_result:", e)

        lpct = self._win_pct(loser_after)
        wpct = self._win_pct(winner_after)

        embed = discord.Embed(
            title="Match Result Reverted",
            description=f"Removed the recorded loss of **{loser.display_name}** to **{winner.display_name}**.",
            color=0x2F855A,
        )
        embed.add_field(
            name=f"{loser.display_name} — Record",
            value=(
                f"W: **{loser_after['wins']}**\n"
                f"L: **{loser_after['losses']}**\n"
                f"Win%: **{lpct:.1f}%**"
            ),
            inline=True,
        )
        embed.add_field(
            name=f"{winner.display_name} — Record",
            value=(
                f"W: **{winner_after['wins']}**\n"
                f"L: **{winner_after['losses']}**\n"
                f"Win%: **{wpct:.1f}%**"
            ),
            inline=True,
        )

        await interaction.followup.send(embed=embed, ephemeral=True)
        if interaction.channel:
            await interaction.channel.send(
                f"↩️ Admin reverted a result: removed the loss for **{loser.display_name}** vs **{winner.display_name}**."
            )

    # ---- Add currency -------------------------------------------------------
    @app_commands.command(name="wallet_add", description="(Admin) Add currency to a user's wallet")
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        user="Player to adjust",
        currency="Choose Mambucks or Shards",
        amount="Amount to add (>=1)",
        shard_set="Required if currency=shards",
    )
    @app_commands.autocomplete(shard_set=ac_shard_set)
    async def wallet_add(
        self,
        interaction: discord.Interaction,
        user: discord.Member,
        currency: Currency,
        amount: app_commands.Range[int, 1, None],
        shard_set: Optional[int] = None,
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        if currency == "mambucks":
            before = db_wallet_get(self.state, user.id)
            after = db_wallet_add(self.state, user.id, d_mambucks=amount)
            await interaction.followup.send(
                (
                    f"✅ Added **{amount} Mambucks** to {user.mention}.\n"
                    f"Before → Mambucks **{before['mambucks']}**\n"
                    f"After  → Mambucks **{after['mambucks']}**"
                ),
                ephemeral=True,
            )
            return

        # shards path
        if shard_set is None:
            return await interaction.followup.send("❌ Please choose a **shard_set** for shards.", ephemeral=True)

        before = db_shards_get(self.state, user.id, shard_set)
        db_shards_add(self.state, user.id, shard_set, amount)
        after = db_shards_get(self.state, user.id, shard_set)
        title = shard_set_name(shard_set)
        await interaction.followup.send(
            (
                f"✅ Added **{amount} {title}** to {user.mention}.\n"
                f"Before → **{before}**\n"
                f"After  → **{after}**"
            ),
            ephemeral=True,
        )

    # ---- Remove currency ----------------------------------------------------
    @app_commands.command(name="wallet_remove", description="(Admin) Remove currency from a user's wallet")
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        user="Player to adjust",
        currency="Choose Mambucks or Shards",
        amount="Amount to remove (>=1)",
        shard_set="Required if currency=shards",
    )
    @app_commands.autocomplete(shard_set=ac_shard_set)
    async def wallet_remove(
        self,
        interaction: discord.Interaction,
        user: discord.Member,
        currency: Currency,
        amount: app_commands.Range[int, 1, None],
        shard_set: Optional[int] = None,
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        if currency == "mambucks":
            before = db_wallet_get(self.state, user.id)
            new_m = max(0, int(before["mambucks"]) - int(amount))
            db_wallet_set(self.state, user.id, mambucks=new_m)
            after = db_wallet_get(self.state, user.id)
            await interaction.followup.send(
                (
                    f"🧹 Removed **{amount} Mambucks** from {user.mention}.\n"
                    f"Before → Mambucks **{before['mambucks']}**\n"
                    f"After  → Mambucks **{after['mambucks']}**"
                ),
                ephemeral=True,
            )
            return

        # shards path
        if shard_set is None:
            return await interaction.followup.send("❌ Please choose a **shard_set** for shards.", ephemeral=True)

        before = db_shards_get(self.state, user.id, shard_set)
        delta = -min(int(amount), int(before))
        if delta == 0:
            return await interaction.followup.send("ℹ️ Nothing to remove (balance is already 0).", ephemeral=True)
        db_shards_add(self.state, user.id, shard_set, delta)
        after = db_shards_get(self.state, user.id, shard_set)
        title = shard_set_name(shard_set)
        await interaction.followup.send(
            (
                f"🧹 Removed **{abs(delta)} {title}** from {user.mention}.\n"
                f"Before → **{before}**\n"
                f"After  → **{after}**"
            ),
            ephemeral=True,
        )
    
    @app_commands.command(name="admin_fragment_override_set", description="(Admin) Temporarily override a card's fragment yield")
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(
        card="Choose the exact printing",
        yield_per_copy="Shards granted per copy while active",
        hours="Duration in hours (default 48 = 2 days)",
        reason="Optional note"
    )
    @app_commands.autocomplete(card=ac_print)
    async def admin_fragment_override_set(
        self,
        interaction: discord.Interaction,
        card: str,
        yield_per_copy: app_commands.Range[int,1,100000],
        hours: app_commands.Range[int,1,24*365] = 48,
        reason: str | None = None,
    ):
        await interaction.response.defer(ephemeral=True)
        c = find_card_by_print_key(self.state, card)
        if not c:
            return await interaction.followup.send("Printing not found.", ephemeral=True)
        set_name = resolve_card_set(self.state, c)
        if not set_name:
            return await interaction.followup.send("Printing is missing set.", ephemeral=True)
        name  = (c.get("name") or c.get("cardname") or "").strip()
        rarity= (c.get("rarity") or c.get("cardrarity") or "").strip()
        code  = (c.get("code") or c.get("cardcode")) or None
        cid   = (c.get("id")   or c.get("cardid"))   or None

        oid = db_shard_override_set(
            self.state,
            card_name=name, card_set=set_name, card_rarity=rarity,
            card_code=code, card_id=cid,
            yield_override=int(yield_per_copy),
            duration_seconds=int(hours)*3600,
            reason=reason or f"Set via /admin_fragment_override_set by {interaction.user.id}"
        )
        until = time.strftime("%Y-%m-%d %H:%M ET", time.localtime(int(time.time()+int(hours)*3600)))
        await interaction.followup.send(
            f"✅ Override **#{oid}**: {card_label(c)} → **{yield_per_copy}** shards/copy until **{until}**.",
            ephemeral=True
        )

    @app_commands.command(name="admin_fragment_override_clear", description="(Admin) Remove overrides for a printing or name+set")
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(card="Exact printing (recommended) OR leave blank and use name+set",
                           name="Card name (used if card left blank)",
                           card_set="Set name (used if card left blank)")
    @app_commands.autocomplete(card=ac_print)
    async def admin_fragment_override_clear(
        self,
        interaction: discord.Interaction,
        card: str | None = None,
        name: str | None = None,
        card_set: str | None = None,
    ):
        await interaction.response.defer(ephemeral=True)
        if card:
            c = find_card_by_print_key(self.state, card)
            if not c:
                return await interaction.followup.send("Printing not found.", ephemeral=True)
            set_name = resolve_card_set(self.state, c) or ""
            n = (c.get("name") or c.get("cardname") or "").strip()
            code = (c.get("code") or c.get("cardcode")) or None
            cid  = (c.get("id")   or c.get("cardid"))   or None
            deleted = db_shard_override_clear(self.state, card_name=n, card_set=set_name, card_code=code, card_id=cid)
        else:
            if not (name and card_set):
                return await interaction.followup.send("Provide either `card` OR (`name` and `card_set`).", ephemeral=True)
            deleted = db_shard_override_clear(self.state, card_name=name, card_set=card_set)

        await interaction.followup.send(f"🧹 Removed **{deleted}** override(s).", ephemeral=True)

    @app_commands.command(name="admin_fragment_override_list", description="(Admin) List active fragment overrides")
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    async def admin_fragment_override_list(self, interaction: discord.Interaction):
        rows = db_shard_override_list_active(self.state)
        if not rows:
            return await interaction.response.send_message("No active overrides.", ephemeral=True)
        lines = []
        for r in rows:
            until = time.strftime("%Y-%m-%d %H:%M ET", time.localtime(int(r["ends_at"])))
            tgt = f"{r['card_name']} [{r.get('card_set','')}]"
            if r.get("card_code") or r.get("card_id"):
                tgt += " (exact print)"
            lines.append(f"• **{tgt}** → **{r['yield_override']}** shards/copy · until **{until}**")
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(Admin(bot))
