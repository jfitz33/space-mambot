import asyncio
import discord, os, time, logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from discord.ext import commands
from discord import app_commands
from typing import List, Literal, Optional

from core.db import (
    db_admin_add_card,
    db_admin_remove_card,
    db_collection_clear,
    db_collection_total_by_rarity,
    db_wallet_set,
    db_wallet_add,
    db_wallet_get,
    db_shards_get,
    db_shards_add,
    db_shard_override_set,
    db_shard_override_clear,
    db_shard_override_list_active,
    db_starter_claim_clear,
    db_stats_reset,
    db_stats_record_loss,
    db_stats_revert_result,
    db_user_set_wins_clear,
    db_team_battleground_user_points_all,
    db_team_battleground_user_points_clear,
    db_team_battleground_user_points_for_user_all_sets,
    db_team_battleground_totals_get,
    db_team_battleground_totals_update,
    db_team_points_clear,
    db_wheel_tokens_clear,
    db_wishlist_clear,
    db_convert_all_mambucks_to_shards,
    db_clear_all_daily_quest_slots,
)
from core.quests.engine import QuestManager, give_reward
from core.quests.schema import (
    db_reset_all_user_quests,
    db_daily_quest_mark_claimed,
    db_daily_quest_find_unclaimed_by_reward_type,
    db_daily_quest_get_slots_for_user,
)
from core.quests.timekeys import daily_key, now_et, rollover_date
from core.constants import CURRENT_ACTIVE_SET, PACKS_BY_SET, TEAM_ROLE_NAMES, PACKS_IN_BOX
from core.currency import shard_set_name  # pretty name per set
from core.cards_shop import find_card_by_print_key, resolve_card_set, card_label
from core.db import db_add_cards
from core.packs import open_pack_from_csv, open_box_from_csv
from core.views import _pack_embed_for_cards
from cogs.packs import ac_pack_name_choices

logger = logging.getLogger(__name__)

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
        self._last_simulated_day: date | None = None

    @property
    def _et(self):
        return ZoneInfo("America/New_York")

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
        return suggest_prints_with_set(self.state, current, include_starters=True)
    
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

    async def _dm_pack_results(
        self,
        recipient: discord.abc.User,
        pack_name: str,
        per_pack: list[list[dict]],
    ) -> bool:
        try:
            dm = await recipient.create_dm()
            for i, cards in enumerate(per_pack, start=1):
                content, embeds, files = _pack_embed_for_cards(
                    self.bot, pack_name, cards, i, len(per_pack)
                )
                send_kwargs: dict = {"embeds": embeds}
                if content:
                    send_kwargs["content"] = content
                if files:
                    send_kwargs["files"] = files
                await dm.send(**send_kwargs)
                if len(per_pack) > 5:
                    await asyncio.sleep(0.2)
            return True
        except Exception:
            logger.warning("Failed to DM pack/box results", exc_info=True)
            return False

    async def _post_pack_results(
        self,
        destination: discord.abc.Messageable | None,
        pack_name: str,
        per_pack: list[list[dict]],
    ) -> None:
        if destination is None:
            return
        for i, cards in enumerate(per_pack, start=1):
            content, embeds, files = _pack_embed_for_cards(
                self.bot, pack_name, cards, i, len(per_pack)
            )
            send_kwargs: dict = {"embeds": embeds}
            if content:
                send_kwargs["content"] = content
            if files:
                send_kwargs["files"] = files
            try:
                await destination.send(**send_kwargs)
            except Exception:
                logger.warning("Failed to post pack/box results to channel", exc_info=True)
                break

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
    
    def _starter_member_ids(self) -> set[int]:
        ids: set[int] = set()
        for guild in self.bot.guilds:
            for role_name in TEAM_ROLE_NAMES:
                role = discord.utils.get(guild.roles, name=role_name)
                if role:
                    ids.update(m.id for m in role.members)
        return ids

    async def _resolve_user_and_roles(
        self, user_id: int, guild: discord.Guild | None
    ) -> tuple[discord.User | discord.Member | None, discord.Member | None, list[str]]:
        user = self.bot.get_user(user_id)
        if not user:
            try:
                user = await self.bot.fetch_user(user_id)
            except Exception:
                user = None

        member: discord.Member | None = None
        if guild:
            member = guild.get_member(user_id)
            if not member:
                try:
                    member = await guild.fetch_member(user_id)
                except Exception:
                    member = None

        roles = [r.name for r in getattr(member, "roles", []) if getattr(r, "name", None)]
        return user, member, roles

    @app_commands.command(name="admin_add_card", description="(Admin) Add a card to a user's collection (rarity from pack)")
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        user="User to modify (ignored if all_users=true)",
        card_name="Card to add (choose the exact printing)",
        qty="Quantity to add (default 1)",
        all_users="Add the card to all starter-role members (Fire/Water)",
    )
    @app_commands.autocomplete(card_name=ac_print)
    async def admin_add_card(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.User],
        card_name: str,
        qty: app_commands.Range[int, 1, 999] = 1,
        all_users: bool = False,
    ):
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

        if all_users:
            await interaction.response.defer(ephemeral=True, thinking=True)
            guild = interaction.guild
            if guild is None:
                await interaction.followup.send("❌ This command can only be used in a server.", ephemeral=True)
                return

            starter_members: set[discord.Member] = set()
            missing_roles: list[str] = []
            for role_name in TEAM_ROLE_NAMES:
                role = discord.utils.get(guild.roles, name=role_name)
                if not role:
                    missing_roles.append(role_name)
                    continue
                starter_members.update(role.members)

            if not starter_members:
                msg = "❌ No starter-role members (Fire/Water) found to update."
                if missing_roles:
                    msg += " Missing roles: " + ", ".join(sorted(missing_roles))
                await interaction.followup.send(msg, ephemeral=True)
                return

            for member in starter_members:
                db_admin_add_card(
                    self.bot.state,
                    member.id,
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
            summary = (
                f"✅ Added **x{qty}** of **{label}** to **{len(starter_members)}** starter members."
            )
            if missing_roles:
                summary += " Missing roles: " + ", ".join(sorted(missing_roles))

            await interaction.followup.send(summary, ephemeral=True)
            if interaction.channel:
                await interaction.channel.send(summary)
            return

        if user is None:
            await interaction.response.send_message(
                "❌ Please select a **user** or set **all_users** to true.",
                ephemeral=True,
            )
            return

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
        user="User to modify (ignored if all_users=true)",
        card_name="Card to remove (choose the exact printing)",
        qty="Quantity to remove (default 1)",
        all_users="Remove the card from all starter-role members (Fire/Water)",
    )
    @app_commands.autocomplete(card_name=ac_print)
    async def admin_remove_card(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.User],
        card_name: str,
        qty: app_commands.Range[int, 1, 999] = 1,
        all_users: bool = False,
    ):
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

        if all_users:
            await interaction.response.defer(ephemeral=True, thinking=True)
            guild = interaction.guild
            if guild is None:
                await interaction.followup.send("❌ This command can only be used in a server.", ephemeral=True)
                return

            starter_members: set[discord.Member] = set()
            missing_roles: list[str] = []
            for role_name in TEAM_ROLE_NAMES:
                role = discord.utils.get(guild.roles, name=role_name)
                if not role:
                    missing_roles.append(role_name)
                    continue
                starter_members.update(role.members)

            if not starter_members:
                msg = "❌ No starter-role members (Fire/Water) found to update."
                if missing_roles:
                    msg += " Missing roles: " + ", ".join(sorted(missing_roles))
                await interaction.followup.send(msg, ephemeral=True)
                return

            total_removed = 0
            affected_members = 0
            for member in starter_members:
                removed, _ = db_admin_remove_card(
                    self.bot.state,
                    member.id,
                    name=name,
                    rarity=rarity,
                    card_set=card_set,
                    card_code=card_code,
                    card_id=card_id,
                    qty=qty,
                )
                if removed:
                    total_removed += removed
                    affected_members += 1

            display_card = dict(card)
            display_card.setdefault("set", card_set)
            label = card_label(display_card)
            if total_removed == 0:
                summary = "ℹ️ No matching rows found for starter members."
            else:
                summary = (
                    f"🗑 Removed up to **x{qty}** of **{label}** from **{affected_members}** "
                    f"starter members (total removed: **{total_removed}**)."
                )
            if missing_roles:
                summary += " Missing roles: " + ", ".join(sorted(missing_roles))

            await interaction.followup.send(summary, ephemeral=True)
            if total_removed > 0 and interaction.channel:
                await interaction.channel.send(summary)
            return

        if user is None:
            await interaction.response.send_message(
                "❌ Please select a **user** or set **all_users** to true.",
                ephemeral=True,
            )
            return

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
        name="admin_award_pack",
        description="(Admin) Open packs for a user and DM them the pulls.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        user="User receiving the packs",
        pack_name="Pack set to open",
        amount="How many packs to open (1-100)",
    )
    @app_commands.autocomplete(pack_name=ac_pack_name_choices)
    async def admin_award_pack(
        self,
        interaction: discord.Interaction,
        user: discord.User,
        pack_name: str,
        amount: app_commands.Range[int, 1, 100] = 1,
    ):
        state = self.state
        normalized_pack = (pack_name or "").strip()
        if normalized_pack not in (state.packs_index or {}):
            await interaction.response.send_message("That pack set could not be found.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        per_pack: list[list[dict]] = []
        for _ in range(amount):
            per_pack.append(open_pack_from_csv(state, normalized_pack, 1))

        flat = [card for pack in per_pack for card in pack]
        db_add_cards(state, user.id, flat, normalized_pack)

        dm_sent = await self._dm_pack_results(user, normalized_pack, per_pack)

        summary = (
            f"{interaction.user.display_name} awarded **{amount}** pack{'s' if amount != 1 else ''}"
            f" of **{normalized_pack}** to {user.mention}."
            f"{' Results sent via DM.' if dm_sent else ' Could not DM results; posting here.'}"
        )

        await interaction.followup.send(summary, ephemeral=True)
        await interaction.channel.send(summary)

        if not dm_sent:
            await self._post_pack_results(interaction.channel, normalized_pack, per_pack)

    @app_commands.command(
        name="admin_award_box",
        description="(Admin) Open a sealed box for a user and DM them the pulls.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        user="User receiving the box",
        pack_name="Pack set to open",
        amount="How many boxes to open (1-5)",
    )
    @app_commands.autocomplete(pack_name=ac_pack_name_choices)
    async def admin_award_box(
        self,
        interaction: discord.Interaction,
        user: discord.User,
        pack_name: str,
        amount: app_commands.Range[int, 1, 5] = 1,
    ):
        state = self.state
        normalized_pack = (pack_name or "").strip()
        if normalized_pack not in (state.packs_index or {}):
            await interaction.response.send_message("That pack set could not be found.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        per_pack: list[list[dict]] = []
        for _ in range(amount):
            per_pack.extend(open_box_from_csv(state, normalized_pack))

        flat = [card for pack in per_pack for card in pack]
        db_add_cards(state, user.id, flat, normalized_pack)

        dm_sent = await self._dm_pack_results(user, normalized_pack, per_pack)

        summary = (
            f"{interaction.user.display_name} awarded **{amount}** box{'es' if amount != 1 else ''}"
            f" of **{normalized_pack}** to {user.mention} ({amount * PACKS_IN_BOX} packs)."
            f"{' Results sent via DM.' if dm_sent else ' Could not DM results; posting here.'}"
        )

        await interaction.followup.send(summary, ephemeral=True)
        await interaction.channel.send(summary)

        if not dm_sent:
            await self._post_pack_results(interaction.channel, normalized_pack, per_pack)

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
        starter_claims_removed = db_starter_claim_clear(self.state, user.id)
        set_wins_removed = db_user_set_wins_clear(self.state, user.id)

        # Clear stored team points within this guild (if any)
        team_points_removed = 0
        if interaction.guild:
            team_points_removed = db_team_points_clear(self.state, interaction.guild.id, user.id)

        battleground_points_removed = 0
        if interaction.guild:
            battleground_rows = db_team_battleground_user_points_for_user_all_sets(
                self.state, interaction.guild.id, user.id
            )
            for row in battleground_rows:
                db_team_battleground_totals_update(
                    self.state,
                    interaction.guild.id,
                    int(row.get("set_id") or 0),
                    str(row.get("team") or ""),
                    duel_delta=-int(row.get("net_points") or 0),
                    bonus_delta=-int(row.get("bonus_points") or 0),
                )
            for set_id in {int(row.get("set_id") or 0) for row in battleground_rows}:
                battleground_points_removed += db_team_battleground_user_points_clear(
                    self.state,
                    interaction.guild.id,
                    int(set_id),
                    user.id,
                )

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
        if starter_claims_removed:
            lines.append("✅ Cleared the starter claim guard; the user can run /start again.")
        else:
            lines.append("ℹ️ Starter claim guard was already clear.")
        if set_wins_removed:
            lines.append(f"✅ Cleared per-set win tracking entries (**{set_wins_removed}** removed).")
        else:
            lines.append("ℹ️ No per-set win tracking entries found to clear.")
        if interaction.guild and team_points_removed:
            lines.append(f"✅ Cleared team territory entries ({team_points_removed} row(s)).")
        if battleground_points_removed:
            lines.append(f"✅ Cleared battleground territory entries ({battleground_points_removed} row(s)).")
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
        name="get_team_points",
        description="(Admin) View battleground team territory for the active set.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        team="Optional team name to filter by",
        user="Optional member to filter by",
    )
    async def get_team_points(
        self,
        interaction: discord.Interaction,
        team: str | None = None,
        user: discord.Member | None = None,
    ):
        if not interaction.guild:
            await interaction.response.send_message(
                "This command can only be used in a server.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        team_filter = (team or "").strip() or None
        user_filter_id = user.id if user else None

        set_id = CURRENT_ACTIVE_SET
        rows = db_team_battleground_user_points_all(
            self.state,
            interaction.guild.id,
            set_id,
            team_filter,
            user_filter_id,
        )

        if user_filter_id is not None and not rows:
            await interaction.followup.send(
                "No team territory found for that member.",
                ephemeral=True,
            )
            return

        if not rows:
            await interaction.followup.send(
                "No team territory entries found for the requested filters.",
                ephemeral=True,
            )
            return

        lines = []
        for info in sorted(
            rows, key=lambda item: (-int(item.get("earned_points", 0)), int(item.get("user_id", 0)))
        ):
            user_id = int(info.get("user_id") or 0)
            member = interaction.guild.get_member(user_id)
            name = member.mention if member else f"User {user_id}"
            team_label = info.get("team") or "Unassigned"
            earned_points = int(info.get("earned_points") or 0)
            net_points = int(info.get("net_points") or 0)
            bonus_points = int(info.get("bonus_points") or 0)
            lines.append(
                f"{name} — Team: **{team_label}**, Territory claimed: **{earned_points:,}**, "
                f"Net territory: **{net_points:,}**, Bonus territory: **{bonus_points:,}**"
            )

        if user_filter_id is None:
            totals = db_team_battleground_totals_get(
                self.state,
                interaction.guild.id,
                set_id,
            )
            team_lines = ["", "Total Territory Controlled"]
            for team_label, info in sorted(
                totals.items(), key=lambda item: (-(int(item[1].get("duel_points", 0)) + int(item[1].get("bonus_points", 0))), item[0].lower())
            ):
                duel_points = int(info.get("duel_points", 0))
                bonus_points = int(info.get("bonus_points", 0))
                total_points = duel_points + bonus_points
                team_lines.append(
                    f"• Team **{team_label}**: Territory controlled: **{total_points:,}** "
                    f"(duel: {duel_points:,}, bonus: {bonus_points:,})"
                )

            lines.extend(team_lines)

        embed = discord.Embed(
            title="Team Territory Overview",
            description="\n".join(lines),
            color=discord.Color.blurple(),
        )
        if team_filter:
            embed.add_field(name="Team filter", value=team_filter, inline=True)
        if user:
            embed.add_field(name="User filter", value=user.mention, inline=True)

        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(
        name="get_poors",
        description="(Admin) List Fire/Water members with few secret rare cards.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        amount_secrets="Maximum number of secret rares a member can have to be listed (default: 1)",
        amount_ultras="Maximum number of ultra rares a member can have to be listed (default: 1)",
    )
    async def get_poors(
        self,
        interaction: discord.Interaction,
        amount_secrets: int | None = 1,
        amount_ultras: int | None = 1,
    ) -> None:
        if not interaction.guild:
            await interaction.response.send_message(
                "This command can only be used in a server.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        max_secrets = max(0, int(amount_secrets if amount_secrets is not None else 1))
        max_ultras = max(0, int(amount_ultras if amount_ultras is not None else 1))

        guild = interaction.guild
        target_roles = {"Fire", "Water"}
        members: dict[int, discord.Member] = {}
        missing_roles: list[str] = []

        for role_name in sorted(target_roles):
            role = discord.utils.get(guild.roles, name=role_name)
            if not role:
                missing_roles.append(role_name)
                continue
            for member in role.members:
                members[member.id] = member

        if not members:
            msg = "No Fire/Water role members found in this server."
            if missing_roles:
                msg += " Missing roles: " + ", ".join(sorted(missing_roles))
            await interaction.followup.send(msg, ephemeral=True)
            return

        qualifying: list[tuple[discord.Member, int, int]] = []
        for member in members.values():
            total_secret = db_collection_total_by_rarity(self.state, member.id, "secret")
            total_ultra = db_collection_total_by_rarity(self.state, member.id, "ultra")
            if total_secret <= max_secrets and total_ultra <= max_ultras:
                qualifying.append((member, total_secret, total_ultra))

        if not qualifying:
            msg = (
                "No Fire/Water members have secret rare totals at or below "
                f"**{max_secrets}** and ultra rare totals at or below **{max_ultras}**."
            )
            if missing_roles:
                msg += " Missing roles: " + ", ".join(sorted(missing_roles))
            await interaction.followup.send(msg, ephemeral=True)
            return

        qualifying.sort(key=lambda pair: (pair[1], pair[2], pair[0].display_name.lower()))

        lines = [
            (
                f"Fire/Water members with ≤ **{max_secrets}** secret rare(s) and "
                f"≤ **{max_ultras}** ultra rare(s):"
            ),
        ]
        for member, total_secret, total_ultra in qualifying:
            lines.append(
                (
                    f"• {member.mention} — **{total_secret}** secret rare(s), "
                    f"**{total_ultra}** ultra rare(s) owned"
                )
            )

        if missing_roles:
            lines.append("")
            lines.append("Missing roles: " + ", ".join(sorted(missing_roles)))

        await interaction.followup.send("\n".join(lines), ephemeral=True)

    @app_commands.command(
        name="admin_reset_user_quests",
        description="(Admin) Clear all quest progress/claims for a user without touching anything else.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(user="Member to reset", reason="Optional reason")
    async def admin_reset_user_quests(
        self, interaction: discord.Interaction, user: discord.Member, reason: str | None = None
    ):
        if user.bot:
            await interaction.response.send_message("You can’t reset a bot account.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        cleared = await db_reset_all_user_quests(self.state, user.id)
        reason_suffix = f" Reason: {reason}" if reason else ""
        await interaction.followup.send(
            (
                f"✅ Cleared quest progress/claims for **{user.display_name}** "
                f"({cleared} rows).{reason_suffix}"
            ),
            ephemeral=True,
        )

    @app_commands.command(
        name="admin_daily_duel_status",
        description="(Admin) Inspect daily duel rollover slots for a user.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(user="Member to inspect")
    async def admin_daily_duel_status(
        self, interaction: discord.Interaction, user: discord.Member
    ) -> None:
        """Show rollover slot state to debug missing queued rewards."""

        await interaction.response.defer(ephemeral=True, thinking=True)

        quests_cog = interaction.client.get_cog("Quests")
        if not quests_cog or not getattr(quests_cog, "qm", None):
            await interaction.followup.send("⚠️ Quests cog not loaded.", ephemeral=True)
            return

        qm = quests_cog.qm
        await qm._refresh_defs_if_needed(rollover_date())

        daily_rollover_quests = [
            q for q in qm._defs.values() if q.category == "daily" and q.max_rollover_days > 0
        ]
        if not daily_rollover_quests:
            await interaction.followup.send(
                "ℹ️ No rollover-enabled daily quests are configured.", ephemeral=True
            )
            return

        lines: list[str] = []
        for q in daily_rollover_quests:
            slots = await qm._ensure_daily_rollover_slots(user.id, q)
            max_target = max(int(s.get("target_count", q.target_count) or 0) for s in slots)
            pending = [s for s in slots if not s.get("claimed_at")]
            claimables = [s for s in pending if int(s.get("progress", 0)) >= max_target]

            lines.append(
                f"**{q.title}** — {len(pending)} pending / {len(claimables)} claimable"
            )

            for slot in slots[-10:]:
                target = int(slot.get("target_count", max_target) or max_target)
                progress = int(slot.get("progress", 0))
                claimed = bool(slot.get("claimed_at"))
                completed = progress >= target
                flags = []
                if slot.get("auto_granted_at"):
                    flags.append("auto")
                status = "✅ claimed" if claimed else ("🏁 ready" if completed else "…")
                if flags:
                    status += " (" + ", ".join(flags) + ")"
                lines.append(f"• {slot['day_key']}: {progress}/{target} — {status}")

            if len(slots) > 10:
                lines.append(f"(showing latest 10 of {len(slots)} slots)")

        await interaction.followup.send("\n".join(lines), ephemeral=True)
    
    @app_commands.command(
        name="admin_daily_quest_slots",
        description="(Admin) View raw daily quest slots and claim status for a user.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        user="Member to inspect",
        quest_id="Optional quest ID filter (leave blank for all daily quests)",
    )
    async def admin_daily_quest_slots(
        self, interaction: discord.Interaction, user: discord.Member, quest_id: str | None = None
    ) -> None:
        """Show each queued/completed/claimed daily quest slot for debugging rollovers."""

        await interaction.response.defer(ephemeral=True, thinking=True)

        slots = await db_daily_quest_get_slots_for_user(self.state, user.id, quest_id)
        if not slots:
            await interaction.followup.send(
                "No daily quest slots found for that user." + (" (quest filter applied)" if quest_id else ""),
                ephemeral=True,
            )
            return

        grouped: dict[str, list[dict]] = {}
        for slot in slots:
            grouped.setdefault(slot.get("quest_id") or "?", []).append(slot)

        lines: list[str] = [
            f"Daily quest slots for **{user.display_name}**" + (f" (quest `{quest_id}`)" if quest_id else "")
        ]

        for qid, quest_slots in grouped.items():
            lines.append(f"\n**{qid}** — {len(quest_slots)} slot(s)")

            display_slots = quest_slots[-25:]
            for slot in display_slots:
                target = int(slot.get("target_count") or 0)
                progress = int(slot.get("progress") or 0)

                if slot.get("claimed_at"):
                    status = "✅ claimed"
                elif progress >= max(1, target):
                    status = "🏁 ready"
                else:
                    status = "…"

                flags: list[str] = []
                if slot.get("auto_granted_at"):
                    flags.append("auto")
                if flags:
                    status += " (" + ", ".join(flags) + ")"

                reward_type = (slot.get("reward_type") or "?").lower()
                lines.append(
                    f"• {slot['day_key']}: {progress}/{max(1, target)} — {status} [{reward_type}]"
                )

            if len(quest_slots) > len(display_slots):
                lines.append(f"(showing latest {len(display_slots)} of {len(quest_slots)} slots)")

        await interaction.followup.send("\n".join(lines), ephemeral=True)

    @app_commands.command(
        name="award_missed_pack_quests",
        description="(Admin) DM and award all unclaimed pack-type daily quest rewards.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def award_missed_pack_quests(self, interaction: discord.Interaction) -> None:
        """Grant any queued pack daily rewards and clear their slots before week 2."""

        await interaction.response.defer(ephemeral=True, thinking=True)

        pending_slots = await db_daily_quest_find_unclaimed_by_reward_type(
            self.state, "pack"
        )
        if not pending_slots:
            await interaction.followup.send(
                "No unclaimed pack daily rewards found.", ephemeral=True
            )
            return

        qm = QuestManager(self.state)
        guild = interaction.guild or self.bot.get_guild(GUILD_ID)

        grouped: dict[int, list[dict]] = {}
        for slot in pending_slots:
            uid = int(slot.get("user_id") or 0)
            if not uid:
                continue
            grouped.setdefault(uid, []).append(slot)

        total_awarded = 0
        failures: list[str] = []
        summaries: list[str] = []

        for user_id, slots in grouped.items():
            user, _member, roles = await self._resolve_user_and_roles(user_id, guild)
            intro_sent = False

            for slot in slots:
                payload = qm._resolve_reward_payload_for_user(
                    slot.get("reward_payload") or {}, roles=roles
                )
                try:
                    if not intro_sent and user is not None:
                        try:
                            await user.send("You missed out on these daily rewards week 1:")
                        except Exception:
                            pass
                        intro_sent = True

                    ack = await give_reward(
                        self.state,
                        user_id,
                        slot.get("reward_type"),
                        payload,
                    )
                    await db_daily_quest_mark_claimed(
                        self.state, user_id, slot["quest_id"], slot["day_key"], auto=True
                    )
                    total_awarded += 1
                    summaries.append(
                        f"<@{user_id}> — {slot['day_key'].split(':')[-1]}: {ack}"
                    )
                except Exception as e:
                    failures.append(
                        f"<@{user_id}> {slot['day_key'].split(':')[-1]}: {e}"
                    )

        response_lines = [
            f"Awarded {total_awarded} pending pack daily reward(s) across {len(grouped)} user(s).",
            "Successful grants were marked claimed to clear queued rewards.",
        ]
        if summaries:
            response_lines.append("\n".join(summaries[:10]))
            if len(summaries) > 10:
                response_lines.append(f"…and {len(summaries) - 10} more")
        if failures:
            response_lines.append("⚠️ Failures:")
            response_lines.append("\n".join(failures[:10]))
            if len(failures) > 10:
                response_lines.append(f"…and {len(failures) - 10} more failures")

        await interaction.followup.send("\n".join(response_lines), ephemeral=True)

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
            set_id=CURRENT_ACTIVE_SET,
        )

        moved_points = 0
        team_message = None
        teams = interaction.client.get_cog("Teams")
        if interaction.guild and teams and hasattr(teams, "apply_duel_result"):
            try:
                moved_points, info = await teams.apply_duel_result(
                    interaction.guild,
                    winner=winner,
                    loser=loser,
                    winner_stats=winner_after,
                    loser_stats=loser_after,
                )
                winner_team = info.get("winner_team", "Unknown team")
                loser_team = info.get("loser_team", "Unknown team")
                winner_total = info.get("winner_total")
                team_message = (
                    f"{winner.display_name} claimed **{moved_points:,}** units of territory "
                    f"for the {winner_team} team."
                )
                if winner_total is not None:
                    team_message += f" Territory controlled: **{int(winner_total):,}**."
            except Exception as exc:
                print("[admin] failed to apply battleground points:", exc)
        if team_message is None:
            team_message = "Team territory could not be updated for this match."

        quests = interaction.client.get_cog("Quests")
        try:
            if quests and getattr(quests, "qm", None):
                await quests.qm.increment(loser.id, "matches_played", 1)
                await quests.qm.increment(winner.id, "matches_played", 2)
        except Exception as e:
            print("[admin] quest tick error during admin_report_loss:", e)

        embed = discord.Embed(
            title="Admin Match Recorded",
            description=(
                f"**{loser.display_name}** lost to **{winner.display_name}**.\n"
                f"{team_message}"
            ),
            color=0xCC3333,
        )

        await interaction.followup.send(embed=embed, ephemeral=True)
        if interaction.channel:
            await interaction.channel.send(
                f"📝 Admin recorded a result: **{loser.display_name}** lost to **{winner.display_name}**. "
                f"{team_message}"
            )

        # Remove the user(s) from the queue if there is an actively paired match
        queue = interaction.client.get_cog("DuelQueue")
        try:
            if queue and hasattr(queue, "clear_pairing"):
                await queue.clear_pairing(loser.id, winner.id)
        except Exception as e:
            print("[admin] failed to clear duel pairing:", e)


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
                await quests.qm.increment(winner.id, "matches_played", -2)
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

    @app_commands.command(
        name="admin_cancel_match",
        description="(Admin) Cancel a current duel pairing so players can requeue.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        player_a="First player in the pairing",
        player_b="Second player in the pairing",
    )
    async def admin_cancel_match(
        self,
        interaction: discord.Interaction,
        player_a: discord.Member,
        player_b: discord.Member,
    ) -> None:
        if player_a.id == player_b.id:
            await interaction.response.send_message(
                "You must choose two different players.", ephemeral=True
            )
            return

        queue = interaction.client.get_cog("DuelQueue")
        if queue is None or not hasattr(queue, "clear_pairing"):
            await interaction.response.send_message(
                "❌ Duel queue is unavailable.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            is_pair = False
            if hasattr(queue, "is_active_pair"):
                is_pair = await queue.is_active_pair(player_a.id, player_b.id)

            if not is_pair:
                await interaction.followup.send(
                    "❌ No active pairing found for those players.", ephemeral=True
                )
                return

            cleared = await queue.clear_pairing(player_a.id, player_b.id)
        except Exception:
            logger.warning("Failed to cancel duel pairing", exc_info=True)
            await interaction.followup.send(
                "❌ Failed to cancel the pairing due to an unexpected error.",
                ephemeral=True,
            )
            return

        if not cleared:
            await interaction.followup.send(
                "❌ No active pairing found for those players.", ephemeral=True
            )
            return

        await interaction.followup.send(
            f"✅ Cancelled the pairing between {player_a.mention} and {player_b.mention}.",
            ephemeral=True,
        )

        if interaction.channel:
            await interaction.channel.send(
                f"🚫 Admin cancelled the duel pairing between **{player_a.display_name}** and **{player_b.display_name}**."
            )

    # ---- Add currency -------------------------------------------------------
    @app_commands.command(name="wallet_add", description="(Admin) Add currency to a user's wallet")
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        user="Player to adjust (ignored if all_users=true)",
        currency="Choose Mambucks or Shards",
        amount="Amount to add (>=1)",
        shard_set="Required if currency=shards",
        all_users="Add the currency to all starter-role members (Fire/Water)",
    )
    @app_commands.autocomplete(shard_set=ac_shard_set)
    async def wallet_add(
        self,
        interaction: discord.Interaction,
        currency: Currency,
        amount: app_commands.Range[int, 1, None],
        user: Optional[discord.Member] = None,  
        shard_set: Optional[int] = None,
        all_users: bool = False,
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        if all_users:
            guild = interaction.guild
            if guild is None:
                return await interaction.followup.send(
                    "❌ This command can only be used in a guild.",
                    ephemeral=True,
                )

            starter_members = set()
            for role_name in TEAM_ROLE_NAMES:
                role = discord.utils.get(guild.roles, name=role_name)
                if role:
                    starter_members.update(role.members)

            if not starter_members:
                return await interaction.followup.send(
                    "❌ No starter-role members (Fire/Water) found to update.",
                    ephemeral=True,
                )

            if currency == "mambucks":
                for member in starter_members:
                    db_wallet_add(self.state, member.id, d_mambucks=amount)

                return await interaction.followup.send(
                    f"✅ Added **{amount} Mambucks** to **{len(starter_members)}** starter members.",
                    ephemeral=True,
                )

            if shard_set is None:
                return await interaction.followup.send(
                    "❌ Please choose a **shard_set** for shards.",
                    ephemeral=True,
                )

            for member in starter_members:
                db_shards_add(self.state, member.id, shard_set, amount)

            title = shard_set_name(shard_set)
            return await interaction.followup.send(
                (
                    f"✅ Added **{amount} {title}** to **{len(starter_members)}** members."
                ),
                ephemeral=True,
            )

        if user is None:
            return await interaction.followup.send(
                "❌ Please select a **user** or set **all_users** to true.",
                ephemeral=True,
            )

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

    # --- Admin: simulate next-day midnight rollover -------------------------
    @app_commands.command(
        name="admin_simulate_next_day",
        description="(Admin) Run midnight ET grants/rollovers early for testing.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def admin_simulate_next_day(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)

        def _as_date(key: str | None) -> date | None:
            try:
                return datetime.strptime(key or "", "%Y%m%d").date()
            except Exception:
                return None

        def _latest_day_from_db() -> tuple[list[date], list[str]]:
            """Look up last processed ET days recorded in the DB for rollovers.

            We consult the daily rewards totals (starter_daily_totals.last_day),
            the wheel_tokens table (max last_grant_day), and the daily_sales table
            (max day_key). Any missing tables simply yield no candidates.
            """

            import sqlite3

            candidates: list[date] = []
            notes: list[str] = []
            try:
                with sqlite3.connect(self.bot.state.db_path) as conn:
                    cur = conn.execute(
                        "SELECT last_day FROM starter_daily_totals WHERE id = 1"
                    )
                    row = cur.fetchone()
                    if row and row[0]:
                        d = _as_date(str(row[0]))
                        if d:
                            notes.append(f"daily totals last_day={row[0]}")
                            candidates.append(d)

                    cur = conn.execute(
                        "SELECT MAX(last_grant_day) FROM wheel_tokens WHERE last_grant_day IS NOT NULL"
                    )
                    row = cur.fetchone()
                    if row and row[0]:
                        d = _as_date(str(row[0]))
                        if d:
                            notes.append(f"wheel_tokens last_grant_day={row[0]}")
                            candidates.append(d)

                    cur = conn.execute(
                        "SELECT MAX(day_key) FROM daily_sales WHERE day_key IS NOT NULL"
                    )
                    row = cur.fetchone()
                    if row and row[0]:
                        d = _as_date(str(row[0]))
                        if d:
                            notes.append(f"daily_sales day_key={row[0]}")
                            candidates.append(d)
            except Exception as e:
                notes.append(f"db lookup error: {e}")

            return candidates, notes

        def _load_persisted_last_sim() -> tuple[list[date], list[str]]:
            """Return the last simulated day stored in sqlite (if any)."""

            import sqlite3

            stored: list[date] = []
            notes: list[str] = []
            try:
                with sqlite3.connect(self.bot.state.db_path) as conn:
                    cur = conn.execute(
                        "SELECT last_day FROM admin_sim_state WHERE id = 1"
                    )
                    row = cur.fetchone()
                    if row and row[0]:
                        d = _as_date(str(row[0]))
                        if d:
                            notes.append(f"admin_sim_state last_day={row[0]}")
                            stored.append(d)
            except Exception as e:
                notes.append(f"persisted sim lookup error: {e}")

            return stored, notes

        def _persist_last_sim(day_key: str):
            """Persist the most recent simulated day for future invocations."""

            import sqlite3

            try:
                with sqlite3.connect(self.bot.state.db_path) as conn, conn:
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS admin_sim_state (
                            id INTEGER PRIMARY KEY CHECK (id = 1),
                            last_day TEXT
                        );
                        """
                    )
                    conn.execute(
                        """
                        INSERT INTO admin_sim_state (id, last_day)
                        VALUES (1, ?)
                        ON CONFLICT(id) DO UPDATE SET last_day = excluded.last_day;
                        """,
                        (day_key,),
                    )
            except Exception:
                pass

        today_et = datetime.now(self._et).date()
        candidates: list[date] = []
        diag_sources: list[str] = []

        # Include previously simulated date (so we move forward from it)
        if self._last_simulated_day:
            candidates.append(self._last_simulated_day)
            diag_sources.append(f"in-memory sim day={self._last_simulated_day:%Y%m%d}")

        # Include the last day processed by each cog so we don't rerun the same day
        daily_cog = self.bot.get_cog("DailyRewards")
        if daily_cog:
            last_daily = _as_date(getattr(daily_cog, "_last_grant_day_key", None))
            if last_daily:
                candidates.append(last_daily)
                diag_sources.append(f"daily cog last_grant_day={last_daily:%Y%m%d}")

        gamba_cog = self.bot.get_cog("GambaChips")
        if gamba_cog:
            last_gamba = _as_date(getattr(gamba_cog, "_last_grant_day_key", None))
            if last_gamba:
                candidates.append(last_gamba)
                diag_sources.append(f"gamba cog last_grant_day={last_gamba:%Y%m%d}")

        sales_cog = self.bot.get_cog("Sales")
        if sales_cog:
            last_sales = _as_date(getattr(sales_cog, "_last_roll_day_key", None))
            if last_sales:
                candidates.append(last_sales)
                diag_sources.append(f"sales cog last_roll_day={last_sales:%Y%m%d}")

        # Include persisted DB state from the various rollovers for maximum coverage
        db_candidates, db_notes = _latest_day_from_db()
        candidates.extend(db_candidates)
        diag_sources.extend(db_notes)

        # Always include last simulated day stored in sqlite so we advance even after restarts
        persisted_days, persisted_notes = _load_persisted_last_sim()
        candidates.extend(persisted_days)
        diag_sources.extend(persisted_notes)

        base_date = max(candidates) if candidates else today_et
        target_date = base_date + timedelta(days=1)
        self._last_simulated_day = target_date

        
        day_key = target_date.strftime("%Y%m%d")
        quest_day_key = daily_key(target_date)

        _persist_last_sim(day_key)

        results: List[str] = []

        print(
            "[admin] simulate_next_day baseline:",
            ", ".join(diag_sources) or "(none)",
        )
        print(
            f"[admin] simulate_next_day advancing from {base_date:%Y%m%d} to {day_key}."
        )

        if daily_cog:
            await daily_cog.run_midnight_grant(day_key=day_key)
            results.append(f"✅ Starter daily rewards granted for **{day_key}**.")
        else:
            results.append("⚠️ Starter daily rewards cog not loaded.")

        if gamba_cog:
            await gamba_cog.run_midnight_grant(day_key=day_key)
            results.append(f"✅ Daily gamba chips granted for **{day_key}**.")
        else:
            results.append("⚠️ Gamba chips cog not loaded.")

        if sales_cog:
            await sales_cog.roll_for_day(day_key)
            results.append(f"✅ Sales rolled for **{day_key}** and banner refreshed.")
        else:
            results.append("⚠️ Sales cog not loaded.")

        quests_cog = self.bot.get_cog("Quests")
        starter_ids: set[int] = set()
        if quests_cog and hasattr(quests_cog, "_starter_member_ids"):
            try:
                starter_ids = set(quests_cog._starter_member_ids())
            except Exception:
                starter_ids = set()
        if not starter_ids:
            starter_ids = self._starter_member_ids()
        if quests_cog:
            advanced = await quests_cog.qm.fast_forward_daily_rollovers(
                target_date, include_user_ids=starter_ids
            )
            results.append(
                f"✅ Daily quest rollovers prepared for **{quest_day_key}** "
                f"({advanced} user/quest slot checks)."
            )
        else:
            results.append("⚠️ Quests cog not loaded.")

        await interaction.followup.send("\n".join(results), ephemeral=True)

    @app_commands.command(
        name="admin_reset_simulated_day",
        description="(Admin) Reset simulate-next-day tracking back to today (ET).",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def admin_reset_simulated_day(self, interaction: discord.Interaction):
        """Remove future-dated rollover markers so simulations don't skip days.

            Also clears any un/catch-up claim state for daily quests at or after the
            reset point so an admin can re-test reward flows without hitting
            "already claimed" responses.
            """

        await interaction.response.defer(ephemeral=True, thinking=True)

        def _clear_future_reward_history(today_key: str, quest_day_key: str):
            """Remove future-dated rollover markers so simulations don't skip days."""

            import sqlite3

            try:
                with sqlite3.connect(self.bot.state.db_path) as conn, conn:
                    # Daily mambucks: clear future markers for totals and per-user grants
                    conn.execute(
                        """
                        UPDATE starter_daily_totals
                           SET last_day = NULL
                         WHERE id = 1 AND last_day > ?;
                        """,
                        (today_key,),
                    )
                    conn.execute(
                        """
                        UPDATE starter_daily_grants
                           SET last_grant_day = NULL
                         WHERE last_grant_day > ?;
                        """,
                        (today_key,),
                    )

                    # Daily gamba chips: clear future markers so next grant applies
                    conn.execute(
                        """
                        UPDATE wheel_tokens
                           SET last_grant_day = NULL
                         WHERE last_grant_day > ?;
                        """,
                        (today_key,),
                    )

                    # Sales: drop any pre-rolled future banners
                    conn.execute(
                        "DELETE FROM daily_sales WHERE day_key > ?;",
                        (today_key,),
                    )

                    # Quest rollovers: drop any future-dated quest snapshots/slots
                    conn.execute(
                        "DELETE FROM daily_quest_days WHERE day_key > ?;",
                        (quest_day_key,),
                    )
                    conn.execute(
                        "DELETE FROM user_daily_quest_slots WHERE day_key > ?;",
                        (quest_day_key,),
                    )
                    conn.execute(
                        """
                        UPDATE user_daily_quest_slots
                           SET progress = 0,
                               completed_at = NULL,
                               claimed_at = NULL,
                               auto_granted_at = NULL
                         WHERE day_key >= ?;
                        """,
                        (quest_day_key,),
                    )
                    conn.execute(
                        "DELETE FROM user_quest_progress WHERE period_key >= ?;",
                        (quest_day_key,),
                    )
            except Exception:
                pass

        def _persist_last_sim(day_key: str):
            import sqlite3

            try:
                with sqlite3.connect(self.bot.state.db_path) as conn, conn:
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS admin_sim_state (
                            id INTEGER PRIMARY KEY CHECK (id = 1),
                            last_day TEXT
                        );
                        """
                    )
                    conn.execute(
                        """
                        INSERT INTO admin_sim_state (id, last_day)
                        VALUES (1, ?)
                        ON CONFLICT(id) DO UPDATE SET last_day = excluded.last_day;
                        """,
                        (day_key,),
                    )
            except Exception:
                pass

        today_et = datetime.now(self._et).date()
        today_key = today_et.strftime("%Y%m%d")
        quest_day_key = daily_key(today_et)

        self._last_simulated_day = today_et
        _clear_future_reward_history(today_key, quest_day_key)
        _persist_last_sim(today_key)

        daily_cog = self.bot.get_cog("DailyRewards")
        if daily_cog:
            daily_cog._last_grant_day_key = today_key

        gamba_cog = self.bot.get_cog("GambaChips")
        if gamba_cog:
            gamba_cog._last_grant_day_key = today_key

        sales_cog = self.bot.get_cog("Sales")
        if sales_cog:
            sales_cog._last_roll_day_key = today_key

        await interaction.followup.send(
            (
                "Simulated day reset to today. Next /admin_simulate_next_day will advance from here. "
                "Any future-dated rewards/rolls have been cleared so upcoming simulations rerun them from tomorrow."
            ),
            ephemeral=True,
        )
    
    @app_commands.command(
        name="admin_end_set",
        description="(Admin) Convert mambucks to shards for the active set and clear queued daily quests",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        mambuck_to_shards="Number of shards granted per mambuck when converting balances",
    )
    async def admin_end_set(
        self,
        interaction: discord.Interaction,
        mambuck_to_shards: app_commands.Range[int, 1, None],
    ):
        """Prepare for a new set by converting mambucks and clearing daily quest queues."""

        await interaction.response.defer(ephemeral=True, thinking=True)

        conversion = db_convert_all_mambucks_to_shards(
            self.state, CURRENT_ACTIVE_SET, int(mambuck_to_shards)
        )
        cleared_slots = db_clear_all_daily_quest_slots(self.state)

        shard_title = shard_set_name(CURRENT_ACTIVE_SET)
        converted_users = int(conversion.get("users", 0))
        total_mambucks = int(conversion.get("total_mambucks", 0))
        total_shards = int(conversion.get("total_shards", 0))

        if converted_users == 0 or total_mambucks == 0:
            conversion_line = (
                f"ℹ️ No mambuck balances needed conversion. Active shard type: **{shard_title}**."
            )
        else:
            conversion_line = (
                f"✅ Converted **{total_mambucks}** mambucks from **{converted_users}** user(s) "
                f"into **{total_shards} {shard_title}** at **1 → {int(mambuck_to_shards)}**."
            )

        quest_line = f"🧹 Cleared **{cleared_slots}** queued daily quest entr{'y' if cleared_slots == 1 else 'ies'}."

        await interaction.followup.send("\n".join([conversion_line, quest_line]), ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(Admin(bot))
