import discord, os
from discord.ext import commands
from discord import app_commands
from typing import List, Literal
from core.packs import resolve_card_in_pack

from core.db import db_admin_add_card, db_admin_remove_card, db_collection_clear, db_wallet_set, db_wallet_add, db_wallet_get

# Set guild ID for development
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None
STARTER_ROLE_NAME = "starter"
Currency = Literal["fitzcoin", "mambucks"]

def _ac_pack_names(state, prefix: str) -> List[str]:
    prefix = (prefix or "").lower()
    names = sorted((state.packs_index or {}).keys())
    return [n for n in names if prefix in n.lower()][:25] if prefix else names[:25]

def _ac_card_names_for_set(state, card_set: str, prefix: str) -> List[str]:
    prefix = (prefix or "").lower()
    candidates = set()
    if card_set and state.packs_index and card_set in state.packs_index:
        for items in state.packs_index[card_set]["by_rarity"].values():
            for it in items:
                candidates.add(it["name"])
    else:
        for p in (state.packs_index or {}).values():
            for items in p["by_rarity"].values():
                for it in items:
                    candidates.add(it["name"])
    names = sorted(candidates)
    return [n for n in names if prefix in n.lower()][:25] if prefix else names[:25]

def _read_option(interaction: discord.Interaction, name: str) -> str:
    data = getattr(interaction, "data", {}) or {}
    opts = {opt.get("name"): opt.get("value") for opt in data.get("options", []) if isinstance(opt, dict)}
    return (opts.get(name) or "").strip()

class Admin(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state = bot.state

    async def ac_card_set(self, interaction: discord.Interaction, current: str):
        return [app_commands.Choice(name=n, value=n) for n in _ac_pack_names(self.bot.state, current)]

    async def ac_card_name(self, interaction: discord.Interaction, current: str):
        selected_set = _read_option(interaction, "card_set") or _read_option(interaction, "cardset")
        names = _ac_card_names_for_set(self.bot.state, selected_set, current)
        return [app_commands.Choice(name=n, value=n) for n in names]

    @app_commands.command(name="admin_add_card", description="(Admin) Add a card to a user's collection (rarity from pack)")
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        user="User to modify",
        card_set="Set/pack name",
        card_name="Card name",
        qty="Quantity to add (default 1)",
        card_code="Card code (optional; narrows if duplicates)",
        card_id="Card id (optional; narrows if duplicates)",
    )
    @app_commands.autocomplete(card_set=ac_card_set, card_name=ac_card_name)
    async def admin_add_card(self, interaction: discord.Interaction,
                             user: discord.User, card_set: str, card_name: str,
                             qty: app_commands.Range[int,1,999]=1,
                             card_code: str="", card_id: str=""):
        try:
            item = resolve_card_in_pack(self.bot.state, card_set, card_name, card_code, card_id)
        except Exception as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True); return
        rarity = item.get("rarity","")
        new_total = db_admin_add_card(self.bot.state, user.id,
                                      name=card_name, rarity=rarity, card_set=card_set,
                                      card_code=item.get("card_code",""), card_id=item.get("card_id",""),
                                      qty=qty)
        await interaction.response.send_message(
            f"✅ Added **x{qty}** of **{card_name}** *(rarity: {rarity}, set: {card_set})* "
            f"to {user.mention}. New total: **{new_total}**.", ephemeral=True)
        await interaction.channel.send(
            f"📦 **{interaction.user.display_name}** added x{qty} **{card_name}** to **{user.display_name}**'s collection."
        )

    @app_commands.command(name="admin_remove_card", description="(Admin) Remove a card row (rarity from pack)")
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        user="User to modify",
        card_set="Set/pack name",
        card_name="Card name",
        qty="Quantity to remove (default 1)",
        card_code="Card code (optional; narrows if duplicates)",
        card_id="Card id (optional; narrows if duplicates)",
    )
    @app_commands.autocomplete(card_set=ac_card_set, card_name=ac_card_name)
    async def admin_remove_card(self, interaction: discord.Interaction,
                                user: discord.User, card_set: str, card_name: str,
                                qty: app_commands.Range[int,1,999]=1,
                                card_code: str="", card_id: str=""):
        try:
            item = resolve_card_in_pack(self.bot.state, card_set, card_name, card_code, card_id)
        except Exception as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True); return
        rarity = item.get("rarity","")
        removed, remaining = db_admin_remove_card(self.bot.state, user.id,
                                                  name=card_name, rarity=rarity, card_set=card_set,
                                                  card_code=item.get("card_code",""), card_id=item.get("card_id",""),
                                                  qty=qty)
        if removed == 0:
            await interaction.response.send_message("ℹ️ No matching row for that card.", ephemeral=True); return
        if remaining > 0:
            await interaction.response.send_message(
                f"✅ Removed **x{removed}** of **{card_name}** *(rarity: {rarity}, set: {card_set})* "
                f"from {user.mention}. Remaining: **{remaining}**.", ephemeral=True)
        else:
            await interaction.response.send_message(
                f"✅ Removed **x{removed}**; that row is now gone from {user.mention}'s collection.",
                ephemeral=True)
        await interaction.channel.send(
            f"🗑 **{interaction.user.display_name}** removed x{removed} **{card_name}** from **{user.display_name}**'s collection."
        )

    @app_commands.command(
    name="admin_reset_user",
    description="(Admin) Clear a user's collection and remove their starter role."
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

        # Clear collection
        deleted = db_collection_clear(self.state, user.id)

        # Empty wallet
        db_wallet_set(self.state, user.id, fitzcoin=0, mambucks=0)

        # Remove starter role (if present)
        removed_role = False
        role = discord.utils.get(interaction.guild.roles, name=STARTER_ROLE_NAME) if interaction.guild else None
        if role and role in user.roles:
            try:
                await user.remove_roles(role, reason=reason or "Admin reset user")
                removed_role = True
            except discord.Forbidden:
                pass  # Manage Roles / hierarchy issue

        # Respond
        lines = [f"✅ Cleared **{deleted}** row(s) for {user.mention}."]
        if role:
            lines.append("✅ Starter role removed." if removed_role else "⚠️ Could not remove starter role (permissions/position).")
        else:
            lines.append("ℹ️ Starter role not found in this server.")
        if reason:
            lines.append(f"📝 Reason: {reason}")

        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    # ---- Add currency -------------------------------------------------------
    @app_commands.command(name="wallet_add", description="Admin: add currency to a user's wallet")
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(user="Player to adjust", currency="Currency", amount="Amount to add (>=1)")
    @app_commands.checks.has_permissions(administrator=True)  # runtime guard
    async def wallet_add(
        self,
        interaction: discord.Interaction,
        user: discord.Member,
        currency: Currency,
        amount: app_commands.Range[int, 1, None],
    ):
        # ⬇️ Defer IMMEDIATELY — no awaits before this
        await interaction.response.defer(ephemeral=True, thinking=True)

        before = db_wallet_get(self.state, user.id)
        if currency == "fitzcoin":
            after = db_wallet_add(self.state, user.id, d_fitzcoin=amount)
        else:
            after = db_wallet_add(self.state, user.id, d_mambucks=amount)

        await interaction.followup.send(
            (
                f"✅ Added **{amount} {currency}** to {user.mention}.\n"
                f"Before → fitzcoin **{before['fitzcoin']}**, mambucks **{before['mambucks']}**\n"
                f"After  → fitzcoin **{after['fitzcoin']}**, mambucks **{after['mambucks']}**"
            ),
            ephemeral=True,
        )

    # ---- Remove currency -------------------------------------------------------
    @app_commands.command(name="wallet_remove", description="Admin: remove currency from a user's wallet")
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(user="Player to adjust", currency="Currency", amount="Amount to remove (>=1)")
    @app_commands.checks.has_permissions(administrator=True)  # runtime guard
    async def wallet_remove(
        self,
        interaction: discord.Interaction,
        user: discord.Member,
        currency: Currency,
        amount: app_commands.Range[int, 1, None],
    ):
        # ⬇️ Defer IMMEDIATELY
        await interaction.response.defer(ephemeral=True, thinking=True)

        before = db_wallet_get(self.state, user.id)
        if currency == "fitzcoin":
            new_f = max(0, before["fitzcoin"] - amount)
            db_wallet_set(self.state, user.id, fitzcoin=new_f)
        else:
            new_m = max(0, before["mambucks"] - amount)
            db_wallet_set(self.state, user.id, mambucks=new_m)
        after = db_wallet_get(self.state, user.id)

        await interaction.followup.send(
            (
                f"🧹 Removed **{amount} {currency}** from {user.mention}.\n"
                f"Before → fitzcoin **{before['fitzcoin']}**, mambucks **{before['mambucks']}**\n"
                f"After  → fitzcoin **{after['fitzcoin']}**, mambucks **{after['mambucks']}**"
            ),
            ephemeral=True,
        )

async def setup(bot: commands.Bot):
    await bot.add_cog(Admin(bot))
