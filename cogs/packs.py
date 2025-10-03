import asyncio
import csv
import os
from pathlib import Path
from typing import Dict, Iterable, List

import discord
import requests
from discord import app_commands
from discord.ext import commands

from core.constants import (
    BOX_COST,
    PACK_COST,
    PACKS_IN_BOX,
    NEMESES_BUNDLE_NAME,
)
from core.views import PacksSelectView
from core.db import db_add_cards
from core.packs import open_box_from_csv

# API and pack csv details
YGOPRO_API_URL = "https://db.ygoprodeck.com/api/v7/cardinfo.php"
RARITY_CHOICES: List[app_commands.Choice[str]] = [
    app_commands.Choice(name="Common", value="Common"),
    app_commands.Choice(name="Rare", value="Rare"),
    app_commands.Choice(name="Super Rare", value="Super Rare"),
    app_commands.Choice(name="Ultra Rare", value="Ultra Rare"),
    app_commands.Choice(name="Secret Rare", value="Secret Rare"),
]
CSV_FIELDS = [
    "cardname",
    "cardq",
    "cardrarity",
    "card_edition",
    "cardset",
    "cardcode",
    "cardid",
    "print_id",
]

# Set guild ID for development
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None
MAX_PACKS = 100
MIN_PACKS = 1

async def ac_pack_name_choices(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    state = interaction.client.state
    names = sorted((state.packs_index or {}).keys())
    bundle_name = NEMESES_BUNDLE_NAME
    lowered = bundle_name.lower()
    if bundle_name and all(name.lower() != lowered for name in names):
        names.insert(0, bundle_name)
    cur = (current or "").lower()
    out: List[app_commands.Choice[str]] = []
    for name in names:
        if cur and cur not in name.lower():
            continue
        trimmed = name[:100]
        out.append(app_commands.Choice(name=trimmed, value=trimmed))
        if len(out) >= 25:
            break
    return out

class Packs(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state = bot.state

    # ------------------------------ utilities ------------------------------
    def _packs_dir(self) -> Path:
        base = Path(self.state.packs_dir)
        if not base.is_absolute():
            root = Path(__file__).resolve().parents[1]
            base = root / base
        return base

    def _resolve_pack_csv(self, filename: str) -> Path:
        base = self._packs_dir().resolve()
        if not filename.lower().endswith(".csv"):
            filename = f"{filename}.csv"
        candidate = (base / filename).resolve()
        if base not in candidate.parents and candidate != base:
            raise ValueError("Invalid pack CSV path.")
        candidate.parent.mkdir(parents=True, exist_ok=True)
        return candidate

    async def ac_pack_csv(self, interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
        base = self._packs_dir()
        if not base.exists():
            return []
        cur = (current or "").lower()
        choices: List[app_commands.Choice[str]] = []
        for path in sorted(base.glob("*.csv")):
            name = path.name
            if cur and cur not in name.lower():
                continue
            choices.append(app_commands.Choice(name=name[:100], value=name))
            if len(choices) >= 25:
                break
        return choices

    @staticmethod
    def _parse_ydk_ids(content: str) -> List[int]:
        ids: List[int] = []
        seen: set[int] = set()
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("!"):
                continue
            if not line.isdigit():
                continue
            cid = int(line)
            if cid in seen:
                continue
            seen.add(cid)
            ids.append(cid)
        return ids

    @staticmethod
    def _fetch_cards_for_ids(ids: Iterable[int]) -> Dict[int, dict]:
        ids = [int(x) for x in ids if isinstance(x, (int, str)) and str(x).isdigit()]
        out: Dict[int, dict] = {}
        if not ids:
            return out
        BATCH = 40
        for idx in range(0, len(ids), BATCH):
            chunk = ids[idx:idx + BATCH]
            try:
                resp = requests.get(YGOPRO_API_URL, params={"id": ",".join(map(str, chunk))}, timeout=20)
                resp.raise_for_status()
                payload = resp.json()
            except requests.RequestException as exc:
                raise RuntimeError(f"YGOPRODeck request failed for ids {chunk}: {exc}") from exc
            for entry in (payload.get("data") or []):
                cid = entry.get("id")
                if cid is None:
                    continue
                out[int(cid)] = entry
        return out

    @staticmethod
    def _read_existing_keys(path: Path) -> set[tuple[str, str, str]]:
        keys: set[tuple[str, str, str]] = set()
        if not path.exists():
            return keys
        try:
            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    card_id = (row.get("cardid") or "").strip()
                    print_id = (row.get("print_id") or "").strip()
                    card_set = (row.get("cardset") or "").strip()
                    keys.add((card_id, print_id, card_set))
        except Exception:
            # If existing file can't be read, ignore duplicate tracking
            return set()
        return keys

    @app_commands.command(name="packlist", description="List available pack types")
    @app_commands.guilds(GUILD)
    async def packlist(self, interaction: discord.Interaction):
        names = sorted((self.bot.state.packs_index or {}).keys())
        if not names:
            await interaction.response.send_message("No packs found. Load CSVs and /reload_data.", ephemeral=True); return
        desc = "\n".join(f"• `{n}`" for n in names[:25])
        await interaction.response.send_message(embed=discord.Embed(title="Available Packs", description=desc, color=0x2b6cb0), ephemeral=True)

    @app_commands.command(name="pack", description="Open packs via dropdown")
    @app_commands.guilds(GUILD)
    @app_commands.describe(amount="How many packs (1-100)")
    async def pack(self, interaction: discord.Interaction, amount: app_commands.Range[int,MIN_PACKS,MAX_PACKS]=1):
        if not self.bot.state.packs_index:
            await interaction.response.send_message("No packs found. Load CSVs and /reload_data.", ephemeral=True); return
        view = PacksSelectView(self.bot.state, requester=interaction.user, amount=amount)
        await interaction.response.send_message("Pick a pack from the dropdown:", view=view, ephemeral=True)

    @app_commands.command(name="box", description=f"Open a sealed box or box bundle.")
    @app_commands.guilds(GUILD)
    async def box(self, interaction: discord.Interaction):
        import inspect
        print("PacksSelectView from:", PacksSelectView.__module__)
        print("Ctor:", inspect.signature(PacksSelectView.__init__))
        view = PacksSelectView(self.bot.state, requester=interaction.user, amount=PACKS_IN_BOX, mode="box")
        await interaction.response.send_message(
            "Pick a pack set for your **box**:", view=view, ephemeral=True
        )
    
    @app_commands.command(
        name="quick_box",
        description="Admin: instantly open a sealed box without rendering pack messages.",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(
        pack_name="Pack set to open",
        amount="How many boxes to open (1-10)",
    )
    @app_commands.autocomplete(pack_name=ac_pack_name_choices)
    async def quick_box(
        self,
        interaction: discord.Interaction,
        pack_name: str,
        amount: app_commands.Range[int, 1, 10] = 1,
    ):
        state = self.bot.state
        if not (state.packs_index or {}):
            await interaction.response.send_message(
                "No packs found. Load CSVs and /reload_data.",
                ephemeral=True,
            )
            return

        normalized_input = (pack_name or "").strip()
        is_bundle = normalized_input.lower() == NEMESES_BUNDLE_NAME.lower()
        if not is_bundle and pack_name not in state.packs_index:
            await interaction.response.send_message(
                "That pack set could not be found.", ephemeral=True
            )
            return

        if is_bundle and not state.packs_index:
            await interaction.response.send_message(
                "No packs available for the Nemeses Bundle.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            quests_cog = interaction.client.get_cog("Quests")
            if is_bundle:
                pack_names = sorted(state.packs_index.keys())
                total_packs_opened = 0
                for _ in range(amount):
                    for bundle_pack in pack_names:
                        per_pack = open_box_from_csv(state, bundle_pack)
                        total_packs_opened += len(per_pack)
                        flat = [card for pack in per_pack for card in pack]
                        db_add_cards(state, interaction.user.id, flat, bundle_pack)

                if quests_cog:
                    await quests_cog.tick_pack_open(
                        user_id=interaction.user.id,
                        amount=total_packs_opened,
                    )
            else:
                per_pack: list[list[dict]] = []
                for _ in range(amount):
                    per_pack.extend(open_box_from_csv(state, pack_name))

                flat = [card for pack in per_pack for card in pack]
                db_add_cards(state, interaction.user.id, flat, pack_name)

                if quests_cog:
                    await quests_cog.tick_pack_open(
                        user_id=interaction.user.id,
                        amount=PACKS_IN_BOX * amount,
                    )
        except Exception:
            await interaction.followup.send(
                "⚠️ Something went wrong opening those quick boxes.",
                ephemeral=True,
            )
            raise

        if is_bundle:
            bundle_suffix = "bundle" if amount == 1 else "bundles"
            pack_list = ", ".join(sorted(state.packs_index.keys()))
            await interaction.followup.send(
                f"Opened {amount} quick Nemeses {bundle_suffix}."
                f" Each bundle includes one box of: {pack_list}.",
                ephemeral=True,
            )
        else:
            suffix = "box" if amount == 1 else "boxes"
            await interaction.followup.send(
                f"Opened {amount} quick {suffix} of {pack_name}, check your collection to see the results",
                ephemeral=True,
            )

    @app_commands.command(
        name="cardpool_import",
        description="Import cards from a YDK file into a pack CSV",
    )
    @app_commands.guilds(GUILD)
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(
        ydk_file="YDK deck list file to import",
        cardset="Pack name to assign to the imported cards",
        cardrarity="Rarity to assign to the imported cards",
        pack_csv="Which pack CSV file to append the cards to",
    )
    @app_commands.choices(cardrarity=RARITY_CHOICES)
    @app_commands.autocomplete(pack_csv=ac_pack_csv)
    async def cardpool_import(
        self,
        interaction: discord.Interaction,
        ydk_file: discord.Attachment,
        cardset: str,
        cardrarity: app_commands.Choice[str],
        pack_csv: str,
    ):
        if not ydk_file.filename or not ydk_file.filename.lower().endswith(".ydk"):
            await interaction.response.send_message("Please upload a valid .ydk file.", ephemeral=True)
            return

        try:
            raw_bytes = await ydk_file.read()
            text = raw_bytes.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = raw_bytes.decode("utf-8", errors="ignore")
        except Exception as exc:
            await interaction.response.send_message(f"Failed to read uploaded file: {exc}", ephemeral=True)
            return

        card_ids = self._parse_ydk_ids(text)
        if not card_ids:
            await interaction.response.send_message("No card IDs found in the provided YDK file.", ephemeral=True)
            return

        try:
            pack_path = self._resolve_pack_csv(pack_csv)
        except ValueError:
            await interaction.response.send_message("Invalid pack CSV selection.", ephemeral=True)
            return

        await interaction.response.defer(thinking=True, ephemeral=True)

        loop = asyncio.get_running_loop()
        try:
            api_data = await loop.run_in_executor(None, self._fetch_cards_for_ids, card_ids)
        except Exception as exc:
            await interaction.followup.send(f"Failed to fetch card data: {exc}", ephemeral=True)
            return

        existing = self._read_existing_keys(pack_path)
        to_write: List[Dict[str, str]] = []
        missing: List[int] = []
        skipped_duplicates = 0
        duplicate_details: List[str] = []
        normalized_cardset = cardset.strip()
        rarity_value = cardrarity.value if isinstance(cardrarity, app_commands.Choice) else str(cardrarity)

        for cid in card_ids:
            entry = api_data.get(cid)
            if not entry:
                missing.append(cid)
                continue
            name = (entry.get("name") or "").strip()
            card_id = str(entry.get("id") or "")
            card_sets = entry.get("card_sets") or []
            card_images = entry.get("card_images") or []
            card_code = ""
            print_id = ""
            if card_sets:
                card_code = str(card_sets[0].get("set_code") or "").strip()
            if card_images:
                print_id = str(card_images[0].get("id") or "").strip()

            key = (card_id, print_id, normalized_cardset)
            if key in existing:
                skipped_duplicates += 1
                duplicate_details.append(
                    f"{name} (ID {card_id or 'N/A'}; print {print_id or 'N/A'}; set {normalized_cardset or 'N/A'})"
                )
                continue
            existing.add(key)

            row = {
                "cardname": name,
                "cardq": "1",
                "cardrarity": rarity_value,
                "card_edition": "1st Edition",
                "cardset": normalized_cardset,
                "cardcode": card_code,
                "cardid": card_id,
                "print_id": print_id,
            }
            to_write.append(row)

        if not to_write:
            summary = "No new cards to add."
            if missing:
                summary += f" {len(missing)} card(s) could not be resolved via the API."
            if skipped_duplicates:
                summary += f" {skipped_duplicates} duplicate card(s) skipped."
                if duplicate_details:
                    detail_str = ", ".join(duplicate_details[:10])
                    if len(duplicate_details) > 10:
                        detail_str += ", ..."
                    summary += f" Duplicates: {detail_str}."
            await interaction.followup.send(summary, ephemeral=True)
            return

        need_header = not pack_path.exists() or pack_path.stat().st_size == 0
        try:
            with open(pack_path, "a", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                if need_header:
                    writer.writeheader()
                for row in to_write:
                    writer.writerow(row)
        except Exception as exc:
            await interaction.followup.send(f"Failed to write to CSV: {exc}", ephemeral=True)
            return

        message_lines = [
            f"Imported **{len(to_write)}** card(s) into `{pack_path.name}` as part of set **{cardset}**.",
        ]
        if skipped_duplicates:
            message_lines.append(f"Skipped {skipped_duplicates} duplicate card(s).")
            if duplicate_details:
                detail_str = ", ".join(duplicate_details[:10])
                if len(duplicate_details) > 10:
                    detail_str += ", ..."
                message_lines.append(f"Duplicates: {detail_str}.")
        if missing:
            missing_str = ", ".join(map(str, missing[:10]))
            if len(missing) > 10:
                missing_str += ", ..."
            message_lines.append(f"Unable to resolve {len(missing)} card(s): {missing_str}")

        await interaction.followup.send("\n".join(message_lines), ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(Packs(bot))
