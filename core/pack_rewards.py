import asyncio
import logging
from typing import Optional

import discord

from core.db import db_add_cards
from core.packs import open_pack_from_csv, open_mini_pack_from_csv
from core.views import _pack_embed_for_cards

logger = logging.getLogger(__name__)


class PackRewardHelper:
    """Grant packs outside the shop UI and DM the results to the recipient."""

    def __init__(self, state, client: discord.Client):
        self.state = state
        self.client = client

    async def grant_pack(self, user_id: int, pack_name: str, quantity: int = 1) -> str:
        if not pack_name:
            raise RuntimeError("Pack reward payload missing 'pack' name.")

        qty = max(1, int(quantity or 1))
        if pack_name not in (self.state.packs_index or {}):
            raise RuntimeError(f"Unknown pack '{pack_name}'.")

        per_pack: list[list[dict]] = []
        for _ in range(qty):
            per_pack.append(open_pack_from_csv(self.state, pack_name, 1))

        flat = [card for pack in per_pack for card in pack]
        db_add_cards(self.state, user_id, flat, pack_name)

        dm_sent = await self._send_results(user_id, pack_name, per_pack)
        status = (
            f"Opened {qty}× {pack_name} pack(s) and sent results via DM."
            if dm_sent
            else f"Opened {qty}× {pack_name} pack(s); could not DM results."
        )
        return status

    async def grant_mini_pack(
        self,
        user_id: int,
        pack_names: list[str],
        quantity: int = 1,
        display_name: str | None = None,
    ) -> str:
        """Grant a mini pack (4 commons + 1 rare) and DM results."""
        if not pack_names:
            raise RuntimeError("Mini pack reward payload missing 'pack' name.")

        qty = max(1, int(quantity or 1))
        available_packs = [name for name in pack_names if name in (self.state.packs_index or {})]
        if not available_packs:
            raise RuntimeError("Unknown pack(s) for mini pack reward.")

        per_pack: list[list[dict]] = []
        for _ in range(qty):
            per_pack.append(open_mini_pack_from_csv(self.state, available_packs))

        flat = [card for pack in per_pack for card in pack]
        db_add_cards(self.state, user_id, flat, available_packs[0])

        pack_label = display_name or available_packs[0]
        dm_sent = await self._send_results(user_id, pack_label, per_pack)
        status = (
            f"Opened {qty}× {pack_label} and sent results via DM."
            if dm_sent
            else f"Opened {qty}× {pack_label}; could not DM results."
        )
        return status

    async def _send_results(self, user_id: int, pack_name: str, per_pack: list[list[dict]]) -> bool:
        user = await self._resolve_user(user_id)
        if user is None:
            return False
        try:
            dm = await user.create_dm()
            for i, cards in enumerate(per_pack, start=1):
                content, embeds, files = _pack_embed_for_cards(self.client, pack_name, cards, i, len(per_pack))
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
            logger.warning("Failed to DM pack rewards to user_id=%s", user_id, exc_info=True)
            return False

    async def _resolve_user(self, user_id: int) -> Optional[discord.User | discord.Member]:
        user = self.client.get_user(user_id)
        if user:
            return user
        try:
            return await self.client.fetch_user(user_id)
        except Exception:
            return None