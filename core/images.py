# core/images.py
from __future__ import annotations
import os, io
from pathlib import Path
from typing import Dict, Iterable, Optional
import discord

RARITY_FILES: Dict[str, str] = {
    "common": "common.png",
    "rare":   "rare.png",
    "super":  "super.png",
    "ultra":  "ultra.png",
    "secret": "secret.png",
}

FALLBACK_BADGES: Dict[str, str] = {
    "common": "🟡",
    "rare":   "🔴",
    "super":  "🔵",
    "ultra":  "🟢",
    "secret": "⚪",
}

def rarity_badge(state_or_bot, rarity: str) -> str:
    """
    Return a badge string for the rarity: '<:rar_common:ID>' or a Unicode fallback.
    - Accepts AppState or Bot; will look for emoji IDs on either `.rarity_emoji_ids`
      or `.state.rarity_emoji_ids`.
    - Normalizes common aliases like 'super rare', 'sr', 'UR', etc.
    """
    # 1) normalize rarity to canonical key
    r = (rarity or "").strip().lower()
    ALIASES = {
        "c": "common", "common": "common",
        "r": "rare", "rare": "rare",
        "sr": "super", "super": "super", "super rare": "super",
        "ur": "ultra", "ultra": "ultra", "ultra rare": "ultra",
        "secr": "secret", "secret": "secret", "secret rare": "secret",
    }
    key = ALIASES.get(r, r)  # default to r if already canonical

    # 2) find the emoji id cache on either the object or its `.state`
    emoji_ids = {}
    if hasattr(state_or_bot, "rarity_emoji_ids") and getattr(state_or_bot, "rarity_emoji_ids"):
        emoji_ids = state_or_bot.rarity_emoji_ids
    elif hasattr(state_or_bot, "state") and getattr(state_or_bot.state, "rarity_emoji_ids", None):
        emoji_ids = state_or_bot.state.rarity_emoji_ids

    eid = (emoji_ids or {}).get(key)
    if eid:
        return f"<:rar_{key}:{int(eid)}>"
    return FALLBACK_BADGES.get(key, "•")

def _images_dir() -> Path:
    # Resolve relative to the repository root (this file is core/images.py)
    # repo_root / images / rarity_logos
    return Path(__file__).resolve().parents[1] / "images" / "rarity_logos"

async def ensure_rarity_emojis(
    bot: discord.Client,
    *,
    guild_ids: Optional[Iterable[int]] = None,
    create_if_missing: bool = True,
    verbose: bool = True,
) -> None:
    """
    Cache rarity emoji IDs into bot.state.rarity_emoji_ids.
    Looks for emojis named: rar_common, rar_rare, rar_super, rar_ultra, rar_secret.
    Optionally creates any missing ones from /images/rarity_logos/*.png.
    """
    if not hasattr(bot, "state") or bot.state is None:
        raise RuntimeError("bot.state is required for caching rarity emoji IDs")
    if not hasattr(bot.state, "rarity_emoji_ids") or bot.state.rarity_emoji_ids is None:
        bot.state.rarity_emoji_ids = {}

    wanted_names = {f"rar_{k}": k for k in RARITY_FILES.keys()}
    resolved: Dict[str, int] = {}

    # determine guilds to scan/create in
    gids = list(guild_ids or [])
    if not gids:
        gids = [g.id for g in getattr(bot, "guilds", [])]

    if verbose:
        print(f"[rarity] scanning guilds: {gids or '[]'}")

    # pass 1: find existing by name
    for gid in gids:
        guild = bot.get_guild(gid)
        if not guild:
            continue
        if verbose:
            print(f"[rarity] checking existing emojis in guild {gid} ({getattr(guild, 'name', '?')})")
        for e in guild.emojis:
            key = wanted_names.get(e.name)
            if key and key not in resolved:
                resolved[key] = e.id
                if verbose:
                    print(f"[rarity] found {e.name} -> {e.id}")
            if len(resolved) == len(RARITY_FILES):
                break
        if len(resolved) == len(RARITY_FILES):
            break

    # pass 2: create missing
    missing = [r for r in RARITY_FILES.keys() if r not in resolved]
    if create_if_missing and missing:
        if not gids:
            if verbose:
                print("[rarity] no guild available to create emojis; using fallbacks")
        else:
            guild = bot.get_guild(gids[0])
            if not guild:
                if verbose:
                    print(f"[rarity] first guild id {gids[0]} not found; using fallbacks")
            else:
                base = _images_dir()
                if verbose:
                    print(f"[rarity] attempting to create missing emojis in guild {guild.id}: {missing}")
                    print(f"[rarity] images dir -> {base}")
                for r in missing:
                    filename = RARITY_FILES[r]
                    path = base / filename
                    if not path.is_file():
                        if verbose:
                            print(f"[rarity] file missing: {path} (skipping {r})")
                        continue
                    try:
                        data = path.read_bytes()
                        if len(data) >= 256 * 1024:
                            if verbose:
                                print(f"[rarity] {filename} is {len(data)} bytes (>=256KB) — Discord will reject it")
                            continue
                        emoji = await guild.create_custom_emoji(
                            name=f"rar_{r}", image=data, reason="Setup rarity emoji"
                        )
                        resolved[r] = emoji.id
                        if verbose:
                            print(f"[rarity] created rar_{r} -> {emoji.id}")
                    except discord.Forbidden:
                        if verbose:
                            print("[rarity] Forbidden: bot needs 'Manage Emojis and Stickers' in this guild")
                        break
                    except discord.HTTPException as e:
                        if verbose:
                            print(f"[rarity] HTTPException creating rar_{r}: {e}")
                    except Exception as e:
                        if verbose:
                            print(f"[rarity] unexpected error creating rar_{r}: {e}")

    if resolved:
        bot.state.rarity_emoji_ids.update(resolved)

    if verbose:
        print(f"[rarity] cached IDs: {bot.state.rarity_emoji_ids} (fallbacks used for missing)")
