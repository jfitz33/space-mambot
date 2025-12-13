# core/images.py
from __future__ import annotations
import re
from io import BytesIO
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple, Any, Sequence
from urllib.parse import quote
import discord
from PIL import Image, ImageDraw, ImageFont
import textwrap

# Prefer GIF files for animated rarity emojis if present; fall back to PNG.
RARITY_FILES: Dict[str, str] = {
    "common": ("common.gif", "common.png"),
    "rare":   ("rare.gif", "rare.png"),
    "super":  ("super.gif", "super.png"),
    "ultra":  ("ultra.gif", "ultra.png"),
    "secret": ("secret.gif", "secret.png"),
    "frostfire": ("frostfire.gif", "frostfire.png"),
}

FALLBACK_BADGES: Dict[str, str] = {
    "common": "🟡",
    "rare":   "🔴",
    "super":  "🔵",
    "ultra":  "🟢",
    "secret": "⚪",
}

PACK_CARD_WIDTH = 180
PACK_CARD_HEIGHT = 262
PACK_CARD_GAP = 12
PACK_BACKGROUND = (15, 23, 42, 255)
PACK_PLACEHOLDER_BG = (30, 41, 59, 255)
PACK_PLACEHOLDER_TEXT = (148, 163, 184, 255)

def _slugify(name: str, max_len: int = 100) -> str:
    base = re.sub(r"[^A-Za-z0-9]+", "_", (name or "").strip()).strip("_")
    return (base or "card")[:max_len]

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
    emoji_animated = {}
    if hasattr(state_or_bot, "rarity_emoji_ids") and getattr(state_or_bot, "rarity_emoji_ids"):
        emoji_ids = state_or_bot.rarity_emoji_ids
        emoji_animated = getattr(state_or_bot, "rarity_emoji_animated", {}) or {}
    elif hasattr(state_or_bot, "state") and getattr(state_or_bot.state, "rarity_emoji_ids", None):
        emoji_ids = state_or_bot.state.rarity_emoji_ids
        emoji_animated = getattr(state_or_bot.state, "rarity_emoji_animated", {}) or {}

    eid = (emoji_ids or {}).get(key)
    if eid:
        anim = "a" if (emoji_animated or {}).get(key) else ""
        return f"<{anim}:rar_{key}:{int(eid)}>"
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
    refresh: bool = False,
) -> None:
    """
    Cache rarity emoji IDs into bot.state.rarity_emoji_ids.
    Looks for emojis named: rar_common, rar_rare, rar_super, rar_ultra, rar_secret.
    Optionally creates any missing ones from /images/rarity_logos/*.gif or *.png.
    Set ``refresh=True`` to delete any existing ``rar_*`` emojis in the scanned
    guilds and recreate them from the local images. The cache is also cleared so
    new IDs are stored.
    """
    if not hasattr(bot, "state") or bot.state is None:
        raise RuntimeError("bot.state is required for caching rarity emoji IDs")
    if not hasattr(bot.state, "rarity_emoji_ids") or bot.state.rarity_emoji_ids is None:
        bot.state.rarity_emoji_ids = {}
    if not hasattr(bot.state, "rarity_emoji_animated") or bot.state.rarity_emoji_animated is None:
        bot.state.rarity_emoji_animated = {}

    wanted_names = {f"rar_{k}": k for k in RARITY_FILES.keys()}
    resolved: Dict[str, int] = {}
    resolved_anim: Dict[str, bool] = {}

    # determine guilds to scan/create in
    gids = list(guild_ids or [])
    if not gids:
        gids = [g.id for g in getattr(bot, "guilds", [])]

    if refresh:
        bot.state.rarity_emoji_ids.clear()
        bot.state.rarity_emoji_animated.clear()
        for gid in gids:
            guild = bot.get_guild(gid)
            if not guild:
                continue
            for e in list(guild.emojis):
                if e.name in wanted_names:
                    try:
                        await e.delete(reason="Refresh rarity emoji")
                        if verbose:
                            print(f"[rarity] deleted {e.name} ({e.id}) in guild {gid}")
                    except discord.Forbidden:
                        if verbose:
                            print(f"[rarity] forbidden deleting {e.name} in guild {gid}")
                    except discord.HTTPException as exc:
                        if verbose:
                            print(f"[rarity] HTTPException deleting {e.name} in guild {gid}: {exc}")

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
                resolved_anim[key] = bool(getattr(e, "animated", False))
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
                    filenames = RARITY_FILES[r]
                    path = None
                    for filename in filenames:
                        candidate = base / filename
                        if candidate.is_file():
                            path = candidate
                            break
                    if not path:
                        if verbose:
                            print(f"[rarity] file missing for {r}: {filenames} (skipping)")
                    try:
                        data = path.read_bytes()
                        if len(data) >= 256 * 1024:
                            if verbose:
                                print(f"[rarity] {path.name} is {len(data)} bytes (>=256KB) — Discord will reject it")
                            continue
                        emoji = await guild.create_custom_emoji(
                            name=f"rar_{r}", image=data, reason="Setup rarity emoji"
                        )
                        resolved[r] = emoji.id
                        resolved_anim[r] = bool(getattr(emoji, "animated", False))
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
        bot.state.rarity_emoji_animated.update(resolved_anim)

    if verbose:
        print(
            f"[rarity] cached IDs: {bot.state.rarity_emoji_ids} "
            f"(fallbacks used for missing)"
        )

def _card_images_dir() -> Path:
    # repo_root / images / card_images
    return Path(__file__).resolve().parents[1] / "images" / "card_images"

def find_card_art_path(name: str, card_id: int | str | None = None) -> Optional[Path]:
    """
    Return a Path to an existing local image for this card.
    Tries:
      <slug>.jpg
      <slug>-<id>.jpg (if id is provided)
    Then also tries .png/.jpeg just in case.
    """
    base = _card_images_dir()
    if not base.exists():
        return None

    slug = _slugify(name)
    exts = ("jpg", "png", "jpeg")

    cid = None
    if card_id is not None:
        s = str(card_id).strip()
        if s.isdigit():
            cid = s

    # Prefer explicit <slug>-<id> if present
    if cid:
        for ext in exts:
            p = base / f"{slug}-{cid}.{ext}"
            if p.exists():
                return p

    # Then plain <slug>.<ext>
    for ext in exts:
        p = base / f"{slug}.{ext}"
        if p.exists():
            return p

    # As a final fallback, allow any file that starts with slug (handles multiple prints)
    for ext in exts:
        matches = list(base.glob(f"{slug}-*.{ext}"))
        if matches:
            return matches[0]

    return None

def card_art_path_for_card(card: dict[str, Any]) -> Optional[Path]:
    """Convenience wrapper that pulls name/id from a card dict."""
    name = (card.get("name") or card.get("cardname") or "").strip()
    raw_id = card.get("card_id") or card.get("cardid") or card.get("id")
    return find_card_art_path(name, raw_id)

def _pack_placeholder_image(name: str) -> Image.Image:
    label = textwrap.fill(name or "No Art", width=16)
    canvas = Image.new("RGBA", (PACK_CARD_WIDTH, PACK_CARD_HEIGHT), PACK_PLACEHOLDER_BG)
    draw = ImageDraw.Draw(canvas)
    font = ImageFont.load_default()
    bbox = draw.multiline_textbbox((0, 0), label, font=font, align="center")
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]
    x = (PACK_CARD_WIDTH - text_width) / 2
    y = (PACK_CARD_HEIGHT - text_height) / 2
    draw.multiline_text(
        (x, y),
        label,
        fill=PACK_PLACEHOLDER_TEXT,
        font=font,
        align="center",
    )
    return canvas


def _pack_card_slot_image(card: dict[str, Any]) -> tuple[Image.Image, bool]:
    """Return a card image resized for the pack strip and whether art was missing."""

    art_path = card_art_path_for_card(card)
    name = (card.get("name") or card.get("cardname") or "Unknown").strip() or "Unknown"

    if art_path is None:
        return _pack_placeholder_image(name), True

    try:
        with Image.open(art_path) as raw:
            src = raw.convert("RGBA")
            width, height = src.size
            if width <= 0 or height <= 0:
                raise ValueError("invalid image dimensions")
            scale = min(PACK_CARD_WIDTH / width, PACK_CARD_HEIGHT / height)
            resized = src.resize(
                (max(1, int(width * scale)), max(1, int(height * scale))),
                Image.LANCZOS,
            )
    except Exception:
        return _pack_placeholder_image(name), True

    slot = Image.new("RGBA", (PACK_CARD_WIDTH, PACK_CARD_HEIGHT), (0, 0, 0, 0))
    offset_x = (PACK_CARD_WIDTH - resized.width) // 2
    offset_y = (PACK_CARD_HEIGHT - resized.height) // 2
    slot.paste(resized, (offset_x, offset_y), resized)
    return slot, False


def compose_pack_strip_image(
    pack_name: str,
    cards: Sequence[dict[str, Any]],
    *,
    pack_index: int | None = None,
) -> tuple[discord.File | None, bool]:
    """Create a horizontal strip image for the provided pack pulls.

    Returns a tuple of (discord.File | None, missing_art_flag).
    """

    if not cards:
        return None, False

    slots: list[Image.Image] = []
    missing_any = False
    for card in cards:
        slot, missing = _pack_card_slot_image(card)
        slots.append(slot)
        missing_any = missing_any or missing

    if not slots:
        return None, missing_any

    width = PACK_CARD_GAP + len(slots) * (PACK_CARD_WIDTH + PACK_CARD_GAP)
    height = PACK_CARD_GAP * 2 + PACK_CARD_HEIGHT
    canvas = Image.new("RGBA", (width, height), PACK_BACKGROUND)

    for idx, slot in enumerate(slots):
        x = PACK_CARD_GAP + idx * (PACK_CARD_WIDTH + PACK_CARD_GAP)
        canvas.paste(slot, (x, PACK_CARD_GAP), slot)

    buffer = BytesIO()
    canvas.save(buffer, format="PNG")
    buffer.seek(0)

    suffix = f"_pack_{pack_index}" if pack_index is not None else ""
    filename = f"{_slugify(pack_name + suffix) or 'pack'}_strip.png"
    discord_file = discord.File(buffer, filename=filename)
    return discord_file, missing_any

_CARD_IMAGE_REPO_BASE = "https://raw.githubusercontent.com/jfitz33/space-mambot/main/images/card_images"

def card_art_url_for_card(card: dict[str, Any]) -> Optional[str]:
    """
    Build the canonical GitHub raw URL for a card's art.

    Falls back to slugifying the card name and appending `.jpg` if we can't
    resolve a local file with a specific suffix.
    """

    path = card_art_path_for_card(card)
    if path is not None:
        filename = path.name
    else:
        name = (card.get("name") or card.get("cardname") or "").strip()
        if not name:
            return None
        slug_name = _slugify(name)
        if not slug_name:
            return None
        candidate = _card_images_dir() / f"{slug_name}.jpg"
        if not candidate.exists():
            return None
        filename = candidate.name

    return f"{_CARD_IMAGE_REPO_BASE}/{quote(filename)}"

def test_card_thumbnail_file() -> Tuple[Optional[discord.File], Optional[str]]:
    """
    Returns (discord.File, 'attachment://filename') for the first image found in /images/card_images.
    If none found, returns (None, None).
    """
    base = _card_images_dir()
    if not base.exists():
        return None, None
    for ext in ("png", "jpg", "jpeg", "webp"):
        for p in base.glob(f"*.{ext}"):
            f = discord.File(p, filename=p.name)
            return f, f"attachment://{p.name}"
    return None, None

def first_test_card_image_path() -> Path | None:
    base = Path(__file__).resolve().parents[1] / "images" / "card_images"
    if not base.exists():
        return None
    for ext in ("png","jpg","jpeg","webp"):
        for p in base.glob(f"*.{ext}"):
            return p
    return None
