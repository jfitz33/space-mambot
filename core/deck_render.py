from __future__ import annotations

import math
import re
import textwrap
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Sequence

from PIL import Image, ImageDraw, ImageFont

from core.images import find_card_art_path

# Visual constants
BACKGROUND_COLOR = (15, 23, 42, 255)
TITLE_COLOR = (241, 245, 249, 255)
PLACEHOLDER_BG = (30, 41, 59, 255)
PLACEHOLDER_TEXT = (148, 163, 184, 255)

CARD_WIDTH = 180
CARD_HEIGHT = 262
CARD_GAP = 14
TITLE_PADDING = 24
TITLE_GAP = 18

FONT_PATH = Path(__file__).resolve().parents[1] / "assets" / "DejaVuSans.ttf"


@dataclass(slots=True)
class DeckCardEntry:
    card_id: str
    name: str
    card_type: str | None = None


def _load_font(size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(str(FONT_PATH), size=size)


def _slugify(text: str, max_len: int = 48) -> str:
    base = re.sub(r"[^A-Za-z0-9]+", "_", (text or "").strip()).strip("_")
    return (base or "deck")[:max_len].lower()


def _create_placeholder_card(name: str) -> Image.Image:
    img = Image.new("RGBA", (CARD_WIDTH, CARD_HEIGHT), PLACEHOLDER_BG)
    draw = ImageDraw.Draw(img)
    font = _load_font(18)
    wrapped = textwrap.fill(name or "No Card", width=16)
    bbox = draw.multiline_textbbox((0, 0), wrapped, font=font, align="center")
    w = bbox[2] - bbox[0]
    h = bbox[3] - bbox[1]
    x = (CARD_WIDTH - w) / 2
    y = (CARD_HEIGHT - h) / 2
    draw.multiline_text((x, y), wrapped, fill=PLACEHOLDER_TEXT, font=font, align="center")
    return img


def _load_card_image(entry: DeckCardEntry, cache: dict[str, Image.Image]) -> Image.Image:
    art_path = find_card_art_path(entry.name, entry.card_id)
    if art_path:
        key = str(art_path)
        cached = cache.get(key)
        if cached is None:
            with Image.open(art_path) as raw:
                src = raw.convert("RGBA")
                w, h = src.size
                scale = min(CARD_WIDTH / max(w, 1), CARD_HEIGHT / max(h, 1))
                resized = src.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
            canvas = Image.new("RGBA", (CARD_WIDTH, CARD_HEIGHT), (0, 0, 0, 0))
            offset_x = (CARD_WIDTH - resized.width) // 2
            offset_y = (CARD_HEIGHT - resized.height) // 2
            canvas.paste(resized, (offset_x, offset_y))
            cache[key] = canvas
            cached = canvas
        return cached.copy()
    return _create_placeholder_card(entry.name)


def _render_empty_section(title: str) -> tuple[BytesIO, str]:
    width = CARD_WIDTH * 3
    height = CARD_HEIGHT + 2 * TITLE_PADDING
    image = Image.new("RGBA", (width, height), BACKGROUND_COLOR)
    draw = ImageDraw.Draw(image)
    title_font = _load_font(34)
    subtitle_font = _load_font(22)

    tmp = Image.new("RGBA", (1, 1))
    tmp_draw = ImageDraw.Draw(tmp)
    title_bbox = tmp_draw.textbbox((0, 0), title, font=title_font)
    title_w = title_bbox[2] - title_bbox[0]
    title_h = title_bbox[3] - title_bbox[1]
    draw.text(((width - title_w) / 2, TITLE_PADDING), title, font=title_font, fill=TITLE_COLOR)

    message = "No cards submitted"
    msg_bbox = tmp_draw.textbbox((0, 0), message, font=subtitle_font)
    msg_w = msg_bbox[2] - msg_bbox[0]
    msg_h = msg_bbox[3] - msg_bbox[1]
    draw.text(
        ((width - msg_w) / 2, TITLE_PADDING + title_h + TITLE_GAP),
        message,
        font=subtitle_font,
        fill=PLACEHOLDER_TEXT,
    )

    buffer = BytesIO()
    image.save(buffer, format="PNG")
    buffer.seek(0)
    filename = f"{_slugify(title)}.png"
    return buffer, filename


def render_deck_section_image(
    title: str,
    cards: Sequence[DeckCardEntry],
    *,
    max_columns: int = 10,
) -> tuple[BytesIO, str]:
    if not cards:
        return _render_empty_section(title)

    columns = max(1, min(max_columns, len(cards)))
    rows = math.ceil(len(cards) / columns)

    title_font = _load_font(34)
    tmp = Image.new("RGBA", (1, 1))
    tmp_draw = ImageDraw.Draw(tmp)
    title_bbox = tmp_draw.textbbox((0, 0), title, font=title_font)
    title_height = title_bbox[3] - title_bbox[1]
    title_width = title_bbox[2] - title_bbox[0]

    width = CARD_GAP + columns * (CARD_WIDTH + CARD_GAP)
    height = (
        TITLE_PADDING
        + title_height
        + TITLE_GAP
        + rows * CARD_HEIGHT
        + (rows - 1) * CARD_GAP
        + CARD_GAP
    )

    image = Image.new("RGBA", (width, height), BACKGROUND_COLOR)
    draw = ImageDraw.Draw(image)

    draw.text(
        ((width - title_width) / 2, TITLE_PADDING),
        title,
        font=title_font,
        fill=TITLE_COLOR,
    )

    card_cache: dict[str, Image.Image] = {}
    for idx, entry in enumerate(cards):
        row = idx // columns
        col = idx % columns
        x = CARD_GAP + col * (CARD_WIDTH + CARD_GAP)
        y = (
            TITLE_PADDING
            + title_height
            + TITLE_GAP
            + row * (CARD_HEIGHT + CARD_GAP)
        )
        card_image = _load_card_image(entry, card_cache)
        image.paste(card_image, (x, y), card_image)

    buffer = BytesIO()
    image.save(buffer, format="PNG")
    buffer.seek(0)
    filename = f"{_slugify(title)}.png"
    return buffer, filename