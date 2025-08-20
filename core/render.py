from __future__ import annotations
from io import BytesIO
from pathlib import Path
from typing import Iterable, Tuple, Optional
from PIL import Image, ImageDraw, ImageFont, ImageFilter

# --- visuals (tweak if you want larger/Sharper text) ---
BG       = (17, 24, 39, 255)
FG       = (241, 245, 249, 255)
FG_SUB   = (148, 163, 184, 255)
LINE_H   = 28        # increase to 30–32 if you want a bigger, sharper panel
FONT_SIZE= 18
LEFT_PAD = 24
TOP_PAD  = 16
RIGHT_PAD= 24
MID_GAP  = 18

BADGE_H  = 24        # target badge height (raise to 24 for a touch more presence)
BADGE_GAP= 8

RARITY_LETTER = {"common":"C","rare":"R","super":"S","ultra":"U","secret":"S"}
RARITY_MAP = {
    "c":"common","common":"common",
    "r":"rare","rare":"rare",
    "sr":"super","super":"super","super rare":"super",
    "ur":"ultra","ultra":"ultra","ultra rare":"ultra",
    "secr":"secret","secret":"secret","secret rare":"secret",
}
RARITY_COLORS = {
    "common": (244, 208, 63, 255),
    "rare":   (231, 76, 60, 255),
    "super":  (52, 152, 219, 255),
    "ultra":  (46, 204, 113, 255),
    "secret": (192, 192, 192, 255),
}

FONT_PATH = Path(__file__).resolve().parents[1] / "assets" / "DejaVuSans.ttf"

def _load_font(size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(str(FONT_PATH), size=size)

def _badges_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "images" / "rarity_logos"

def _load_badge_image(rarity: str) -> Optional[Image.Image]:
    p = _badges_dir() / f"{rarity}.png"
    if p.exists():
        try:
            return Image.open(p).convert("RGBA")
        except Exception:
            return None
    return None

def _center_y(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont, line_top: int) -> float:
    """Return the y to draw text so that its bounding box is vertically centered in the row."""
    l, t, r, b = draw.textbbox((0, 0), text, font=font)  # bbox if drawn at (0,0)
    h = b - t
    return line_top + (LINE_H - h) / 2 - t  # subtract t: PIL bboxes are baseline-relative

def render_pack_panel(
    cards: Iterable[dict],
    *,
    card_image_path: Optional[Path] = None,
    filename: str = "pack_panel.png",
    scale: float = 1.0,  # render at 2.0 for sharper Discord preview
) -> Tuple[bytes, str]:
    """Composite PNG: left = (rarity badge + card name), right = card art scaled to the list height."""
    S = max(1.0, float(scale))  # don’t allow < 1.0

    # Scaled layout constants (larger name font, taller rows)
    LINE_H     = int(round(30 * S))   # was 28
    FONT_SIZE  = int(round(20 * S))   # was 22
    LEFT_PAD   = int(round(24 * S))
    TOP_PAD    = int(round(16 * S))
    RIGHT_PAD  = int(round(24 * S))
    MID_GAP    = int(round(18 * S))
    BADGE_H    = int(round(26 * S))   # was 22
    BADGE_GAP  = int(round(10 * S))   # a bit more breathing room

    # Normalize rows (no code)
    rows = []
    for c in cards:
        name  = (c.get("name") or c.get("cardname") or "Unknown").strip()
        r_raw = (c.get("rarity") or c.get("cardrarity") or "").strip().lower()
        rarity= RARITY_MAP.get(r_raw, r_raw)
        rows.append((rarity, name))
    if not rows:
        rows = [("common", "No cards")]

    # Fonts
    font = _load_font(FONT_SIZE)

    # Vertical centering using exact bbox
    def _center_y(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont, line_top: int) -> float:
        l, t, r, b = draw.textbbox((0, 0), text, font=font)
        h = b - t
        return line_top + (LINE_H - h) / 2 - t

    # Estimate left column width (badge + gap + name)
    tmp_im = Image.new("RGBA", (10, 10))
    tmp_dr = ImageDraw.Draw(tmp_im)
    max_name_w = max(tmp_dr.textlength(n, font=font) for _, n in rows)
    left_col_w = int(LEFT_PAD + BADGE_H + BADGE_GAP + max_name_w + LEFT_PAD)

    # Right art height = exactly stacked text height
    lines_h = len(rows) * LINE_H

    # Load/resize art
    art_img, art_w = None, 0
    if card_image_path and Path(card_image_path).exists():
        try:
            src = Image.open(card_image_path).convert("RGBA")
            w, h = src.size
            scale_h = lines_h / max(1, h)
            art_img = src.resize((int(w * scale_h), int(h * scale_h)), Image.LANCZOS)
            if scale_h > 1.0:
                art_img = art_img.filter(ImageFilter.UnsharpMask(radius=1.0, percent=160, threshold=2))
            art_w = art_img.width
        except Exception:
            art_img = None
            art_w = 0

    total_w = left_col_w + (MID_GAP if art_img else 0) + art_w + RIGHT_PAD
    total_h = TOP_PAD + lines_h + TOP_PAD

    im = Image.new("RGBA", (total_w, total_h), BG)
    draw = ImageDraw.Draw(im)

    # Render rows (badge image if available, else colored dot+letter)
    y = TOP_PAD
    x = LEFT_PAD
    for rarity, name in rows:
        badge = _load_badge_image(rarity)
        if badge is not None:
            bw, bh = badge.size
            s = BADGE_H / max(1, bh)
            bw2, bh2 = int(bw * s), int(bh * s)
            bimg = badge.resize((bw2, bh2), Image.LANCZOS)
            if s > 1.0:
                bimg = bimg.filter(ImageFilter.UnsharpMask(radius=0.8, percent=150, threshold=2))
            by = y + (LINE_H - bh2) // 2
            im.paste(bimg, (x, by), bimg)
            badge_right = x + bw2
        else:
            dot_d   = BADGE_H
            dot_top = y + (LINE_H - dot_d) // 2
            draw.ellipse([x, dot_top, x + dot_d, dot_top + dot_d], fill=RARITY_COLORS.get(rarity, (100,116,139,255)))
            letter = RARITY_LETTER.get(rarity, "?")
            dot_font = _load_font(max(8, int(FONT_SIZE * 0.65)))
            l, t, r, b = draw.textbbox((0, 0), letter, font=dot_font)
            lw, lh = (r - l), (b - t)
            draw.text((x + (dot_d - lw)/2, dot_top + (dot_d - lh)/2), letter, font=dot_font, fill=(15,23,42,255))
            badge_right = x + dot_d

        txt_x = badge_right + BADGE_GAP
        name_y = _center_y(draw, name, font, y)
        draw.text((txt_x, name_y), name, font=font, fill=FG)

        y += LINE_H

    if art_img:
        im.alpha_composite(art_img, (left_col_w + MID_GAP, TOP_PAD))

    buf = BytesIO()
    im.save(buf, format="PNG", optimize=True)
    return buf.getvalue(), filename
