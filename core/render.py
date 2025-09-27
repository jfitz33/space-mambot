from __future__ import annotations
from io import BytesIO
from pathlib import Path
from dataclasses import dataclass
from typing import Iterable, Tuple, Optional, Dict, List
from PIL import Image, ImageDraw, ImageFont, ImageFilter

from .images import RARITY_FILES

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

@dataclass
class BadgeFrames:
    frames: List[Image.Image]
    durations: List[int]
    is_animated: bool = False

def _load_badge_image(rarity: str) -> Optional[BadgeFrames]:
    base = _badges_dir()
    filenames = RARITY_FILES.get(rarity) or (f"{rarity}.gif", f"{rarity}.png")

    for name in filenames:
        path = base / name
        if not path.exists():
            continue
        try:
            src = Image.open(path)
        except Exception:
            continue

        try:
            if getattr(src, "is_animated", False):
                frames: List[Image.Image] = []
                durations: List[int] = []
                total = getattr(src, "n_frames", 1)
                for idx in range(max(1, total)):
                    try:
                        src.seek(idx)
                    except EOFError:
                        break
                    frame = src.convert("RGBA").copy()
                    frames.append(frame)
                    dur = src.info.get("duration")
                    durations.append(int(dur) if isinstance(dur, (int, float)) and dur else 100)
                if frames:
                    return BadgeFrames(frames=frames, durations=durations or [100], is_animated=True)
            else:
                frame = src.convert("RGBA").copy()
                return BadgeFrames(frames=[frame], durations=[0], is_animated=False)
        except Exception:
            continue
        finally:
            try:
                src.close()
            except Exception:
                pass
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
    """Composite image of pack pulls.

    Returns a PNG when all rarity badges are static, or an animated GIF when any
    badge uses multiple frames. Layout: left column = rarity badge + card name,
    right column = optional card art scaled to match the text height.
    """
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

    badge_cache: Dict[str, Optional[BadgeFrames]] = {}
    badge_assets: List[Optional[BadgeFrames]] = []
    for rarity, _ in rows:
        if rarity not in badge_cache:
            badge_cache[rarity] = _load_badge_image(rarity)
        badge_assets.append(badge_cache[rarity])

    dot_font = _load_font(max(8, int(FONT_SIZE * 0.65)))

    def _draw_frame(im: Image.Image, badge_frames: List[Optional[Image.Image]]) -> None:
        draw = ImageDraw.Draw(im)
        y = TOP_PAD
        x = LEFT_PAD
        for (rarity, name), badge_img in zip(rows, badge_frames):
            if badge_img is not None:
                bw, bh = badge_img.size
                s = BADGE_H / max(1, bh)
                bw2, bh2 = int(bw * s), int(bh * s)
                bimg = badge_img.resize((bw2, bh2), Image.LANCZOS)
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
                l, t, r, b = draw.textbbox((0, 0), letter, font=dot_font)
                lw, lh = (r - l), (b - t)
                draw.text((x + (dot_d - lw)/2, dot_top + (dot_d - lh)/2), letter, font=dot_font, fill=(15,23,42,255))
                badge_right = x + dot_d

            txt_x = badge_right + BADGE_GAP
            name_y = _center_y(draw, name, font, y)
            draw.text((txt_x, name_y), name, font=font, fill=FG)

            y += LINE_H

    animated = any(asset and asset.is_animated for asset in badge_assets)

    if not animated:
        im = Image.new("RGBA", (total_w, total_h), BG)
        frame_badges: List[Optional[Image.Image]] = [
            (asset.frames[0] if asset and asset.frames else None)
            for asset in badge_assets
        ]
        _draw_frame(im, frame_badges)
        if art_img:
            im.alpha_composite(art_img, (left_col_w + MID_GAP, TOP_PAD))

        buf = BytesIO()
        im.save(buf, format="PNG", optimize=True)
        return buf.getvalue(), filename

    frame_count = max((len(asset.frames) for asset in badge_assets if asset and asset.frames), default=1)
    frames: List[Image.Image] = []
    durations: List[int] = []

    for idx in range(frame_count):
        frame_im = Image.new("RGBA", (total_w, total_h), BG)
        badge_frame_list: List[Optional[Image.Image]] = []
        for asset in badge_assets:
            if asset and asset.frames:
                badge_frame_list.append(asset.frames[idx % len(asset.frames)])
            else:
                badge_frame_list.append(None)
        _draw_frame(frame_im, badge_frame_list)
        if art_img:
            frame_im.alpha_composite(art_img, (left_col_w + MID_GAP, TOP_PAD))
        frames.append(frame_im)

        frame_durs = []
        for asset in badge_assets:
            if asset and asset.is_animated:
                durs = asset.durations or [100]
                frame_durs.append(durs[idx % len(durs)] if durs else 100)
        durations.append(max(20, max(frame_durs) if frame_durs else 100))

    buf = BytesIO()
    base_frame, *extra_frames = frames
    gif_name = filename
    if gif_name.lower().endswith(".png"):
        gif_name = gif_name[:-4] + ".gif"
    base_frame.save(
        buf,
        format="GIF",
        save_all=True,
        append_images=extra_frames,
        duration=durations,
        loop=0,
        disposal=2,
    )
    return buf.getvalue(), gif_name