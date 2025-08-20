# tools/prepare_emojis.py
from pathlib import Path
from io import BytesIO
from PIL import Image

SRC = Path(__file__).resolve().parents[1] / "images" / "rarity_logos"
DST = SRC / "_out"
DST.mkdir(parents=True, exist_ok=True)

TARGET_PX = 128
MAX_BYTES = 256 * 1024
FILES = {
    "common.png",
    "rare.png",
    "super.png",
    "ultra.png",
    "secret.png",
}

def square_pad(im, bg=(0, 0, 0, 0)):
    w, h = im.size
    s = max(w, h)
    out = Image.new("RGBA", (s, s), bg)
    out.paste(im, ((s - w)//2, (s - h)//2))
    return out

def compress_one(path: Path, out_path: Path):
    im = Image.open(path).convert("RGBA")
    im = square_pad(im)
    if max(im.size) > TARGET_PX:
        im = im.resize((TARGET_PX, TARGET_PX), Image.LANCZOS)

    # try a few palettes/optimizations until < 256KB
    for colors in (64, 48, 32, 16):
        pal = im.convert("P", palette=Image.Palette.ADAPTIVE, colors=colors)
        for optimize in (True, False):
            buf = BytesIO()
            pal.save(buf, format="PNG", optimize=optimize)
            size = buf.tell()
            if size <= MAX_BYTES:
                out_path.write_bytes(buf.getvalue())
                return size, colors, optimize
    # fallback to whatever best we got (last attempt)
    out_path.write_bytes(buf.getvalue())
    return buf.tell(), colors, optimize

def main():
    print(f"SRC: {SRC}")
    for name in FILES:
        src = SRC / name
        if not src.is_file():
            print(f"!! Missing {src}")
            continue
        dst = DST / name
        size, colors, optimize = compress_one(src, dst)
        ok = "OK " if size <= MAX_BYTES else "BIG"
        print(f"{ok} {name:10} -> {size/1024:.1f} KB (colors={colors}, optimize={optimize}) -> {dst}")

if __name__ == "__main__":
    main()
