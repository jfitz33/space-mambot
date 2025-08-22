# cogs/rewards_wheel.py
import os
import io
import math
import random
import asyncio
from typing import List, Tuple, Optional
from functools import lru_cache

import discord
from discord import app_commands
from discord.ext import commands
from PIL import Image, ImageDraw, ImageFont
from core.db import db_wallet_get, db_wallet_try_spend_mambucks, db_add_cards

# ---------------- Guild scope ----------------
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

# ---------------- Tunables ----------------
WHEEL_COST_MB = 100           # Mambucks per spin
WHEEL_SIZE = 384
SPIN_SECONDS = 5.0
FPS = 18                      # 5s * 18fps ≈ 90 frames
PAD_SECONDS = 0.8             # static lead-in (covers client start-up)
TAIL_SECONDS = 0.8            # static hold at end (prevents frame-1 flash)
GIF_COLORS = 64               # fewer colors -> smaller GIF
MAX_UPLOAD_BYTES = 8 * 1024 * 1024

# Weighted rarity segments by duplication (equal slice renderer)
# 20 total segments: 8C (40%), 5R (25%), 3SR (15%), 3UR (15%), 1SCR (5%)
RARITY_SEGMENTS: List[Tuple[str, int]] = [
    ("COMMON", 8),
    ("RARE", 5),
    ("SUPER RARE", 3),
    ("ULTRA RARE", 3),
    ("SECRET RARE", 1),
]

# ---------------- Helpers: render, wheel data, rarity pick ----------------
def _parse_segments() -> List[str]:
    opts: List[str] = []
    for rarity, n in RARITY_SEGMENTS:
        opts.extend([rarity] * n)
    return opts

def _load_font(size: int = 18) -> ImageFont.ImageFont:
    try:
        return ImageFont.truetype("arial.ttf", size)
    except Exception:
        return ImageFont.load_default()

@lru_cache(maxsize=64)
def _wheel_base_cached(options_key: tuple[str, ...], size: int) -> Image.Image:
    """Cache the wheel with labels; copy() before modifying."""
    return _draw_wheel_base(list(options_key), size=size)

def _draw_wheel_base(options: List[str], size: int = WHEEL_SIZE) -> Image.Image:
    """Return a PIL image of the wheel (no pointer), with slice labels."""
    W = H = size
    cx, cy = W // 2, H // 2
    radius = size // 2 - 12
    img = Image.new("RGBA", (W, H), (255, 255, 255, 255))
    draw = ImageDraw.Draw(img)
    font = _load_font(18)

    palette = [
        (244, 67, 54), (33, 150, 243), (76, 175, 80), (255, 235, 59), (156, 39, 176),
        (255, 152, 0), (121, 85, 72), (63, 81, 181), (0, 188, 212), (205, 220, 57),
        (233, 30, 99), (3, 169, 244), (139, 195, 74), (255, 193, 7), (103, 58, 183),
        (255, 87, 34), (96, 125, 139), (0, 150, 136), (158, 158, 158), (0, 137, 123),
        (124, 77, 255), (175, 180, 43), (255, 112, 67), (41, 98, 255),
    ]

    n = max(1, len(options))
    sweep = 360 / n
    start_angle = -90  # 12 o'clock

    for i, label in enumerate(options):
        a0 = start_angle + i * sweep
        a1 = a0 + sweep
        color = palette[i % len(palette)]
        draw.pieslice([cx - radius, cy - radius, cx + radius, cy + radius],
                      a0, a1, fill=color, outline=(255, 255, 255), width=2)

        mid = math.radians((a0 + a1) / 2)
        tx = cx + int(math.cos(mid) * (radius * 0.6))
        ty = cy + int(math.sin(mid) * (radius * 0.6))
        text = label  # "COMMON", etc.
        bbox = draw.textbbox((0, 0), text, font=font)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        draw.text((tx - tw // 2, ty - th // 2), text, fill=(255, 255, 255),
                  font=font, stroke_width=2, stroke_fill=(0, 0, 0))

    # center hub
    draw.ellipse([cx - 14, cy - 14, cx + 14, cy + 14], fill=(250, 250, 250), outline=(0, 0, 0))
    return img

def _draw_pointer_pointing_down(img: Image.Image):
    """Draw a pointer triangle at the top that POINTS DOWN into the wheel."""
    draw = ImageDraw.Draw(img)
    W, H = img.size
    cx, cy = W // 2, H // 2
    radius = min(W, H) // 2 - 12
    pointer = [
        (cx - 14, cy - radius - 22),  # left base (above rim)
        (cx + 14, cy - radius - 22),  # right base (above rim)
        (cx,     cy - radius + 6),    # apex (below rim) -> points DOWN
    ]
    draw.polygon(pointer, fill=(0, 0, 0))

def _render_static_wheel(options: List[str], winner_idx: Optional[int] = None, size: int = WHEEL_SIZE) -> io.BytesIO:
    base = _wheel_base_cached(tuple(options), size).copy()
    n = max(1, len(options))
    sweep = 360 / n
    angle = (winner_idx + 0.5) * sweep if winner_idx is not None else 0.0
    rotated = base.rotate(angle, resample=Image.BICUBIC, expand=False,
                          center=(size // 2, size // 2), fillcolor=(255, 255, 255))
    _draw_pointer_pointing_down(rotated)
    buf = io.BytesIO()
    rotated.save(buf, format="PNG")
    buf.seek(0)
    return buf

def _render_spin_gif(
    options: List[str],
    winner_idx: int,
    *,
    size: int = WHEEL_SIZE,
    duration_sec: float = SPIN_SECONDS,
    pad_sec: float = PAD_SECONDS,
    tail_sec: float = TAIL_SECONDS,
    spins: int = 2,
    fps: int = FPS
) -> io.BytesIO:
    base = _wheel_base_cached(tuple(options), size).copy()
    n = max(1, len(options))
    sweep = 360 / n
    final_deg = (winner_idx + 0.5) * sweep + spins * 360

    total_time   = pad_sec + duration_sec + tail_sec
    total_frames = max(8, int(fps * total_time))
    frames: List[Image.Image] = []
    cx, cy = size // 2, size // 2

    for i in range(total_frames):
        t = i / (total_frames - 1) * total_time
        if t <= pad_sec:
            angle = 0.0
        elif t >= pad_sec + duration_sec:
            angle = final_deg
        else:
            prog = (t - pad_sec) / duration_sec
            prog = 1 - (1 - prog) ** 3  # ease-out cubic
            angle = final_deg * prog

        frame = base.rotate(angle, resample=Image.BICUBIC, expand=False, center=(cx, cy),
                            fillcolor=(255, 255, 255)).convert("RGBA")
        _draw_pointer_pointing_down(frame)
        frame = frame.convert("P", palette=Image.ADAPTIVE, colors=GIF_COLORS)
        frames.append(frame)

    buf = io.BytesIO()
    frames[0].save(
        buf, format="GIF", save_all=True, append_images=frames[1:],
        duration=int(1000 / fps), loop=0, disposal=2, optimize=True
    )
    buf.seek(0)
    return buf

async def _make_gif_async(options: List[str], winner_idx: int, size: int, duration_sec: float, pad_sec: float, tail_sec: float) -> io.BytesIO:
    return await asyncio.to_thread(_render_spin_gif, options, winner_idx, size=size, duration_sec=duration_sec, pad_sec=pad_sec, tail_sec=tail_sec)

# ---------------- Prize selection helpers ----------------
def _normalize_rarity(r: str) -> str:
    r = (r or "").strip().upper()
    if r in ("C", "COMMON"): return "COMMON"
    if r in ("R", "RARE"): return "RARE"
    if r in ("SR", "SUPER", "SUPER RARE"): return "SUPER RARE"
    if r in ("UR", "ULTRA", "ULTRA RARE"): return "ULTRA RARE"
    if r in ("SCR", "SECRET", "SECRET RARE"): return "SECRET RARE"
    return "SECRET RARE"

def _build_rarity_pools_from_state(state) -> dict[str, list[dict]]:
    """Collect all cards by normalized rarity across packs/starters, regardless of original casing/alias."""
    pools: dict[str, list[dict]] = {
        "COMMON": [], "RARE": [], "SUPER RARE": [], "ULTRA RARE": [], "SECRET RARE": []
    }

    def add_from(idx):
        if not isinstance(idx, dict):
            return
        for p in idx.values():
            br = p.get("by_rarity") if isinstance(p, dict) else None
            if not isinstance(br, dict):
                continue
            for raw_key, lst in br.items():
                norm = _normalize_rarity(raw_key)
                if norm not in pools:
                    pools[norm] = []
                if isinstance(lst, list):
                    pools[norm].extend(lst)

    add_from(getattr(state, "packs_index", None))
    # support either attribute name
    add_from(getattr(state, "starters_index", None) or getattr(state, "starters", None))

    return pools

def _pick_random_card_by_rarity(state, rarity: str) -> dict | None:
    """
    Try exact rarity first, then degrade to the next tier if empty.
    E.g., SECRET→ULTRA→SUPER→RARE→COMMON.
    """
    want = _normalize_rarity(rarity)
    pools = _build_rarity_pools_from_state(state)

    degrade_order = {
        "SECRET RARE": ["SECRET RARE", "ULTRA RARE", "SUPER RARE", "RARE", "COMMON"],
        "ULTRA RARE":  ["ULTRA RARE", "SUPER RARE", "RARE", "COMMON"],
        "SUPER RARE":  ["SUPER RARE", "RARE", "COMMON"],
        "RARE":        ["RARE", "COMMON"],
        "COMMON":      ["COMMON"],
    }

    for bucket in degrade_order.get(want, ["COMMON"]):
        candidates = pools.get(bucket) or []
        if candidates:
            return random.choice(candidates)
    return None

async def _award_card_to_user(state, user_id: int, card: dict, qty: int = 1) -> None:
    if db_add_cards:
        try:
            # Example signature; adjust to your DB helper
            db_add_cards(state, user_id, card)
            return
        except Exception as e:
            print(f"[rewards] db_add_to_collection failed: {e}")

    # If you use a different function, call it here:
    # from core.db import my_insert_fn
    # my_insert_fn(...)

    # Fallback: no-op (so the command still runs); feel free to raise instead
    print("[rewards] NOTE: No DB award function wired; prize not persisted.")

async def _debit_wallet_or_error(state, user: discord.User, amount: int) -> Optional[str]:
    """
    Attempts to debit Mambucks from the user's wallet.
    Return None on success, or an error message string on failure.
    """
    try:
        if db_wallet_get and db_wallet_try_spend_mambucks:
            bal = int((db_wallet_get(state, user.id)).get("mambucks", 0))
            if bal is None or bal < amount:
                return f"You need {amount} Mambucks to spin. Current balance: {bal if bal is not None else 0}."
            # do the debit
            res = await db_wallet_try_spend_mambucks(state, user.id, amount) if asyncio.iscoroutinefunction(db_wallet_try_spend_mambucks) else db_wallet_try_spend_mambucks(state, user.id, amount)
            if res is False:
                return "Could not debit your wallet. Please try again."
            return None
        # If you expose wallet on state:
        if hasattr(state, "wallet") and hasattr(state.wallet, "debit"):
            bal = getattr(state.wallet, "get_balance", lambda *_: None)(user.id)
            if bal is None or bal < amount:
                return f"You need {amount} Mambucks to spin. Current balance: {bal if bal is not None else 0}."
            ok = state.wallet.debit(user.id, amount, reason="Rewards Wheel spin")
            if not ok:
                return "Could not debit your wallet. Please try again."
            return None
        return "Wallet integration not wired. Please hook up _debit_wallet_or_error()."
    except Exception as e:
        return f"Wallet error: {e}"

# ---------------- Views ----------------
class ConfirmView(discord.ui.View):
    """Fallback 2-button confirmation if your project doesn't provide one."""
    def __init__(self, author_id: int, timeout: float = 30.0):
        super().__init__(timeout=timeout)
        self.author_id = author_id
        self.value: Optional[bool] = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the original user can respond.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.danger)
    async def yes(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = True
        for c in self.children:
            if isinstance(c, discord.ui.Button):
                c.disabled = True
        await interaction.response.edit_message(view=self)
        self.stop()

    @discord.ui.button(label="No", style=discord.ButtonStyle.secondary)
    async def no(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = False
        for c in self.children:
            if isinstance(c, discord.ui.Button):
                c.disabled = True
        await interaction.response.edit_message(view=self)
        self.stop()

# If you already have a project-wide confirm view, import and alias it here:
# from core.views import YourConfirmView as ConfirmView

class WheelView(discord.ui.View):
    def __init__(self, author_id: int, options: List[str], private: bool, size: int, state):
        super().__init__(timeout=180.0)
        self.author_id = author_id
        self.options = options
        self.private = private
        self.size = size
        self.state = state
        self.spinning = False  # gate re-entrancy

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the original user can spin this wheel.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Spin", style=discord.ButtonStyle.primary, emoji="🎡")
    async def spin_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.spinning:
            await interaction.response.send_message("Already spinning… please wait.", ephemeral=True)
            return

        # Disable while spinning
        self.spinning = True
        button.disabled = True
        await interaction.response.edit_message(view=self)

        # Show "Preparing…" ABOVE the wheel by re-attaching a fresh idle PNG (single attachment)
        idle_png = _render_static_wheel(self.options, winner_idx=None, size=self.size)
        prep = discord.Embed(title="🎡 Preparing spin…", description="Rendering animation…")
        prep.set_image(url="attachment://idle.png")
        await interaction.edit_original_response(
            embed=prep,
            attachments=[discord.File(idle_png, filename="idle.png")],
            view=self
        )

        # Pick winner rarity via the weighted index
        winner_idx = random.randrange(len(self.options))
        winner_rarity = self.options[winner_idx]

        # Build padded + tailed GIF
        try:
            gif_buf = await _make_gif_async(
                self.options, winner_idx,
                size=self.size, duration_sec=SPIN_SECONDS,
                pad_sec=PAD_SECONDS, tail_sec=TAIL_SECONDS
            )
        except Exception as e:
            # Fallback: static result
            final_png = _render_static_wheel(self.options, winner_idx, size=self.size)
            final_embed = discord.Embed(title="🎡 Wheel Result", description=f"**Winner:** {winner_rarity}\n\n(Spin rendering failed; showing static result.)")
            final_embed.set_image(url="attachment://result.png")
            await interaction.edit_original_response(
                embed=final_embed,
                attachments=[discord.File(final_png, filename="result.png")],
                view=self
            )
            self.spinning = False
            button.disabled = False
            return

        # Size fallback
        if gif_buf.getbuffer().nbytes > MAX_UPLOAD_BYTES:
            try:
                small = await _make_gif_async(
                    self.options, winner_idx,
                    size=max(256, self.size // 2),
                    duration_sec=SPIN_SECONDS, pad_sec=PAD_SECONDS, tail_sec=TAIL_SECONDS
                )
                if small.getbuffer().nbytes <= MAX_UPLOAD_BYTES:
                    gif_buf = small
                else:
                    raise RuntimeError("gif-too-large")
            except Exception:
                final_png = _render_static_wheel(self.options, winner_idx, size=self.size)
                final_embed = discord.Embed(title="🎡 Wheel Result", description=f"**Winner:** {winner_rarity}")
                final_embed.set_image(url="attachment://result.png")
                await interaction.edit_original_response(
                    embed=final_embed,
                    attachments=[discord.File(final_png, filename="result.png")],
                    view=self
                )
                self.spinning = False
                button.disabled = False
                return

        # Show ONLY the GIF (single attachment)
        spinning = discord.Embed(title="🎡 Spinning…", description="Good luck!")
        spinning.set_image(url="attachment://spin.gif")
        await interaction.edit_original_response(
            embed=spinning,
            attachments=[discord.File(io.BytesIO(gif_buf.getvalue()), filename="spin.gif")],
            view=self
        )

        # Wait until mid-tail so there’s no replay flash
        await asyncio.sleep(PAD_SECONDS + SPIN_SECONDS + (TAIL_SECONDS * 0.5))

        # Pick & award a random card of the winner rarity
        prize = _pick_random_card_by_rarity(self.state, winner_rarity)
        prize_line = ""
        if prize:
            await _award_card_to_user(self.state, interaction.user.id, prize, qty=1)
            pname = prize.get("name") or prize.get("cardname") or "Unknown Card"
            pset  = prize.get("set")  or prize.get("cardset")  or "Unknown Set"
            prize_line = f"\n**Prize:** {pname} — *{pset}*"
        else:
            prize_line = "\n*(No prize source found for that rarity — contact an admin.)*"

        # Send the final result as a NEW message with a fresh view…
        result_png = _render_static_wheel(self.options, winner_idx, size=self.size)
        result = discord.Embed(title="🎡 Wheel Result", description=f"**Winner:** {winner_rarity}{prize_line}")
        result.set_image(url="attachment://result.png")
        new_view = WheelView(
            author_id=self.author_id,
            options=self.options,
            private=self.private,
            size=self.size,
            state=self.state
        )
        await interaction.followup.send(
            embed=result,
            file=discord.File(result_png, filename="result.png"),
            view=new_view,
            ephemeral=self.private
        )

        # …then remove the spinning message to avoid any frame-1 flash
        try:
            await interaction.delete_original_response()
        except Exception:
            pass

        self.spinning = False  # ready for the next spin (on the new view)

# ---------------- Cog ----------------
class RewardsWheel(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # Build weighted options once
        self.options = _parse_segments()

    @app_commands.command(name="wheel", description=f"Spin the rewards wheel (costs {WHEEL_COST_MB} Mambucks).")
    @app_commands.guilds(GUILD)
    @app_commands.describe(private="Send privately (ephemeral).")
    async def wheel(self, interaction: discord.Interaction, private: bool = False):
        await interaction.response.defer(ephemeral=private)

        # Ask for confirmation
        prompt = discord.Embed(
            title="🎰 Spend Mambucks?",
            description=f"Are you sure you want to spend **{WHEEL_COST_MB} Mambucks** to spin the rewards wheel?"
        )
        view = ConfirmView(author_id=interaction.user.id)
        await interaction.edit_original_response(embed=prompt, view=view)

        # Wait for the user's choice (or timeout)
        timeout = await view.wait()  # returns True if timed out
        if timeout or view.value is None:
            # Treat timeout as cancel
            await interaction.edit_original_response(
                embed=discord.Embed(description="User cancelled the rewards wheel spin (timed out)."),
                view=None
            )
            return

        if view.value is False:
            await interaction.edit_original_response(
                embed=discord.Embed(description="User cancelled the rewards wheel spin."),
                view=None
            )
            return

        # User said "Yes" — debit first
        err = await _debit_wallet_or_error(self.bot.state, interaction.user, WHEEL_COST_MB)
        if err:
            await interaction.edit_original_response(
                embed=discord.Embed(title="❌ Cannot spin", description=err),
                view=None
            )
            return

        # Replace with initial idle wheel + Spin button
        idle_png = _render_static_wheel(self.options, winner_idx=None, size=WHEEL_SIZE)
        file = discord.File(idle_png, filename="wheel.png")
        embed = discord.Embed(title="🎡 Rewards Wheel", description="Press **Spin** to choose your prize.")
        embed.set_image(url="attachment://wheel.png")
        view2 = WheelView(
            author_id=interaction.user.id,
            options=self.options,
            private=private,
            size=WHEEL_SIZE,
            state=self.bot.state
        )
        await interaction.edit_original_response(embed=embed, attachments=[file], view=view2)


async def setup(bot: commands.Bot):
    await bot.add_cog(RewardsWheel(bot))
