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
from core.db import db_wallet_get, db_wallet_try_spend_mambucks, db_add_cards, db_wheel_tokens_get, db_wheel_tokens_try_spend
from core.cards_shop import card_label

# ---------------- Guild scope ----------------
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

WHEEL_TOKEN_COST = 1  # cost per spin

# ---------------- Tunables ----------------
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
    ("COMMON", 3),
    ("RARE", 2),
    ("SUPER RARE", 2),
    ("ULTRA RARE", 1),
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

def _build_rarity_pools_from_state(state) -> dict[str, list[tuple[str, dict]]]:
    """
    Collect all printings by normalized rarity across all packs,
    returning mapping rarity -> list of (set_name, printing_dict).
    """
    pools: dict[str, list[tuple[str, dict]]] = {
        "COMMON": [], "RARE": [], "SUPER RARE": [], "ULTRA RARE": [], "SECRET RARE": []
    }

    def add_from(idx: dict | None):
        if not isinstance(idx, dict):
            return
        for set_name, pack in idx.items():
            br = pack.get("by_rarity") if isinstance(pack, dict) else None
            if not isinstance(br, dict):
                continue
            for raw_key, lst in br.items():
                norm = _normalize_rarity(raw_key)
                if isinstance(lst, list):
                    for it in lst:
                        pools.setdefault(norm, []).append((set_name, it))

    add_from(getattr(state, "packs_index", None))
    add_from(getattr(state, "starters_index", None) or getattr(state, "starters", None))
    return pools

def _pick_random_card_by_rarity(state, rarity: str) -> Optional[tuple[str, dict]]:
    """
    Returns (set_name, printing) or None. Degrades rarity if bucket is empty.
    """
    want = _normalize_rarity(rarity)
    pools = _build_rarity_pools_from_state(state)

    degrade_order = {
        "SECRET RARE": ["SECRET RARE", "ULTRA RARE", "SUPER RARE", "RARE", "COMMON"],
        "ULTRA RARE":  ["ULTRA RARE",  "SUPER RARE", "RARE", "COMMON"],
        "SUPER RARE":  ["SUPER RARE",  "RARE", "COMMON"],
        "RARE":        ["RARE", "COMMON"],
        "COMMON":      ["COMMON"],
    }

    for bucket in degrade_order.get(want, ["COMMON"]):
        candidates = pools.get(bucket) or []
        if candidates:
            return random.choice(candidates)  # (set_name, printing)
    return None

async def _award_card_to_user(state, user_id: int, printing: dict, set_name: str, qty: int = 1) -> None:
    """Insert the exact printing with the correct set into the user's collection."""
    try:
        # db_add_cards(state, user_id, [printing, printing, ...], set_name)
        db_add_cards(state, user_id, [printing] * int(max(1, qty)), set_name)
    except Exception as e:
        print(f"[rewards] db_add_to_collection failed: {e}")

# ---------------- Views ----------------
class WheelTokenConfirmView(discord.ui.View):
    """
    Shows 'You have N tokens. Spend 1 to spin?' and, on Yes, spends 1 token and
    calls the provided on_confirm(interaction) to start your existing wheel spin.
    """
    def __init__(self, state, requester: discord.Member, on_confirm, *, timeout: float = 90):
        super().__init__(timeout=timeout)
        self.state = state
        self.requester = requester
        self.on_confirm = on_confirm
        self._processing = False

    def _title(self) -> str:
        bal = db_wheel_tokens_get(self.state, self.requester.id)
        return f"You have **{bal}** wheel token(s). Spend **1** to spin?"

    async def send_or_update(self, interaction: discord.Interaction):
        # Helper to send the panel with up-to-date balance
        embed = discord.Embed(title="Spin the Wheel", description=self._title(), color=0x2b6cb0)
        try:
            await interaction.response.send_message(embed=embed, view=self, ephemeral=True)
        except discord.InteractionResponded:
            await interaction.followup.send(embed=embed, view=self, ephemeral=True)

    @discord.ui.button(label="Yes, spin (cost: 1 token)", style=discord.ButtonStyle.success)
    async def yes(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.requester.id:
            return await interaction.response.send_message("This isn’t for you.", ephemeral=True)
        if self._processing:
            try: await interaction.response.defer_update()
            except: pass
            return
        self._processing = True

        # Try to spend a token
        new_bal = db_wheel_tokens_try_spend(self.state, self.requester.id, 1)
        if new_bal is None:
            self._processing = False
            # Refresh the panel to show correct balance
            return await interaction.response.edit_message(
                embed=discord.Embed(
                    title="Spin the Wheel",
                    description="❌ Not enough tokens.\n" + self._title(),
                    color=0xe53e3e,
                ),
                view=self
            )

        # Remove the buttons, proceed to spin
        self.stop()
        for ch in self.children: ch.disabled = True
        try:
            await interaction.response.edit_message(
                embed=discord.Embed(title="Spinning...", description="Good luck! 🎡", color=0x2b6cb0),
                view=None
            )
        except discord.InteractionResponded:
            pass

        try:
            await self.on_confirm(interaction)  # ← call your existing spin flow here
        finally:
            self._processing = False

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def no(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.requester.id:
            return await interaction.response.send_message("This isn’t for you.", ephemeral=True)
        self.stop()
        for ch in self.children: ch.disabled = True
        await interaction.response.edit_message(
            embed=discord.Embed(title="Spin cancelled.", color=0x718096),
            view=None
        )

class WheelView(discord.ui.View):
    def __init__(self, author_id: int, options: List[str], size: int, state):
        super().__init__(timeout=180.0)
        self.author_id = author_id
        self.options = options
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
        picked = _pick_random_card_by_rarity(self.state, winner_rarity)
        if picked:
            set_name, printing = picked
            await _award_card_to_user(self.state, interaction.user.id, printing, set_name, qty=1)
            prize_line = f"\n**Prize:** {card_label(printing)}"
        else:
            prize_line = "\n*(No prize source found for that rarity — contact an admin.)*"

        # Send the final result as a NEW message with a fresh view…
        result_png = _render_static_wheel(self.options, winner_idx, size=self.size)
        result = discord.Embed(title="🎡 Wheel Result", description=f"**Winner:** {winner_rarity}{prize_line}")
        result.set_image(url="attachment://result.png")

        # no further spin button offered here:
        await interaction.followup.send(
            embed=result,
            file=discord.File(result_png, filename="result.png"),
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

    async def _show_wheel_idle(self, interaction: discord.Interaction):
        # Use the message that contained the confirm buttons
        msg = interaction.message

        # Render the static wheel (your existing helper)
        idle_png = _render_static_wheel(self.options, winner_idx=None, size=WHEEL_SIZE)
        file = discord.File(idle_png, filename="wheel.png")

        embed = discord.Embed(
            title="🎡 Rewards Wheel",
            description="Press **Spin** to choose your prize.",
            color=0x2b6cb0,
        )
        embed.set_image(url="attachment://wheel.png")

        # Your existing interactive view that handles the actual spin & animation
        view = WheelView(
            author_id=interaction.user.id,
            options=self.options,
            size=WHEEL_SIZE,
            state=self.bot.state,
        )

        # Replace the confirm panel with the idle wheel + Spin button
        try:
            await msg.edit(content=None, embed=embed, attachments=[file], view=view)
        except discord.InteractionResponded:
            # If for some reason we can't edit that message, fall back to a followup
            await interaction.followup.send(embed=embed, file=file, view=view)

    @app_commands.command(name="wheel", description="Spin the rewards wheel (cost: 1 Wheel Token).")
    @app_commands.guilds(GUILD)
    async def wheel(self, interaction: discord.Interaction):
        # Make the initial interaction visible, but we won't finish the flow here—
        # the confirm view will handle the rest.
        await interaction.response.defer()

        # Show their current token balance up front
        tokens = db_wheel_tokens_get(self.bot.state, interaction.user.id)
        if tokens < WHEEL_TOKEN_COST:
            await interaction.edit_original_response(
                embed=discord.Embed(
                    title="❌ Not enough Wheel Tokens",
                    description=f"You need **{WHEEL_TOKEN_COST}** token to spin.\nYou currently have **{tokens}**."
                ),
                view=None
            )
            return

        # Build the confirmation prompt; the view will:
        #   - spend 1 token
        #   - then call our callback to display the wheel UI (idle + Spin button)
        prompt = discord.Embed(
            title="🎰 Spin the Rewards Wheel?",
            description=(
                f"Cost: **{WHEEL_TOKEN_COST}** Wheel Token\n"
                f"You currently have **{tokens}**.\n\n"
                "Proceed?"
            ),
            color=0x2b6cb0,
        )

        view = WheelTokenConfirmView(
            self.bot.state,
            interaction.user,
            # When the user clicks "Yes", the view spends the token and then calls this:
            on_confirm=lambda inter: self._show_wheel_idle(inter),
        )

        # show the confirm UI on the original deferred message
        await interaction.edit_original_response(embed=prompt, view=view)


async def setup(bot: commands.Bot):
    await bot.add_cog(RewardsWheel(bot))
