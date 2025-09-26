from __future__ import annotations

import os
import re
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from core.cards_shop import ensure_shop_index

API_URL = "https://db.ygoprodeck.com/api/v7/cardinfo.php"

# ------------------------ small utils ----------------------------------------

def _slugify(name: str, max_len: int = 100) -> str:
    base = re.sub(r"[^A-Za-z0-9]+", "_", (name or "").strip()).strip("_")
    return (base or "card")[:max_len]

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _has_existing_art(base: Path, slug: str) -> bool:
    """Return True if any file for this slug already exists (prefer .jpg)."""
    for ext in ("jpg", "jpeg", "png", "webp"):
        if (base / f"{slug}.{ext}").exists():
            return True
    return False

# ------------------------ scan state (cardpool) ----------------------

def collect_cardpool_from_state(state) -> Tuple[Dict[int, str], List[str]]:
    """Return ({id: name}, [names_without_numeric_id]) for the full card pool."""

    ensure_shop_index(state)
    ids_map: Dict[int, str] = {}
    names_no_id: set[str] = set()
    for card in getattr(state, "_shop_print_by_key", {}).values():
        name = (card.get("name") or card.get("cardname") or "").strip()
        if not name:
            continue

        raw_id = (
            card.get("id")
            or card.get("cardid")
            or card.get("card_id")
            or card.get("passcode")
        )
        sid = str(raw_id).strip() if raw_id is not None else ""
        if sid.isdigit():
            ids_map.setdefault(int(sid), name)
        else:
            names_no_id.add(name)
    # ensure we don't redundantly try to resolve names already covered via ID
    resolved_names = set(ids_map.values())
    names_without_id = [n for n in names_no_id if n not in resolved_names]

    return ids_map, names_without_id

# ------------------------ YGOPRODeck lookups ---------------------------------

def get_full_image_urls_for_ids(ids: Iterable[int | str]) -> Dict[int, str]:
    """Given numeric IDs, return {id: image_url} (normal-size)."""
    ids = [int(x) for x in ids if str(x).strip()]
    if not ids:
        return {}
    out: Dict[int, str] = {}
    BATCH = 50

    for i in range(0, len(ids), BATCH):
        chunk = ids[i:i+BATCH]
        try:
            resp = requests.get(API_URL, params={"id": ",".join(map(str, chunk))}, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
        except requests.RequestException as e:
            raise RuntimeError(f"YGOPRODeck request failed for ids {chunk}: {e}") from e

        for entry in (payload.get("data") or []):
            cid = entry.get("id")
            imgs = entry.get("card_images") or []
            url = None
            if imgs:
                url = imgs[0].get("image_url") or imgs[0].get("image_url_small")
            if cid is not None and url:
                out[int(cid)] = url
    return out

def get_full_image_url_for_name(name: str) -> Optional[str]:
    """Exact-name lookup → normal-size image URL (or None)."""
    try:
        resp = requests.get(API_URL, params={"name": name}, timeout=20)
        resp.raise_for_status()
        payload = resp.json()
    except requests.RequestException:
        return None
    data = payload.get("data") or []
    if not data:
        return None
    imgs = data[0].get("card_images") or []
    if not imgs:
        return None
    return imgs[0].get("image_url") or imgs[0].get("image_url_small")

# ------------------------ Downloader -----------------------------------------

def _download_file(url: str, dest: Path, *, overwrite: bool = True) -> bool:
    if dest.exists() and not overwrite:
        return True
    try:
        with requests.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            tmp = dest.with_suffix(dest.suffix + ".part")
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        f.write(chunk)
            tmp.replace(dest)
        return True
    except requests.RequestException:
        return False

# ------------------------ Main entrypoint ------------------------------------

def download_cardpool_art_from_state(
    state,
    out_dir: str | os.PathLike | None = None,
    *,
    fallback_to_name: bool = True,
    polite_delay_sec: float = 0.1,   # between name lookups
) -> List[Path]:
    """
    From the loaded shop/starter indexes:
      - collect every card available in the card pool
      - fetch normal image URLs (ID first; optional exact-name fallback)
      - write to images/card_images/<card_name>.jpg
        (🆕 if the file already exists, skip download — NO -<id> suffixing)
    Returns list of written Path objects.
    """
    out_base = Path(out_dir) if out_dir else (Path(__file__).resolve().parents[1] / "images" / "card_images")
    _ensure_dir(out_base)

    ids_map, names_wo_id = collect_cardpool_from_state(state)
    if not ids_map and not names_wo_id:
        print("[art] No cards found in state cardpool.")
        return []

    urls_by_id = get_full_image_urls_for_ids(ids_map.keys())

    # name fallback: for (1) cards with no id, (2) ids that failed to resolve
    names_to_resolve: List[str] = names_wo_id[:]
    for cid, nm in ids_map.items():
        if cid not in urls_by_id:
            names_to_resolve.append(nm)

    urls_by_name: Dict[str, str] = {}
    if fallback_to_name and names_to_resolve:
        seen = set()
        for name in names_to_resolve:
            if name in seen:
                continue
            seen.add(name)
            url = get_full_image_url_for_name(name)
            if url:
                urls_by_name[name] = url
            if polite_delay_sec:
                time.sleep(polite_delay_sec)

    written: List[Path] = []
    seen_slugs: set[str] = set()

    # Save all with IDs (prefer id URL, fallback to name URL)
    for cid, name in ids_map.items():
        url = urls_by_id.get(cid) or urls_by_name.get(name)
        if not url:
            continue
        slug = _slugify(name)
        if slug in seen_slugs or _has_existing_art(out_base, slug):
            # Skip because <slug>.jpg (or another ext) already exists
            continue
        dest = out_base / f"{slug}.jpg"
        if _download_file(url, dest):
            written.append(dest)
            seen_slugs.add(slug)

    # Save name-only entries not already present
    for name in names_wo_id:
        slug = _slugify(name)
        if slug in seen_slugs or _has_existing_art(out_base, slug):
            continue
        url = urls_by_name.get(name)
        if not url:
            continue
        dest = out_base / f"{slug}.jpg"
        if _download_file(url, dest):
            written.append(dest)
            seen_slugs.add(slug)

    total_candidates = len(ids_map) + len(names_wo_id)
    print(
        f"[art] Downloaded {len(written)} images (of {total_candidates} candidates) → {out_base}"
    )
    return written