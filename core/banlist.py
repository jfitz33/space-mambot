from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict

DEFAULT_BANLIST_PATH = Path(__file__).resolve().parents[1] / "data" / "banlist.json"


def _normalize_card_id(value: str | int | None) -> str:
    """Return a canonical representation for a card identifier."""
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.isdigit():
        # Remove leading zeros so 000123 and 123 match.
        try:
            return str(int(text))
        except ValueError:
            # Fall back to the raw string if conversion fails unexpectedly.
            pass
    return text.lower()


def _coerce_limit(raw) -> int | None:
    try:
        limit = int(raw)
    except (TypeError, ValueError):
        return None
    return max(0, limit)


def _store_limit(
    identifier,
    limit_value,
    id_map: Dict[str, int],
    name_map: Dict[str, int],
) -> None:
    if identifier is None:
        return
    limit = _coerce_limit(limit_value)
    if limit is None:
        return
    key = str(identifier).strip()
    if not key:
        return
    norm_id = _normalize_card_id(key)
    if norm_id.isdigit():
        id_map[norm_id] = limit
    elif norm_id:
        name_map[norm_id] = limit


@dataclass
class Banlist:
    """Simple structure storing copy limits for cards."""

    default_limit: int = 3
    limits_by_id: Dict[str, int] = field(default_factory=dict)
    limits_by_name: Dict[str, int] = field(default_factory=dict)

    def limit_for(self, *, card_id: str | None = None, card_name: str | None = None) -> int:
        """Return the allowed copy count for a card."""
        if card_id:
            norm_id = _normalize_card_id(card_id)
            if norm_id and norm_id in self.limits_by_id:
                return self.limits_by_id[norm_id]
        if card_name:
            name_key = (card_name or "").strip().lower()
            if name_key and name_key in self.limits_by_name:
                return self.limits_by_name[name_key]
        return self.default_limit


def load_banlist(path: str | Path | None = None) -> Banlist:
    """Load banlist data from JSON, supporting several simple schemas."""
    file_path = Path(path) if path else DEFAULT_BANLIST_PATH
    limits_by_id: Dict[str, int] = {}
    limits_by_name: Dict[str, int] = {}
    default_limit = 3

    data: dict | None = None
    try:
        with open(file_path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        data = None
    except json.JSONDecodeError:
        data = None
    except OSError:
        data = None

    if isinstance(data, dict):
        parsed_default = _coerce_limit(data.get("default_limit"))
        if parsed_default is not None:
            default_limit = parsed_default

        cards_section = data.get("cards")
        if isinstance(cards_section, dict):
            for identifier, limit_value in cards_section.items():
                _store_limit(identifier, limit_value, limits_by_id, limits_by_name)

        # Support category-based schemas (forbidden/limited/semi-limited).
        category_defaults = [
            ("forbidden", 0),
            ("limited", 1),
            ("semi_limited", 2),
            ("semi-limited", 2),
            ("semi", 2),
        ]
        for key, fallback_limit in category_defaults:
            entries = data.get(key)
            if entries is None:
                continue
            if isinstance(entries, dict):
                for identifier, limit_value in entries.items():
                    effective = _coerce_limit(limit_value)
                    _store_limit(identifier, fallback_limit if effective is None else effective, limits_by_id, limits_by_name)
            elif isinstance(entries, list):
                for identifier in entries:
                    _store_limit(identifier, fallback_limit, limits_by_id, limits_by_name)
            else:
                _store_limit(entries, fallback_limit, limits_by_id, limits_by_name)

        # If no explicit "cards" mapping existed, attempt to treat remaining key/value
        # pairs as direct limits.
        if not limits_by_id and not limits_by_name:
            reserved_keys = {"default_limit"} | {k for k, _ in category_defaults} | {"cards"}
            for identifier, limit_value in data.items():
                if identifier in reserved_keys:
                    continue
                _store_limit(identifier, limit_value, limits_by_id, limits_by_name)

    return Banlist(default_limit=default_limit, limits_by_id=limits_by_id, limits_by_name=limits_by_name)


__all__ = ["Banlist", "load_banlist"]
