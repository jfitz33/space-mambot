from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict

DEFAULT_BANLIST_PATH = Path(__file__).resolve().parents[1] / "data" / "banlist.json"


def _normalize_card_name(value: str | None) -> str:
    if not value:
        return ""
    return str(value).strip().lower()


def _coerce_limit(raw) -> int | None:
    try:
        limit = int(raw)
    except (TypeError, ValueError):
        return None
    return max(0, limit)


def _store_limit(identifier, limit_value, name_map: Dict[str, int]) -> None:
    if identifier is None:
        return
    limit = _coerce_limit(limit_value)
    if limit is None:
        return
    key = str(identifier).strip()
    if not key:
        return
    norm_name = _normalize_card_name(key)
    if norm_name:
        name_map[norm_name] = limit


@dataclass
class Banlist:
    """Simple structure storing copy limits for cards."""

    default_limit: int = 3
    limits_by_name: Dict[str, int] = field(default_factory=dict)
    def limit_for(self, card_name: str | None) -> int:
        """Return the allowed copy count for a card name."""
        if not card_name:
            return self.default_limit
        name_key = _normalize_card_name(card_name)
        if name_key and name_key in self.limits_by_name:
            return self.limits_by_name[name_key]
        return self.default_limit


def load_banlist(path: str | Path | None = None) -> Banlist:
    """Load banlist data from JSON."""
    file_path = Path(path) if path else DEFAULT_BANLIST_PATH
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
                    _store_limit(
                        identifier,
                        fallback_limit if effective is None else effective,
                        limits_by_name,
                    )
            elif isinstance(entries, list):
                for identifier in entries:
                    _store_limit(identifier, fallback_limit, limits_by_name)
            else:
                _store_limit(entries, fallback_limit, limits_by_name)

        limits_section = data.get("limits")
        if isinstance(limits_section, dict):
            for identifier, limit_value in limits_section.items():
                _store_limit(identifier, limit_value, limits_by_name)

        if not limits_by_name:
            reserved_keys = {"default_limit", "limits"} | {
                key for key, _ in category_defaults
            }
            for identifier, limit_value in data.items():
                if identifier in reserved_keys:
                    continue
                _store_limit(identifier, limit_value, limits_by_name)

    return Banlist(
        default_limit=default_limit,
        limits_by_name=limits_by_name,
    )


__all__ = ["Banlist", "load_banlist"]
