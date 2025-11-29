from dataclasses import dataclass, field
from typing import Dict, Any

@dataclass
class AppState:
    db_path: str
    packs_dir: str
    packs_index: Dict[str, dict] = field(default_factory=dict)  # pack_name -> {"by_rarity": {...}}
    tins_index: Dict[str, dict] = field(default_factory=dict)   # tin_name -> {"promo_cards": [...], "packs": [...]}
    cfg: Dict[str, Any] = field(default_factory=dict)
