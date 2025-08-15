from dataclasses import dataclass, field
from typing import Dict, Any

@dataclass
class AppState:
    db_path: str
    packs_dir: str
    packs_index: Dict[str, dict] = field(default_factory=dict)  # pack_name -> {"by_rarity": {...}}
    cfg: Dict[str, Any] = field(default_factory=dict)
