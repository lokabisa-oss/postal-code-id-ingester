from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
from datetime import datetime, timezone


@dataclass
class AugmentedPostalCode:
    village_code: str
    postal_code: str
    source: str
    confidence: float
    retrieved_at: str
    raw: Optional[Dict[str, Any]] = None

    @staticmethod
    def now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()
