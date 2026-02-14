from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml


class ConfidencePricing:
    """Confidence-band pricing for usage-based billing."""

    def __init__(self, pricing_path: str) -> None:
        path = Path(pricing_path)
        if not path.is_file():
            self.cfg = {"bands": []}
            return
        with open(path, "r", encoding="utf-8") as f:
            self.cfg = yaml.safe_load(f) or {}

    def price(self, confidence: float) -> Dict[str, Any]:
        for band in self.cfg.get("bands", []):
            if band["min_confidence"] <= confidence <= band["max_confidence"]:
                return {
                    "price": band["price_per_order"],
                    "tier": band.get("description", "unknown"),
                }
        return {"price": 0.0, "tier": "unknown"}
