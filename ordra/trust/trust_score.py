from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Type, TypeVar

import yaml

T = TypeVar("T", bound="TrustScoreEngine")


def tier_from_score(score: int) -> str:
    """Map numeric trust score to tier."""
    if score >= 90:
        return "GOLD"
    if score >= 75:
        return "SILVER"
    return "BRONZE"


class TrustScoreEngine:
    """Dynamic trust score with decay (mistakes lower tier)."""

    def __init__(self, decay_cfg: Dict[str, Any]) -> None:
        self.decay = decay_cfg.get("decay_rules", [])
        self.floor = decay_cfg.get("floor", {}).get("minimum_score", 0)

    @classmethod
    def from_yaml(cls: Type[T], path: str) -> T:
        p = Path(path)
        if not p.is_file():
            return cls({})
        with open(p, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        return cls(cfg)

    def compute_score(self, episodes: List[Dict[str, Any]]) -> int:
        score = 100
        for e in episodes:
            if e.get("outcome") != "AUTO_POST":
                continue
            if e.get("human_overrides_used"):
                score -= self._penalty("HITL_OVERRIDE")
            if e.get("sap_post_failed"):
                score -= self._penalty("AUTO_POST_ERROR")
        return max(score, self.floor)

    def _penalty(self, trigger: str) -> int:
        for r in self.decay:
            if r.get("trigger") == trigger:
                return int(r.get("penalty", 0))
        return 0
