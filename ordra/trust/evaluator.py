from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

from ordra.trust.trust_score import TrustScoreEngine, tier_from_score


class CustomerTrustEvaluator:
    """Evaluates customer trust tier from episodic memory (with optional decay)."""

    def __init__(
        self,
        trust_path: str,
        episode_store: Any,
        decay_path: str | None = None,
    ) -> None:
        path = Path(trust_path)
        self.tiers = {}
        if path.is_file():
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            self.tiers = data.get("tiers", {})
        self.store = episode_store
        self.decay_engine = TrustScoreEngine.from_yaml(decay_path) if decay_path and Path(decay_path).is_file() else None

    def evaluate(self, customer_key: str) -> Tuple[str, int]:
        """
        Returns (tier, clean_count or score).
        Uses trust score with decay if configured; else clean AUTO_POST count.
        """
        result = self.store.retrieve_recipes(customer_key=customer_key, limit=50)
        recipes = result.get("recipes", [])

        if self.decay_engine:
            score = self.decay_engine.compute_score(recipes)
            return tier_from_score(score), score

        clean = [
            e for e in recipes
            if e.get("outcome") == "AUTO_POST" and not e.get("human_overrides_used")
        ]
        clean_count = len(clean)
        current = "BRONZE"
        for tier, cfg in self.tiers.items():
            if clean_count >= cfg.get("min_clean_episodes", 0):
                current = tier
        return current, clean_count
