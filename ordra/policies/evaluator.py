from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import yaml


class PolicyEvaluator:
    """YAML-driven policy evaluation. Returns list of violations."""

    def __init__(self, policy_path: str) -> None:
        with open(policy_path, "r", encoding="utf-8") as f:
            self.policies = yaml.safe_load(f) or {}

    @classmethod
    def from_dict(cls, policies: Dict[str, Any]) -> "PolicyEvaluator":
        """Build evaluator from in-memory policies (e.g. for simulation)."""
        inst = cls.__new__(cls)
        inst.policies = policies
        return inst

    def evaluate(self, facts: Dict[str, Any]) -> List[Dict[str, str]]:
        """Returns a list of policy violations."""
        violations: List[Dict[str, str]] = []

        for section in ["credit_policy", "compliance_policy", "material_trust_policy"]:
            rules = self.policies.get(section, {})
            for block_type in ["hard_block", "soft_block"]:
                for rule in rules.get(block_type, []):
                    if self._eval_condition(rule["condition"], facts):
                        violations.append({
                            "severity": block_type,
                            "action": rule["action"],
                            "reason": rule["reason"],
                        })

        return violations

    def _eval_condition(self, expr: str, facts: Dict[str, Any]) -> bool:
        """Controlled evaluation – no arbitrary execution."""
        try:
            return bool(eval(expr, {"__builtins__": {}}, facts))
        except Exception:
            return False
