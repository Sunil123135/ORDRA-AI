from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List

from ordra.policies.evaluator import PolicyEvaluator


class PolicySimulator:
    """What-if policy simulation for governance tuning."""

    def __init__(self, evaluator: PolicyEvaluator) -> None:
        self.evaluator = evaluator

    def simulate(self, facts: Dict[str, Any], policy_patch: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Evaluate policies with a patch applied.
        policy_patch = { "confidence_policy.auto_post_threshold": 0.90 }
        """
        simulated = deepcopy(self.evaluator.policies)
        for path, value in policy_patch.items():
            keys = path.split(".")
            ref = simulated
            for k in keys[:-1]:
                ref = ref.setdefault(k, {})
            ref[keys[-1]] = value
        sim_eval = PolicyEvaluator.from_dict(simulated)
        return sim_eval.evaluate(facts)
