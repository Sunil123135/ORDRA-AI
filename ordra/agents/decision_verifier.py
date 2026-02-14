from __future__ import annotations

from typing import Any, Dict, Tuple

from ordra.agents.verifier_agent import GeminiVerifier


class DecisionVerifier:
    """
    Safety-only verifier.
    Can ONLY downgrade AUTO_POST to CS_REVIEW.
    Never upgrades.
    """

    def __init__(self) -> None:
        self.verifier = GeminiVerifier()

    def verify(
        self,
        decision: Dict[str, Any],
        validated_order: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Tuple[bool, str]:
        """
        Returns (allow, reason).
        allow=False means veto AUTO_POST → force HITL.
        """
        action = decision.get("action")

        if action != "AUTO_POST":
            return True, "Non-AUTO_POST action allowed"

        query = "Is it SAFE to automatically create this SAP sales order?"
        draft = str({
            "decision": decision,
            "validated_order": validated_order,
        })[:4000]

        ctx_str = (
            f"Issues={validated_order.get('issues')}\n"
            f"Confidence={decision.get('confidence_overall')}\n"
            f"Policy={decision.get('policy_flags', {})}"
        )

        score, critique = self.verifier.verify(query, draft, ctx_str)

        if score < 85:
            return False, f"Decision vetoed by verifier: {critique}"

        return True, "Decision safe"
