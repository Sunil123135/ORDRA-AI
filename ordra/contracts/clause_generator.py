from __future__ import annotations

from typing import Any, Callable, Dict, Optional


class ContractClauseGenerator:
    """Generate legal contract clauses from automation policies."""

    def __init__(self, llm: Optional[Any] = None) -> None:
        self.llm = llm

    def generate(self, policies: Dict[str, Any]) -> str:
        """
        Convert automation policies into clear, enforceable enterprise contract clauses.
        If no LLM is set, returns a static template clause.
        """
        if self.llm is not None and hasattr(self.llm, "generate_text"):
            prompt = f"""
You are a legal contracts expert.

Convert the following automation policies into
clear, enforceable enterprise contract clauses.

[POLICIES]
{policies}

[REQUIREMENTS]
- Formal legal tone
- Clear accountability
- Define when AUTO_POST is allowed
- Define liability boundaries
"""
            return self.llm.generate_text(prompt)

        return (
            "Automatic Order Creation shall be enabled only when the system confidence score "
            "exceeds 95% and the Customer Trust Tier is SILVER or higher. "
            "Any deviation or exception shall require manual review. "
            "Liability boundaries are defined in the Order Processing Agreement."
        )
