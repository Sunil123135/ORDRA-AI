"""
Skill registry: Markdown-as-code for customer PO layouts, SAP rules, OCR playbook, order policy.
Loaded into prompt context based on customer_candidate, doc_type, issue_code.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional


def _default_skills_dir() -> Path:
    return Path(__file__).resolve().parent / "content"


def load_skills_for_context(
    customer_id: Optional[str] = None,
    doc_type: Optional[str] = None,
    issue_codes: Optional[List[str]] = None,
    *,
    skills_dir: Optional[Path] = None,
) -> str:
    """
    Load and concatenate skill Markdown relevant to this context.
    - customer_id: e.g. ACME_COM for customer-specific PO layout hints
    - doc_type: pdf_scan | digital
    - issue_codes: e.g. MAT_UNMAPPED, CREDIT_BLOCK for remediation hints
    """
    base = skills_dir or _default_skills_dir()
    if not base.is_dir():
        return ""

    parts: List[str] = []

    # Always include order_policy and sap_validation_rules if present
    for name in ("order_policy.md", "sap_validation_rules.md", "ocr_playbook.md"):
        p = base / name
        if p.is_file():
            try:
                parts.append(p.read_text(encoding="utf-8").strip())
            except Exception:
                pass

    # Customer-specific layout hints
    if customer_id:
        safe_id = customer_id.replace(" ", "_").upper()
        for candidate in (base / f"customer_{safe_id}.md", base / f"customer_{safe_id.lower()}.md"):
            if candidate.is_file():
                try:
                    parts.append(f"[Customer layout hints - {customer_id}]\n" + candidate.read_text(encoding="utf-8").strip())
                except Exception:
                    pass
                break

    # Doc-type specific (e.g. scanned vs digital)
    if doc_type:
        p = base / f"doc_{doc_type}.md"
        if p.is_file():
            try:
                parts.append(p.read_text(encoding="utf-8").strip())
            except Exception:
                pass

    return "\n\n---\n\n".join(parts) if parts else ""
