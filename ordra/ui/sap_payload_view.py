"""Helpers for Streamlit SAP Payload Preview pane."""
from __future__ import annotations

import json
from typing import Any, Dict, List


def summarize_issues(issues: List[Dict[str, Any]]) -> Dict[str, int]:
    out = {"BLOCK": 0, "WARN": 0, "INFO": 0}
    for i in issues or []:
        sev = (i.get("severity") or "INFO").upper()
        if sev in out:
            out[sev] += 1
    return out


def pretty_json(obj: Any) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False, default=str)
