"""Resolve sender email to customer_key and SAP sold-to/ship-to from config (no hardcoded emails)."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import yaml


def _default_cfg_path() -> Path:
    return Path(__file__).resolve().parent.parent / "config" / "customer_identity.yaml"


class CustomerIdentityResolver:
    def __init__(self, cfg_path: Optional[str] = None) -> None:
        path = _default_cfg_path() if not cfg_path else Path(cfg_path)
        if not path.is_absolute():
            path = _default_cfg_path().parent / path.name
        try:
            raw = path.read_text(encoding="utf-8")
            self.cfg = yaml.safe_load(raw) or {}
        except FileNotFoundError:
            self.cfg = {}

    def resolve(self, email_from: Optional[str]) -> Optional[Dict[str, Any]]:
        email_from = (email_from or "").strip().lower()
        if not email_from:
            return None

        for c in self.cfg.get("customers") or []:
            addrs = [e.lower() for e in c.get("email_addresses") or []]
            if email_from in addrs:
                return {
                    "customer_key": c["customer_key"],
                    "sold_to": c["sap_mapping"]["sold_to"],
                    "ship_to": c["sap_mapping"]["ship_to"],
                    "trust_tier_override": c.get("trust_tier_override"),
                    "auto_post_enabled": c.get("auto_post_enabled"),
                    "resolution": "EXACT_EMAIL_MATCH",
                }
            for domain in c.get("allowed_domains") or []:
                if email_from.endswith("@" + domain.lower()):
                    return {
                        "customer_key": c["customer_key"],
                        "sold_to": c["sap_mapping"]["sold_to"],
                        "ship_to": c["sap_mapping"]["ship_to"],
                        "trust_tier_override": c.get("trust_tier_override"),
                        "auto_post_enabled": c.get("auto_post_enabled"),
                        "resolution": "DOMAIN_MATCH",
                    }
        return None
