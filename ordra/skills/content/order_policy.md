# Order policy (mandatory fields + downgrade rules)

- **Mandatory header fields**: PO number, PO date, Sold-to, Ship-to. If any missing or UNKNOWN → ASK_CUSTOMER.
- **Mandatory per line**: Customer material, Quantity, UOM. Unmapped material → CS_REVIEW; CS may provide material_mappings override.
- **Credit**: If credit blocked → HOLD (Finance). If not validated → CS_REVIEW.
- **Auto-post allowlist**: Only customers with stable history; verifier can veto AUTO_POST to CS_REVIEW.
