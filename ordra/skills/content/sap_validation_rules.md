# Common SAP validation rules (pricing, credit, ATP)

- **Pricing**: Line-level pricing checked against condition types; mismatches → CS_REVIEW.
- **Credit**: Credit block forces HOLD; partial credit may allow CS_REVIEW with override.
- **ATP**: Shortage → warn; derive plant/route from sold-to and material; partial delivery possible.
- **Materials**: Customer material must map to SAP material number; use human_overrides.material_mappings for CS-approved mapping.
