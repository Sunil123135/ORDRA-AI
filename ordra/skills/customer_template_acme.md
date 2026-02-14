---
name: customer-template-acme
description: Extraction hints for ACME customer PO layouts
intent_triggers: ["customer:acme_com"]
---

# ACME PO Layout Hints

- PO Number appears near the top-right as: "Purchase Order No:"
- Ship-to block starts with "Deliver To:" and includes a 6-digit code.
- Line items table headers: "Item", "Part No", "Qty", "UOM", "Req Date"
- If multiple pages, line items continue without repeating headers; detect column alignment.

Extraction rules:
- Always capture Ship-to code even if address spans multiple lines.
- If Req Date is missing per line, fall back to header "Delivery Date".
