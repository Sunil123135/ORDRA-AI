# ORDRA-AI — Cursor Task File (Execution Script)

Goal: Working capstone slice — Email/PO ingest → extract → validate (mock) → decide → HITL → (mock) SAP create → audit & deck.

## Global Rules

- All LLM outputs MUST validate against JSON Schema.
- No hallucinations: missing values must be null + uncertainty_reason.
- AUTO_POST is gated by safe-to-post policy in code (not only LLM).
- Every job produces an AuditRecord.
- Build MVP with SAP mock; keep interfaces pluggable for real SAP later.

## Repo Setup

- Python 3.11+, FastAPI, Streamlit, SQLite, FAISS (optional/stub), NetworkX.

## Implemented (MVP)

- **TASK 0–2**: Repo structure, core schemas, SQLite + JobService (with `get_latest_approved_overrides`).
- **TASK 3–4**: Stub document ingestion, LLM layer (OpenAI + schema validation, stub when no key).
- **TASK 5–6**: Mock SAP validation, decision gate (hard guardrails), HITL.
- **TASK 7**: NetworkX DAG from YAML + ParallelDAGExecutor with retries.
- **TASK 8**: API endpoints (intake, run, jobs, audit, review/complete).
- **TASK 9**: Streamlit UI (Inbox Triage, CS Review with Overrides JSON, Decision Deck).
- **Patch**: CS override → re-run: overrides injected in run, `sap_validate_materials` applies `human_overrides.material_mappings`, HITL payload includes `override_template`.

## Optional next (Phase 2)

- **TASK 10**: FAISS memory for alias resolution and layout hints.
- Real SAP connectors (BAPI/OData), Outlook add-in, Fiori panel.

## Demo scenarios (README)

1. Happy path auto-post (all validations pass).
2. Unmapped material → CS review → approve with `material_mappings` → re-run → AUTO_POST.
3. Credit blocked → HOLD + audit.
