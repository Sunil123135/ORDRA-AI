from __future__ import annotations

import json
from json import JSONDecodeError

import requests
import streamlit as st

API_BASE = st.secrets.get("API_BASE", "http://localhost:8000")


def fetch_job(job_id: str):
    return requests.get(f"{API_BASE}/jobs/{job_id}", timeout=30).json()


def fetch_audit(job_id: str):
    r = requests.get(f"{API_BASE}/jobs/{job_id}/audit", timeout=30)
    if r.status_code != 200:
        return None
    return r.json()


def run_and_refresh(job_id: str):
    run_resp = requests.post(f"{API_BASE}/jobs/{job_id}/run", timeout=120)
    run_resp.raise_for_status()
    st.session_state.last_run = run_resp.json()
    st.session_state.job_cache = fetch_job(job_id)
    st.session_state.audit_cache = fetch_audit(job_id)


st.set_page_config(page_title="ORDRA-AI", layout="wide")

st.title("ORDRA-AI — Outlook Panel Mock + CS Review")

# --- Sidebar navigation (replaces st.tabs for auto-switching) ---
if "page" not in st.session_state:
    st.session_state.page = "Inbox Triage"

st.sidebar.title("ORDRA-AI Navigation")
_nav_options = ["Inbox Triage", "CS Review", "SAP Payload Preview", "Decision Deck"]
_nav_index = _nav_options.index(st.session_state.page) if st.session_state.page in _nav_options else 0
st.session_state.page = st.sidebar.radio("Go to", _nav_options, index=_nav_index)

# ----------------------------
# Inbox Triage
# ----------------------------
if st.session_state.page == "Inbox Triage":
    st.subheader("Inbox Triage — Create & Run Job")

    if "job_id" not in st.session_state:
        st.session_state.job_id = None
    if "last_run" not in st.session_state:
        st.session_state.last_run = None
    if "job_cache" not in st.session_state:
        st.session_state.job_cache = None
    if "audit_cache" not in st.session_state:
        st.session_state.audit_cache = None
    if "rerun_on_arrival" not in st.session_state:
        st.session_state.rerun_on_arrival = False

    # Auto re-run banner (demo feedback)
    if st.session_state.get("rerun_on_arrival"):
        st.warning("⏳ Auto re-run scheduled from CS approval…", icon="🔁")

    col1, col2 = st.columns(2)

    with col1:
        email_subject = st.text_input("Email subject", value="PO for supplies")
        email_from = st.text_input("From", value="buyer@acme.com")
        email_body = st.text_area("Email body", height=180, value="Please find attached PO for ACME Healthcare. Ship to ACME Mumbai.")

    with col2:
        st.caption("MVP: Paste extracted text (later replace with file upload + OCR).")
        pdf_text = st.text_area("PDF text", height=140, value="PO-48392\nPO Date: 02-Feb-2026\nShip To: ACME Mumbai\nLine: A-123 20 EA 2450 INR")
        ocr_text = st.text_area("OCR text (optional)", height=80, value="")
        excel_tables = st.text_area("Excel tables (optional)", height=80, value="")

    cta1, cta2, cta3, cta4, cta5 = st.columns([1, 1, 1, 1, 2])

    with cta1:
        if st.button("Create Job", use_container_width=True):
            resp = requests.post(
                f"{API_BASE}/intake",
                json={
                    "email_subject": email_subject,
                    "email_from": email_from,
                    "email_body": email_body,
                    "pdf_text": pdf_text,
                    "ocr_text": ocr_text,
                    "excel_tables": excel_tables
                },
                timeout=30,
            )
            resp.raise_for_status()
            st.session_state.job_id = resp.json()["job_id"]
            st.success(f"Job created: {st.session_state.job_id}")
            st.session_state.job_cache = fetch_job(st.session_state.job_id)
            st.session_state.audit_cache = fetch_audit(st.session_state.job_id)

    with cta2:
        if st.button("Run Job", use_container_width=True, disabled=not st.session_state.job_id):
            run_and_refresh(st.session_state.job_id)
            st.success("Job executed and refreshed.")

    with cta3:
        if st.button("Re-run last job", use_container_width=True, disabled=not st.session_state.job_id):
            run_and_refresh(st.session_state.job_id)
            st.success("Re-run completed and refreshed.")

    with cta4:
        if st.button("Re-run last job now", use_container_width=True, disabled=not st.session_state.job_id):
            st.session_state.rerun_on_arrival = True
            st.rerun()

    with cta5:
        st.info("Tip: If decision is CS_REVIEW / ASK_CUSTOMER / HOLD, go to CS Review tab, apply overrides, then re-run.")

    # Auto-trigger re-run once (used when returning from CS Review)
    if st.session_state.get("rerun_on_arrival") and st.session_state.get("job_id"):
        try:
            run_and_refresh(st.session_state.job_id)
            st.success("Auto re-run completed (post-approval).")
        except Exception as e:
            st.error(f"Auto re-run failed: {e}")
        finally:
            st.session_state.rerun_on_arrival = False

    if st.session_state.get("job_id"):
        st.divider()
        st.subheader("Latest Status (Auto-refreshed)")

        job = st.session_state.get("job_cache")
        if not job:
            try:
                job = fetch_job(st.session_state.job_id)
                st.session_state.job_cache = job
            except Exception:
                job = None

        if job:
            st.write("**Job ID:**", job.get("job_id"))
            st.write("**Status:**", job.get("status"))

            outputs = job.get("outputs", {}) or {}
            decision = outputs.get("decision") or {}
            st.write("**Decision:**", decision)

            sap_order_number = outputs.get("sap_order_number")
            hitl_task_id = outputs.get("hitl_task_id")

            if sap_order_number:
                st.success(f"SAP Order Created (mock): {sap_order_number}")
            elif hitl_task_id:
                st.warning(f"HITL Task created: {hitl_task_id}")
                col_nav1, col_nav2 = st.columns([1, 3])
                with col_nav1:
                    if st.button("Open CS Review", use_container_width=True):
                        st.session_state.page = "CS Review"
                        st.rerun()
                with col_nav2:
                    st.caption("Go to CS Review to approve/reject and add overrides, then re-run the job.")
            else:
                st.info("No SAP order or HITL task yet. Check decision/audit for details.")

            colL, colR = st.columns(2)
            with colL:
                st.markdown("### Extracted Order")
                st.json(outputs.get("extracted_order", {}))

            with colR:
                st.markdown("### Validated Order")
                st.json(outputs.get("validated_order", {}))

        st.subheader("Audit (Auto-refreshed)")
        audit = st.session_state.get("audit_cache")
        if audit is None:
            st.info("No audit found yet (or job failed before audit creation).")
        else:
            st.json(audit)

# ----------------------------
# SAP Payload Preview
# ----------------------------
if st.session_state.page == "SAP Payload Preview":
    from ordra.ui.sap_payload_view import pretty_json, summarize_issues

    st.subheader("SAP Payload Preview (ECC BAPI)")

    job = st.session_state.get("job_cache")
    if not job:
        st.info("Run a job from Inbox Triage to see the SAP payload preview here.")
    else:
        outputs = job.get("outputs") or {}
        extracted_bapi = outputs.get("extracted_bapi") or job.get("extracted_bapi") or {}
        validated = outputs.get("validated_order") or job.get("validated_order") or {}
        issues = (validated.get("issues") if isinstance(validated, dict) else []) or outputs.get("issues") or []

        st.markdown("#### 1) Extracted BAPI-aligned JSON")
        st.code(pretty_json(extracted_bapi), language="json")

        st.markdown("#### 2) Validation Summary")
        counts = summarize_issues(issues if isinstance(issues, list) else [])
        c1, c2, c3 = st.columns(3)
        c1.metric("BLOCK", counts["BLOCK"])
        c2.metric("WARN", counts["WARN"])
        c3.metric("INFO", counts["INFO"])

        if issues:
            st.write("Issues:")
            st.dataframe(issues)

        st.markdown("#### 3) Exact ECC Call Payload")
        bapi_call_payload = outputs.get("sap_bapi_payload") or job.get("sap_bapi_payload") or extracted_bapi
        st.code(pretty_json(bapi_call_payload), language="json")
        st.caption("This is the exact structure that would be sent to BAPI_SALESORDER_CREATEFROMDAT2.")

# ----------------------------
# CS Review
# ----------------------------
if st.session_state.page == "CS Review":
    st.subheader("CS Review — Open Tasks")

    # Auto-refresh open tasks when entering CS Review
    try:
        st.session_state.open_tasks = requests.get(f"{API_BASE}/review/open", timeout=30).json()["tasks"]
    except Exception:
        st.session_state.open_tasks = []

    tasks = st.session_state.get("open_tasks", [])
    current_job_id = st.session_state.get("job_id")

    default_task_id = None
    if current_job_id:
        for t in tasks:
            if t.get("job_id") == current_job_id and t.get("status") == "OPEN":
                default_task_id = t.get("task_id")
                break

    task_ids = [t["task_id"] for t in tasks]
    default_index = 0
    if default_task_id and default_task_id in task_ids:
        default_index = task_ids.index(default_task_id)

    if not tasks:
        st.info("No open tasks. Run a job that triggers CS_REVIEW to see tasks here.")
    else:
        selected = st.selectbox(
            "Select a task",
            options=task_ids,
            index=default_index,
        )
        task = next(t for t in tasks if t["task_id"] == selected)

        st.write("**Role:**", task.get("role"))
        st.write("**Status:**", task.get("status"))
        st.write("**Job ID:**", task.get("job_id"))

        payload = task.get("payload", {})
        st.markdown("### Validated Draft (from agent)")
        st.json(payload.get("validated", {}))
        st.markdown("### Decision Recommendation")
        st.json(payload.get("decision", {}))

        st.markdown("### Approve / Reject")

        colA, colB = st.columns([1, 2])

        with colA:
            reason = st.text_input("Reason (optional)", value="Reviewed and approved.")

        with colB:
            st.caption("Paste a JSON object. Example: {\"material_mappings\": {\"1\": \"0000098765\"}}")
            overrides_text = st.text_area(
                "Overrides JSON (optional)",
                value='{"material_mappings": {"1": "0000098765"}}',
                height=110
            )

        overrides_obj = {}
        overrides_error = None
        try:
            overrides_obj = json.loads(overrides_text.strip() or "{}")
            if not isinstance(overrides_obj, dict):
                overrides_error = "Overrides must be a JSON object (dictionary)."
        except JSONDecodeError as e:
            overrides_error = f"Invalid JSON: {e}"

        if overrides_error:
            st.error(overrides_error)

        c1, c2, c3, c4, c5 = st.columns([1, 1, 1, 1, 2])

        def _post_approval(status: str, reason_text: str, overrides: dict):
            resp = requests.post(
                f"{API_BASE}/review/{selected}/complete",
                json={"status": status, "reason": reason_text, "overrides": overrides},
                timeout=30
            )
            resp.raise_for_status()
            return resp.json()

        if c1.button("Approve", use_container_width=True, disabled=bool(overrides_error)):
            _post_approval("APPROVED", reason, overrides_obj)
            st.success("Approved. Overrides saved. Now re-run the job to AUTO_POST.")

        if c2.button("Approve + Go back to Inbox", use_container_width=True, disabled=bool(overrides_error)):
            _post_approval("APPROVED", reason, overrides_obj)
            st.success("Approved. Returning to Inbox…")
            st.session_state.page = "Inbox Triage"
            st.session_state.rerun_on_arrival = True
            st.rerun()

        if c3.button("Reject", use_container_width=True):
            _post_approval("REJECTED", "Rejected by CS", {})
            st.warning("Rejected. Job on hold.")

        if c4.button("Changes Requested", use_container_width=True):
            _post_approval("CHANGES_REQUESTED", "Need customer clarification", {})
            st.info("Changes requested. Job on hold.")

        if c5.button("Copy Template", use_container_width=True):
            st.code('{"material_mappings": {"1": "0000098765"}}', language="json")

# ----------------------------
# Decision Deck / Audit
# ----------------------------
if st.session_state.page == "Decision Deck":
    st.subheader("Decision Deck / Audit")
    job_id = st.text_input("Enter Job ID to fetch audit", value=st.session_state.get("job_id") or "")
    if st.button("Fetch Audit", use_container_width=True, disabled=not job_id):
        resp = requests.get(f"{API_BASE}/jobs/{job_id}/audit", timeout=30)
        if resp.status_code != 200:
            st.error("Audit not found.")
        else:
            audit = resp.json()
            st.success("Audit loaded.")
            st.json(audit)
