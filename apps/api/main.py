from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from ordra.db.sqlite import SQLiteDB
from ordra.services.job_service import JobService
from ordra.runtime.run_job import run_job

app = FastAPI(title="ORDRA-AI API", version="0.2.0")

DB_PATH = "ordra.db"
db = SQLiteDB(DB_PATH)
db.init()
jobs = JobService(db)


class IntakeRequest(BaseModel):
    email_subject: Optional[str] = None
    email_from: Optional[str] = None
    email_body: Optional[str] = ""
    pdf_text: Optional[str] = None
    ocr_text: Optional[str] = None
    excel_tables: Optional[str] = None
    force_credit_block: Optional[bool] = False
    force_atp_short: Optional[bool] = False
    mailbox_query: Optional[Dict[str, Any]] = None


class RunResponse(BaseModel):
    job_id: str
    status: str
    decision: Optional[Dict[str, Any]] = None
    hitl_task_id: Optional[str] = None
    sap_order_number: Optional[str] = None


class ReviewCompleteRequest(BaseModel):
    status: str = Field(..., pattern="^(APPROVED|REJECTED|CHANGES_REQUESTED)$")
    reason: Optional[str] = None
    overrides: Dict[str, Any] = Field(default_factory=dict)


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.post("/intake")
def intake(req: IntakeRequest) -> Dict[str, Any]:
    job_input = req.model_dump()
    created = jobs.create_job(job_input)
    return created


@app.get("/jobs/{job_id}")
def get_job(job_id: str) -> Dict[str, Any]:
    try:
        return jobs.get_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Job not found")


@app.get("/jobs/{job_id}/audit")
def get_audit(job_id: str) -> Dict[str, Any]:
    try:
        return jobs.get_audit(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Audit not found")


@app.post("/jobs/{job_id}/run", response_model=RunResponse)
def run(job_id: str) -> RunResponse:
    try:
        job = jobs.get_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Job not found")

    job_input = job["input"] or {}
    job_input["job_id"] = job_id

    overrides = jobs.get_latest_approved_overrides(job_id)
    job_input["human_overrides"] = overrides

    try:
        out_ctx = run_job(job_input)
    except Exception as e:
        jobs.update_job_status(job_id, "FAILED")
        raise HTTPException(status_code=500, detail=f"Run failed: {e}")

    decision = out_ctx.get("decision")
    sap_res = out_ctx.get("sap_order_result") or {}
    sap_order_number = sap_res.get("sap_order_number")

    outputs = {
        "extracted_order": out_ctx.get("extracted_order"),
        "validated_order": out_ctx.get("validated_order"),
        "decision": decision,
        "hitl_task_id": out_ctx.get("hitl_task_id"),
        "sap_order_number": sap_order_number,
        "decision_deck": out_ctx.get("decision_deck"),
        "_dag_exec": out_ctx.get("_dag_exec"),
        "extracted_bapi": out_ctx.get("extracted_bapi"),
        "sap_bapi_payload": out_ctx.get("sap_bapi_payload"),
    }

    audit = out_ctx.get("audit_record") or {"job_id": job_id, "created_at": datetime.utcnow().isoformat()}
    jobs.save_audit(job_id, audit)

    runtime = out_ctx.get("_runtime") or {}
    hitl_tasks = runtime.get("hitl_tasks") or {}
    for _, t in hitl_tasks.items():
        t["job_id"] = job_id
        jobs.upsert_hitl_task(t)

    if sap_order_number:
        jobs.save_job_outputs(job_id, outputs, status="COMPLETED")
        return RunResponse(job_id=job_id, status="COMPLETED", decision=decision, sap_order_number=sap_order_number)

    if out_ctx.get("hitl_task_id"):
        jobs.save_job_outputs(job_id, outputs, status="NEEDS_REVIEW")
        return RunResponse(job_id=job_id, status="NEEDS_REVIEW", decision=decision, hitl_task_id=out_ctx.get("hitl_task_id"))

    jobs.save_job_outputs(job_id, outputs, status="ON_HOLD")
    return RunResponse(job_id=job_id, status="ON_HOLD", decision=decision)


@app.get("/review/open")
def list_open_tasks() -> Dict[str, Any]:
    return {"tasks": jobs.list_open_hitl_tasks()}


@app.post("/review/{task_id}/complete")
def complete_review(task_id: str, req: ReviewCompleteRequest) -> Dict[str, Any]:
    try:
        task = jobs.get_hitl_task(task_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Task not found")

    decision = {"reason": req.reason, "overrides": req.overrides}
    updated = jobs.complete_hitl_task(task_id, req.status, decision)

    job_id = updated["job_id"]
    if req.status == "APPROVED":
        jobs.update_job_status(job_id, "APPROVED_BY_HUMAN")
    elif req.status == "REJECTED":
        jobs.update_job_status(job_id, "REJECTED_BY_HUMAN")
    else:
        jobs.update_job_status(job_id, "CHANGES_REQUESTED")

    return updated
