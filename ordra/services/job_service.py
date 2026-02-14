from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from ordra.db.sqlite import SQLiteDB


class JobService:
    def __init__(self, db: SQLiteDB) -> None:
        self.db = db

    # ---------- Jobs ----------
    def create_job(self, job_input: Dict[str, Any]) -> Dict[str, Any]:
        job_id = f"J-{uuid.uuid4().hex[:10]}"
        now = self.db.now()
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO jobs(job_id, status, created_at, updated_at, input_json, output_json)
                VALUES(?,?,?,?,?,NULL)
                """,
                (job_id, "CREATED", now, now, self.db.dumps(job_input)),
            )
        return {"job_id": job_id, "status": "CREATED"}

    def get_job(self, job_id: str) -> Dict[str, Any]:
        with self.db.connect() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            if not row:
                raise KeyError("Job not found")
            return {
                "job_id": row["job_id"],
                "status": row["status"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "input": self.db.loads(row["input_json"]),
                "outputs": self.db.loads(row["output_json"]) or {},
            }

    def update_job_status(self, job_id: str, status: str) -> None:
        now = self.db.now()
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE jobs SET status = ?, updated_at = ? WHERE job_id = ?",
                (status, now, job_id),
            )

    def save_job_outputs(self, job_id: str, outputs: Dict[str, Any], status: Optional[str] = None) -> None:
        now = self.db.now()
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE jobs SET output_json = ?, updated_at = ? WHERE job_id = ?",
                (self.db.dumps(outputs), now, job_id),
            )
            if status:
                conn.execute(
                    "UPDATE jobs SET status = ?, updated_at = ? WHERE job_id = ?",
                    (status, now, job_id),
                )

    # ---------- Audits ----------
    def save_audit(self, job_id: str, audit: Dict[str, Any]) -> None:
        now = self.db.now()
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO audits(job_id, created_at, audit_json)
                VALUES(?,?,?)
                ON CONFLICT(job_id) DO UPDATE SET
                    audit_json = excluded.audit_json,
                    created_at = excluded.created_at
                """,
                (job_id, now, self.db.dumps(audit)),
            )

    def get_audit(self, job_id: str) -> Dict[str, Any]:
        with self.db.connect() as conn:
            row = conn.execute("SELECT * FROM audits WHERE job_id = ?", (job_id,)).fetchone()
            if not row:
                raise KeyError("Audit not found")
            return self.db.loads(row["audit_json"])

    def get_latest_approved_overrides(self, job_id: str) -> Dict[str, Any]:
        """
        Returns the latest overrides dict from an APPROVED HITL task for this job.
        If none exist, returns {}.

        Expected shape (example):
          {
            "material_mappings": { "1": "0000098765", "2": "0000011223" },
            "ship_to_id": "200987"
          }
        """
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT decision_json
                FROM hitl_tasks
                WHERE job_id = ? AND status = 'APPROVED'
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (job_id,),
            ).fetchone()

            if not row or not row["decision_json"]:
                return {}

            decision = self.db.loads(row["decision_json"]) or {}
            overrides = decision.get("overrides") or {}
            if not isinstance(overrides, dict):
                return {}
            return overrides

    # ---------- HITL Tasks ----------
    def upsert_hitl_task(self, task: Dict[str, Any]) -> None:
        """
        Task shape expected:
          - task_id, job_id, status, role, created_at, payload, decision(optional)
        """
        now = self.db.now()
        decision = task.get("decision")
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO hitl_tasks(task_id, job_id, status, role, created_at, updated_at, payload_json, decision_json)
                VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(task_id) DO UPDATE SET
                    status = excluded.status,
                    role = excluded.role,
                    updated_at = excluded.updated_at,
                    payload_json = excluded.payload_json,
                    decision_json = excluded.decision_json
                """,
                (
                    task["task_id"],
                    task["job_id"],
                    task["status"],
                    task.get("role") or "CS",
                    task.get("created_at") or now,
                    now,
                    self.db.dumps(task.get("payload") or {}),
                    self.db.dumps(decision) if decision is not None else None,
                ),
            )

    def list_open_hitl_tasks(self) -> List[Dict[str, Any]]:
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM hitl_tasks WHERE status = 'OPEN' ORDER BY created_at DESC"
            ).fetchall()
            out: List[Dict[str, Any]] = []
            for r in rows:
                out.append(
                    {
                        "task_id": r["task_id"],
                        "job_id": r["job_id"],
                        "status": r["status"],
                        "role": r["role"],
                        "created_at": r["created_at"],
                        "updated_at": r["updated_at"],
                        "payload": self.db.loads(r["payload_json"]),
                        "decision": self.db.loads(r["decision_json"]),
                    }
                )
            return out

    def get_hitl_task(self, task_id: str) -> Dict[str, Any]:
        with self.db.connect() as conn:
            r = conn.execute("SELECT * FROM hitl_tasks WHERE task_id = ?", (task_id,)).fetchone()
            if not r:
                raise KeyError("Task not found")
            return {
                "task_id": r["task_id"],
                "job_id": r["job_id"],
                "status": r["status"],
                "role": r["role"],
                "created_at": r["created_at"],
                "updated_at": r["updated_at"],
                "payload": self.db.loads(r["payload_json"]),
                "decision": self.db.loads(r["decision_json"]),
            }

    def complete_hitl_task(self, task_id: str, status: str, decision: Dict[str, Any]) -> Dict[str, Any]:
        now = self.db.now()
        with self.db.connect() as conn:
            row = conn.execute("SELECT job_id, role, payload_json FROM hitl_tasks WHERE task_id = ?", (task_id,)).fetchone()
            if not row:
                raise KeyError("Task not found")

            conn.execute(
                """
                UPDATE hitl_tasks
                SET status = ?, updated_at = ?, decision_json = ?
                WHERE task_id = ?
                """,
                (status, now, self.db.dumps(decision), task_id),
            )
        return self.get_hitl_task(task_id)
