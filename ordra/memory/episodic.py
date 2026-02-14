from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from ordra.db.sqlite import SQLiteDB


def _utc_iso() -> str:
    return datetime.utcnow().isoformat()


def _stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _safe_trunc(s: str, n: int = 500) -> str:
    s = s or ""
    return s[:n]


@dataclass
class EpisodeSkeleton:
    episode_id: str
    job_id: str
    customer_key: str
    layout_hash: str
    outcome: str
    created_at: str
    skeleton: Dict[str, Any]


class MemorySkeletonizer:
    """
    ORDRA-specific skeletonization:
      - Strip heavy payloads
      - Keep logic and corrections (HITL overrides) and key tool decisions
    """

    @staticmethod
    def compute_layout_hash(ctx: Dict[str, Any]) -> str:
        """
        Layout signature should be stable and light.
        Uses hints + first chunk fragments.
        """
        customer = (ctx.get("intake_context") or {}).get("customer_candidate") or "UNKNOWN"
        layout_hints = ctx.get("layout_hints") or ""
        pdf_head = _safe_trunc("\n".join(ctx.get("pdf_text_chunks") or []), 600)
        ocr_head = _safe_trunc("\n".join(ctx.get("ocr_text_chunks") or []), 600)

        signature = f"CUST={customer}\nHINTS={layout_hints}\nPDF={pdf_head}\nOCR={ocr_head}"
        return _stable_hash(signature)

    @staticmethod
    def skeletonize(ctx: Dict[str, Any]) -> Dict[str, Any]:
        """
        Produces a 'recipe' of the run:
          - decisions & why
          - validation issues
          - HITL overrides (approved)
          - which routes/tools were taken
        """
        job_id = ctx.get("job_id") or "UNKNOWN"
        intake = ctx.get("intake_context") or {}
        customer_key = intake.get("customer_candidate") or "UNKNOWN"

        decision = ctx.get("decision") or {}
        validated = ctx.get("validated_order") or {}
        issues = validated.get("issues") or []

        # capture overrides used in the run (if any)
        human_overrides = ctx.get("human_overrides") or {}

        # important runtime traces
        dag_exec = ctx.get("_dag_exec") or {}

        skeleton = {
            "job_id": job_id,
            "customer_key": customer_key,
            "layout_hash": MemorySkeletonizer.compute_layout_hash(ctx),
            "outcome": decision.get("action") or "UNKNOWN",
            "safe_to_post": bool(decision.get("safe_to_post")),
            "decision_reasons": decision.get("reasons") or [],
            "issues": issues,
            "human_overrides_used": human_overrides,
            "tool_path": {
                "used_pdf_text": bool(ctx.get("pdf_text_chunks")),
                "used_ocr": bool(ctx.get("ocr_text_chunks")),
                "used_excel": bool(ctx.get("excel_tables")),
            },
            "models": {
                # fill if you track model IDs in ctx["_runtime"]
                "extractor": (ctx.get("_runtime") or {}).get("model_extractor"),
                "verifier": (ctx.get("_runtime") or {}).get("model_verifier"),
            },
            "dag_exec": {
                "completed": dag_exec.get("completed"),
                "failed": dag_exec.get("failed"),
                "node_results": dag_exec.get("node_results"),
            },
            "timestamp": _utc_iso(),
        }
        return skeleton


class EpisodicMemoryStore:
    """
    SQLite-backed episodic memory store. Stores skeleton JSON per job/customer/layout.
    """

    def __init__(self, db: SQLiteDB) -> None:
        self.db = db

    def save_episode(self, ctx: Dict[str, Any]) -> EpisodeSkeleton:
        job_id = ctx.get("job_id") or "UNKNOWN"
        intake = ctx.get("intake_context") or {}
        customer_key = intake.get("customer_candidate") or "UNKNOWN"
        layout_hash = MemorySkeletonizer.compute_layout_hash(ctx)

        skeleton = MemorySkeletonizer.skeletonize(ctx)
        outcome = skeleton.get("outcome") or "UNKNOWN"

        episode_id = f"E-{job_id}-{layout_hash}"
        created_at = self.db.now()

        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO episodes(episode_id, job_id, customer_key, layout_hash, outcome, created_at, skeleton_json)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(episode_id) DO UPDATE SET
                    outcome = excluded.outcome,
                    created_at = excluded.created_at,
                    skeleton_json = excluded.skeleton_json
                """,
                (
                    episode_id,
                    job_id,
                    customer_key,
                    layout_hash,
                    outcome,
                    created_at,
                    self.db.dumps(skeleton),
                ),
            )

        return EpisodeSkeleton(
            episode_id=episode_id,
            job_id=job_id,
            customer_key=customer_key,
            layout_hash=layout_hash,
            outcome=outcome,
            created_at=created_at,
            skeleton=skeleton,
        )

    def retrieve_recipes(self, customer_key: str, layout_hash: Optional[str] = None, limit: int = 5) -> Dict[str, Any]:
        """
        Returns lightweight 'recipe hints' to inject into prompts.
        """
        q = """
            SELECT skeleton_json
            FROM episodes
            WHERE customer_key = ?
        """
        args: Tuple[Any, ...] = (customer_key,)

        if layout_hash:
            q += " AND layout_hash = ?"
            args = (customer_key, layout_hash)

        q += " ORDER BY created_at DESC LIMIT ?"
        args = (*args, int(limit))

        with self.db.connect() as conn:
            rows = conn.execute(q, args).fetchall()

        recipes = []
        for r in rows:
            sk = self.db.loads(r["skeleton_json"]) or {}
            recipes.append(
                {
                    "outcome": sk.get("outcome"),
                    "decision_reasons": sk.get("decision_reasons"),
                    "issues": sk.get("issues"),
                    "human_overrides_used": sk.get("human_overrides_used"),
                    "tool_path": sk.get("tool_path"),
                    "timestamp": sk.get("timestamp"),
                }
            )

        # Return as a single text blob too (easy injection)
        text = json.dumps(recipes, ensure_ascii=False, indent=2)
        return {"recipes": recipes, "recipes_text": text}
