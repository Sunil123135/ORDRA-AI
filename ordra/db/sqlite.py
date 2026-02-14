from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Generator, Optional


def _utc_iso() -> str:
    return datetime.utcnow().isoformat()


class SQLiteDB:
    """
    Minimal SQLite wrapper.
    - Uses WAL for better concurrency
    - Stores JSON as TEXT
    """

    def __init__(self, db_path: str = "ordra.db") -> None:
        self.db_path = db_path

    @contextmanager
    def connect(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init(self) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    input_json TEXT NOT NULL,
                    output_json TEXT
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS audits (
                    job_id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    audit_json TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id)
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS hitl_tasks (
                    task_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    role TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    decision_json TEXT,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id)
                );
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_hitl_job ON hitl_tasks(job_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_hitl_status ON hitl_tasks(status);")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS episodes (
                    episode_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    customer_key TEXT NOT NULL,
                    layout_hash TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    skeleton_json TEXT NOT NULL
                );
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_episodes_customer ON episodes(customer_key);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_episodes_layout ON episodes(layout_hash);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_episodes_created ON episodes(created_at);")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS revenue_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    customer_key TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    tier TEXT NOT NULL,
                    price REAL NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_revenue_job ON revenue_events(job_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_revenue_customer ON revenue_events(customer_key);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_revenue_created ON revenue_events(created_at);")

    @staticmethod
    def dumps(obj: Any) -> str:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def loads(s: Optional[str]) -> Any:
        if s is None:
            return None
        return json.loads(s)

    def now(self) -> str:
        return _utc_iso()
