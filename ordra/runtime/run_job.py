from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from ordra.agents.verifier_agent import GeminiVerifier
from ordra.connectors.o365_client import O365Client, O365Error
from ordra.db.sqlite import SQLiteDB
from ordra.llm.openai_client import OpenAIClient, LLMConfig
from ordra.memory.episodic import EpisodicMemoryStore
from ordra.orchestration.dag_spec import build_graph_from_yaml
from ordra.sap.sap_client import SapClient
from ordra.sap.validation import SapValidator
from ordra.orchestration.executor import ParallelDAGExecutor
from ordra.runtime.handlers import build_handlers


def _default_dag_path() -> str:
    p = Path(__file__).resolve().parent.parent / "orchestration" / "dag_spec.yaml"
    return str(p)


def run_job(
    job_input: Dict[str, Any],
    *,
    dag_yaml_path: Optional[str] = None,
    prompts_dir: Optional[str] = None,
    schemas_dir: Optional[str] = None,
    max_workers: int = 6,
    runtime_overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    base = Path(__file__).resolve().parent.parent
    dag_yaml_path = dag_yaml_path or _default_dag_path()
    if not Path(dag_yaml_path).is_file():
        dag_yaml_path = str(base / "orchestration" / "dag_spec.yaml")
    prompts_dir = prompts_dir or str(base / "llm" / "prompts")
    schemas_dir = schemas_dir or str(base / "llm" / "schemas")

    openai_client = OpenAIClient(
        prompts_dir=prompts_dir,
        schemas_dir=schemas_dir,
        config=LLMConfig(
            model_chat="gpt-4o-mini",
            temperature=0.0,
            max_output_tokens=2000,
            retries=2,
            backoff_seconds=(2, 5),
        ),
    )

    db = SQLiteDB("ordra.db")
    db.init()
    episode_store = EpisodicMemoryStore(db)
    verifier = GeminiVerifier()

    ctx: Dict[str, Any] = dict(job_input)
    ctx["_runtime"] = {
        "openai_client": openai_client,
        "verifier": verifier,
        "episode_store": episode_store,
        "db": db,
        "hitl_tasks": {},
        "model_extractor": openai_client.config.model_chat,
        "model_verifier": verifier.config.model,
    }
    try:
        ctx["_runtime"]["o365_client"] = O365Client()
    except O365Error:
        ctx["_runtime"]["o365_client"] = None
    ctx["_runtime"]["sap_client"] = SapClient()
    ctx["_runtime"]["sap_validator"] = SapValidator()
    if runtime_overrides:
        ctx["_runtime"].update(runtime_overrides)

    graph = build_graph_from_yaml(dag_yaml_path)
    handlers = build_handlers()
    executor = ParallelDAGExecutor(handlers=handlers, max_workers=max_workers, enable_wave_parallelism=True)
    out_ctx = executor.run(graph, ctx)
    return out_ctx


def get_open_hitl_tasks(ctx: Dict[str, Any]) -> Dict[str, Any]:
    rt = ctx.get("_runtime") or {}
    return rt.get("hitl_tasks") or {}
