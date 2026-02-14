from __future__ import annotations

import copy
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set

import networkx as nx
from concurrent.futures import ThreadPoolExecutor, as_completed

from ordra.orchestration.dag_spec import NodeSpec, RetryPolicy


class NodeFailure(Exception):
    pass


class TransientError(Exception):
    """Raise for retry-eligible transient failures (timeouts, network, 5xx)."""
    pass


def _deterministic_merge(base: Dict[str, Any], updates: Dict[str, Any], node_id: str) -> Dict[str, Any]:
    merged = base
    for k, v in updates.items():
        if k not in merged:
            merged[k] = v
            continue
        if isinstance(merged[k], dict) and isinstance(v, dict):
            inner = dict(merged[k])
            for ik, iv in v.items():
                if ik not in inner:
                    inner[ik] = iv
                else:
                    inner[f"{node_id}.{ik}"] = iv
            merged[k] = inner
            continue
        merged[f"{node_id}.{k}"] = v
    return merged


def _parse_bool(s: str) -> Optional[bool]:
    sl = s.strip().lower()
    if sl in ("true", "1", "yes", "y"):
        return True
    if sl in ("false", "0", "no", "n"):
        return False
    return None


def _get_ctx_path(ctx: Dict[str, Any], path: str) -> Any:
    cur: Any = ctx
    for seg in path.split("."):
        if isinstance(cur, dict) and seg in cur:
            cur = cur[seg]
        else:
            return None
    return cur


def _eval_clause(clause: str, ctx: Dict[str, Any]) -> bool:
    clause = clause.strip()
    if " in " in clause:
        left, right = [x.strip() for x in clause.split(" in ", 1)]
        left_val = _get_ctx_path(ctx, left)
        if right.startswith("[") and right.endswith("]"):
            raw = right[1:-1].strip()
            items = []
            if raw:
                for tok in raw.split(","):
                    t = tok.strip().strip("'").strip('"')
                    items.append(t)
            return str(left_val) in items
        return False
    if "==" in clause:
        left, right = [x.strip() for x in clause.split("==", 1)]
        left_val = _get_ctx_path(ctx, left)
        b = _parse_bool(right)
        if b is not None:
            return bool(left_val) is b
        if (right.startswith("'") and right.endswith("'")) or (right.startswith('"') and right.endswith('"')):
            lit = right[1:-1]
            return str(left_val) == lit
        return str(left_val) == right
    val = _get_ctx_path(ctx, clause)
    return bool(val)


def _eval_when_expr(expr: str, ctx: Dict[str, Any]) -> bool:
    if not expr or not isinstance(expr, str):
        return True
    or_parts = [p.strip() for p in expr.split(" or ")]
    or_results = []
    for part in or_parts:
        and_parts = [p.strip() for p in part.split(" and ")]
        and_results = []
        for clause in and_parts:
            and_results.append(_eval_clause(clause, ctx))
        or_results.append(all(and_results))
    return any(or_results)


@dataclass
class NodeRunResult:
    node_id: str
    ok: bool
    updates: Dict[str, Any]
    error: Optional[str] = None
    attempts: int = 0
    duration_s: float = 0.0


class ParallelDAGExecutor:
    def __init__(
        self,
        handlers: Dict[str, Callable[[Dict[str, Any], NodeSpec], Dict[str, Any]]],
        *,
        max_workers: int = 6,
        enable_wave_parallelism: bool = True,
    ):
        self.handlers = handlers
        self.max_workers = max_workers
        self.enable_wave_parallelism = enable_wave_parallelism

    def run(self, graph: nx.DiGraph, ctx: Dict[str, Any]) -> Dict[str, Any]:
        if not nx.is_directed_acyclic_graph(graph):
            raise ValueError("Graph must be a DAG")

        shared_ctx = ctx
        pending: Set[str] = set(graph.nodes)
        completed: Set[str] = set()
        failed: Set[str] = set()
        results: Dict[str, NodeRunResult] = {}
        deps_map: Dict[str, Set[str]] = {n: set(graph.predecessors(n)) for n in graph.nodes}

        while pending:
            ready = [n for n in pending if deps_map[n].issubset(completed)]
            if not ready:
                stuck = sorted(list(pending))
                raise NodeFailure(f"DAG stuck; pending nodes cannot run: {stuck}")

            if self.enable_wave_parallelism and len(ready) > 1:
                wave_outcomes = self._run_wave_parallel(graph, shared_ctx, ready)
            else:
                wave_outcomes = [self._run_single(graph, shared_ctx, n) for n in ready]

            for outcome in sorted(wave_outcomes, key=lambda r: r.node_id):
                results[outcome.node_id] = outcome
                pending.remove(outcome.node_id)
                if outcome.ok:
                    completed.add(outcome.node_id)
                    shared_ctx = _deterministic_merge(shared_ctx, outcome.updates, outcome.node_id)
                else:
                    failed.add(outcome.node_id)
                    raise NodeFailure(
                        f"Node {outcome.node_id} failed after {outcome.attempts} attempts: {outcome.error}"
                    )

        shared_ctx["_dag_exec"] = {
            "completed": sorted(list(completed)),
            "failed": sorted(list(failed)),
            "node_results": {k: results[k].__dict__ for k in sorted(results.keys())},
        }
        return shared_ctx

    def _run_wave_parallel(self, graph: nx.DiGraph, shared_ctx: Dict[str, Any], ready: List[str]) -> List[NodeRunResult]:
        wave_ctx = copy.deepcopy(shared_ctx)
        outcomes: List[NodeRunResult] = []
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(ready))) as pool:
            future_map = {
                pool.submit(self._run_single_with_ctx, graph, wave_ctx, node_id): node_id for node_id in ready
            }
            for fut in as_completed(future_map):
                outcomes.append(fut.result())
        return outcomes

    def _run_single_with_ctx(self, graph: nx.DiGraph, ctx_snapshot: Dict[str, Any], node_id: str) -> NodeRunResult:
        return self._run_single(graph, ctx_snapshot, node_id)

    def _run_single(self, graph: nx.DiGraph, ctx_for_node: Dict[str, Any], node_id: str) -> NodeRunResult:
        node_data = graph.nodes[node_id]
        spec: NodeSpec = node_data["spec"]

        if spec.when and not _eval_when_expr(spec.when, ctx_for_node):
            return NodeRunResult(node_id=node_id, ok=True, updates={}, attempts=0, duration_s=0.0)

        handler = self.handlers.get(spec.handler)
        if not handler:
            raise ValueError(f"No handler registered for {spec.handler}")

        retry: RetryPolicy = spec.retry
        max_attempts = retry.max_attempts
        backoffs = retry.backoff_seconds or [1]

        t0 = time.time()
        last_err: Optional[str] = None
        for attempt in range(1, max_attempts + 1):
            try:
                updates = handler(ctx_for_node, spec) or {}
                dt = time.time() - t0
                return NodeRunResult(
                    node_id=node_id, ok=True, updates=updates, attempts=attempt, duration_s=dt
                )
            except TransientError as te:
                last_err = str(te)
                if attempt == max_attempts:
                    dt = time.time() - t0
                    return NodeRunResult(
                        node_id=node_id, ok=False, updates={}, error=last_err, attempts=attempt, duration_s=dt
                    )
                time.sleep(backoffs[min(attempt - 1, len(backoffs) - 1)])
            except Exception as e:
                dt = time.time() - t0
                return NodeRunResult(
                    node_id=node_id, ok=False, updates={}, error=str(e), attempts=attempt, duration_s=dt
                )
        dt = time.time() - t0
        return NodeRunResult(node_id=node_id, ok=False, updates={}, error=last_err or "unknown", attempts=max_attempts, duration_s=dt)
