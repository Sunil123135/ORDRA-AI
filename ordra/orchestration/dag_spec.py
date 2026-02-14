from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import networkx as nx
import yaml


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 1
    backoff_seconds: Optional[List[int]] = None

    def normalized(self) -> "RetryPolicy":
        return RetryPolicy(
            max_attempts=self.max_attempts,
            backoff_seconds=self.backoff_seconds or [1],
        )


@dataclass(frozen=True)
class NodeSpec:
    id: str
    type: str  # "tool" | "agent"
    handler: str
    deps: List[str]
    when: Optional[str]
    retry: RetryPolicy
    timeout_seconds: int
    outputs: List[str]


def _load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _parse_retry(node: Dict[str, Any], defaults: Dict[str, Any]) -> RetryPolicy:
    node_retry = node.get("retry") or defaults.get("retry") or {}
    max_attempts = int(node_retry.get("max_attempts", 1))
    backoff_seconds = node_retry.get("backoff_seconds", [1])
    if isinstance(backoff_seconds, int):
        backoff_seconds = [backoff_seconds]
    backoff_seconds = [int(x) for x in backoff_seconds]
    return RetryPolicy(max_attempts=max_attempts, backoff_seconds=backoff_seconds).normalized()


def _parse_timeout(node: Dict[str, Any], defaults: Dict[str, Any]) -> int:
    return int(node.get("timeout_seconds", defaults.get("timeout_seconds", 30)))


def load_dag_spec(path: str) -> Dict[str, Any]:
    """
    Loads the raw DAG spec dictionary from YAML.
    """
    spec = _load_yaml(path)
    if "nodes" not in spec or not isinstance(spec["nodes"], list):
        raise ValueError("DAG spec invalid: nodes[] missing")
    return spec


def build_graph_from_yaml(path: str) -> nx.DiGraph:
    """
    Build a NetworkX DiGraph from YAML spec.

    Each node in graph has metadata:
      - type, handler, when, retry, timeout_seconds, outputs
    Edges are created from deps -> node_id
    """
    spec = load_dag_spec(path)
    defaults = spec.get("defaults", {})
    nodes_raw: List[Dict[str, Any]] = spec["nodes"]

    g = nx.DiGraph()
    seen_ids: set = set()

    for n in nodes_raw:
        node_id = n.get("id")
        if not node_id or not isinstance(node_id, str):
            raise ValueError("Node missing string id")
        if node_id in seen_ids:
            raise ValueError(f"Duplicate node id: {node_id}")
        seen_ids.add(node_id)

        node_type = n.get("type", "tool")
        handler = n.get("handler")
        if not handler:
            raise ValueError(f"Node {node_id} missing handler")

        deps = n.get("deps", [])
        if deps is None:
            deps = []
        if not isinstance(deps, list):
            raise ValueError(f"Node {node_id} deps must be a list")

        when = n.get("when")
        outputs = n.get("outputs", [])
        if outputs is None:
            outputs = []
        if not isinstance(outputs, list):
            raise ValueError(f"Node {node_id} outputs must be a list")

        retry = _parse_retry(n, defaults)
        timeout_seconds = _parse_timeout(n, defaults)

        node_spec = NodeSpec(
            id=node_id,
            type=node_type,
            handler=str(handler),
            deps=[str(d) for d in deps],
            when=str(when) if when is not None else None,
            retry=retry,
            timeout_seconds=timeout_seconds,
            outputs=[str(o) for o in outputs],
        )

        g.add_node(node_id, spec=node_spec)

    for node_id in list(g.nodes):
        node_spec: NodeSpec = g.nodes[node_id]["spec"]
        for dep in node_spec.deps:
            if dep not in g.nodes:
                raise ValueError(f"Node {node_id} depends on missing node: {dep}")
            g.add_edge(dep, node_id)

    if not nx.is_directed_acyclic_graph(g):
        cycle = nx.find_cycle(g, orientation="original")
        raise ValueError(f"DAG has a cycle: {cycle}")

    return g
