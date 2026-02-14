from pathlib import Path

import networkx as nx

from ordra.orchestration.dag_spec import build_graph_from_yaml, load_dag_spec


def test_load_dag_spec():
    path = Path(__file__).resolve().parent.parent / "ordra" / "orchestration" / "dag_spec.yaml"
    spec = load_dag_spec(str(path))
    assert "nodes" in spec
    assert "dag_name" in spec
    assert len(spec["nodes"]) > 5


def test_build_graph():
    path = Path(__file__).resolve().parent.parent / "ordra" / "orchestration" / "dag_spec.yaml"
    g = build_graph_from_yaml(str(path))
    assert isinstance(g, nx.DiGraph)
    assert "ingest_email" in g.nodes
    assert ("ingest_email", "detect_customer") in g.edges
    assert nx.is_directed_acyclic_graph(g)
