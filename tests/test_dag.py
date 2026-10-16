"""Tests for the DAG compiler (stralis.dag)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from stralis.dag import CompiledDAG, compile_pipeline
from stralis.errors import PipelineValidationError
from stralis.models import NodeSpec, PipelineSpec


def node(nid: str, deps: list[str] | None = None) -> NodeSpec:
    return NodeSpec(id=nid, handler="h", depends_on=list(deps or []))


def spec(*nodes: NodeSpec, name: str = "p") -> PipelineSpec:
    return PipelineSpec(name=name, nodes=list(nodes))


# ---------------------------------------------------------------------------
# Structure / topological order
# ---------------------------------------------------------------------------


def test_diamond_graph():
    dag = compile_pipeline(
        spec(
            node("a"),
            node("b", ["a"]),
            node("c", ["a"]),
            node("d", ["b", "c"]),
        )
    )
    assert isinstance(dag, CompiledDAG)
    assert dag.order == ("a", "b", "c", "d")
    assert sorted(dag.order) == ["a", "b", "c", "d"]  # every node exactly once
    assert set(dag.nodes) == {"a", "b", "c", "d"}

    assert dag.roots() == ["a"]
    assert dag.children_of("a") == ("b", "c")
    assert dag.children_of("b") == ("d",)
    assert dag.children_of("c") == ("d",)
    assert dag.children_of("d") == ()
    assert dag.parents_of("d") == ("b", "c")
    assert dag.parents_of("b") == ("a",)
    assert dag.parents_of("a") == ()


def test_diamond_is_ready_semantics():
    dag = compile_pipeline(
        spec(
            node("a"),
            node("b", ["a"]),
            node("c", ["a"]),
            node("d", ["b", "c"]),
        )
    )
    # roots are always ready
    assert dag.is_ready("a", set()) is True
    assert dag.is_ready("a", {"x"}) is True
    # b needs only a
    assert dag.is_ready("b", set()) is False
    assert dag.is_ready("b", {"a"}) is True
    # c needs only a (b being done is irrelevant)
    assert dag.is_ready("c", {"b"}) is False
    assert dag.is_ready("c", {"a"}) is True
    # d needs both b and c
    assert dag.is_ready("d", set()) is False
    assert dag.is_ready("d", {"a", "b"}) is False
    assert dag.is_ready("d", {"b", "c"}) is True
    assert dag.is_ready("d", {"a", "b", "c"}) is True


def test_declaration_order_tie_breaking():
    # z and a are independent; z is declared first so it wins the tie.
    dag = compile_pipeline(spec(node("z"), node("a"), node("b", ["a"])))
    assert dag.order == ("z", "a", "b")
    assert dag.roots() == ["z", "a"]


def test_single_node_pipeline():
    dag = compile_pipeline(spec(node("solo")))
    assert dag.order == ("solo",)
    assert dag.roots() == ["solo"]
    assert dag.parents_of("solo") == ()
    assert dag.children_of("solo") == ()
    assert dag.is_ready("solo", set()) is True


def test_spec_is_preserved():
    s = spec(node("a"))
    dag = compile_pipeline(s)
    assert dag.spec is s
    assert dag.nodes["a"] is s.nodes[0]


# ---------------------------------------------------------------------------
# Accessors raise KeyError on unknown ids
# ---------------------------------------------------------------------------


def test_unknown_accessor_raises_keyerror():
    dag = compile_pipeline(spec(node("a")))
    with pytest.raises(KeyError):
        dag.parents_of("nope")
    with pytest.raises(KeyError):
        dag.children_of("nope")
    with pytest.raises(KeyError):
        dag.is_ready("nope", set())


# ---------------------------------------------------------------------------
# Validation errors
# ---------------------------------------------------------------------------


def test_unknown_depends_on():
    dag_spec = spec(node("a"), node("b", ["a", "ghost"]))
    with pytest.raises(PipelineValidationError) as exc:
        compile_pipeline(dag_spec)
    errs = exc.value.errors
    assert len(errs) == 1
    # must name both the node and the missing dep
    assert "b" in errs[0] and "ghost" in errs[0]


def test_self_dependency():
    dag_spec = spec(node("a", ["a"]))
    with pytest.raises(PipelineValidationError) as exc:
        compile_pipeline(dag_spec)
    assert any("a" in e for e in exc.value.errors)


def test_three_node_cycle():
    dag_spec = spec(
        node("a", ["c"]),
        node("b", ["a"]),
        node("c", ["b"]),
    )
    with pytest.raises(PipelineValidationError) as exc:
        compile_pipeline(dag_spec)
    text = " ".join(exc.value.errors)
    assert "cycle detected" in text
    assert "->" in text
    assert "a -> b -> c -> a" in text


def test_multiple_independent_errors_all_collected():
    dag_spec = spec(
        node("a"),
        node("b", ["a", "ghost"]),  # unknown dep
        node("c", ["c"]),           # self dep
    )
    with pytest.raises(PipelineValidationError) as exc:
        compile_pipeline(dag_spec)
    assert len(exc.value.errors) >= 2


# ---------------------------------------------------------------------------
# Duplicate ids are rejected by PipelineSpec's pydantic validator
# ---------------------------------------------------------------------------


def test_duplicate_node_ids_rejected_by_pydantic():
    with pytest.raises(ValidationError):
        PipelineSpec(name="p", nodes=[node("a"), node("a")])
