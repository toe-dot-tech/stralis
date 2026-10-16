"""Compile a :class:`PipelineSpec` into an immutable, executable DAG.

The compiler is pure (stdlib + pydantic models only): it validates the whole
spec up front, collecting *every* problem before raising, then produces a
deterministic topological order that respects the user's declaration intent.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass

from stralis.errors import PipelineValidationError
from stralis.models import NodeSpec, PipelineSpec


@dataclass(frozen=True)
class CompiledDAG:
    spec: PipelineSpec
    nodes: dict[str, NodeSpec]
    parents: dict[str, tuple[str, ...]]
    children: dict[str, tuple[str, ...]]
    order: tuple[str, ...]

    def roots(self) -> list[str]:
        """Nodes with no parents, in topological order."""
        return [nid for nid in self.order if not self.parents[nid]]

    def parents_of(self, node_id: str) -> tuple[str, ...]:
        return self.parents[node_id]

    def children_of(self, node_id: str) -> tuple[str, ...]:
        return self.children[node_id]

    def is_ready(self, node_id: str, succeeded: set[str]) -> bool:
        """True iff every parent of ``node_id`` is in ``succeeded``.

        Roots (no parents) are always ready. Unknown ``node_id`` raises KeyError.
        """
        return all(p in succeeded for p in self.parents[node_id])


def compile_pipeline(spec: PipelineSpec) -> CompiledDAG:
    """Validate ``spec`` and return its compiled DAG.

    Raises :class:`PipelineValidationError` with *all* detected problems:
    unknown ``depends_on`` references, self-dependencies, and cycles.
    """
    nodes: dict[str, NodeSpec] = {n.id: n for n in spec.nodes}
    parents: dict[str, list[str]] = {nid: [] for nid in nodes}
    children: dict[str, list[str]] = {nid: [] for nid in nodes}
    errors: list[str] = []

    # Pass 1: resolve edges, collecting reference errors.
    for n in spec.nodes:
        for dep in n.depends_on:
            if dep == n.id:
                errors.append(f"node '{n.id}' depends on itself")
            elif dep not in nodes:
                errors.append(f"node '{n.id}' depends on unknown node '{dep}'")
            else:
                parents[n.id].append(dep)
                children[dep].append(n.id)

    # Pass 2: cycle detection over the resolved graph.
    errors.extend(_find_cycles(list(nodes), children))

    if errors:
        raise PipelineValidationError(errors)

    frozen_parents = {nid: tuple(p) for nid, p in parents.items()}
    frozen_children = {nid: tuple(c) for nid, c in children.items()}
    order = _topo_order(spec, frozen_parents, frozen_children)

    return CompiledDAG(
        spec=spec,
        nodes=nodes,
        parents=frozen_parents,
        children=frozen_children,
        order=order,
    )


def _find_cycles(node_ids: list[str], children: dict[str, list[str]]) -> list[str]:
    """Return one ``cycle detected: a -> b -> a`` error per cycle found.

    Iterative DFS with an explicit gray-stack so long chains never hit the
    recursion limit. Cycles are reported in execution (``parent -> child``)
    order.
    """
    errors: list[str] = []
    color: dict[str, int] = {nid: 0 for nid in node_ids}  # 0=white 1=gray 2=black

    for start in node_ids:
        if color[start] != 0:
            continue
        color[start] = 1
        path = [start]
        stack: list[tuple[str, int]] = [(start, 0)]
        while stack:
            u, i = stack[-1]
            kids = children[u]
            if i < len(kids):
                stack[-1] = (u, i + 1)
                v = kids[i]
                if color[v] == 1:  # back edge into the active path -> cycle
                    idx = path.index(v)
                    errors.append("cycle detected: " + " -> ".join(path[idx:] + [v]))
                elif color[v] == 0:
                    color[v] = 1
                    path.append(v)
                    stack.append((v, 0))
            else:
                stack.pop()
                path.pop()
                color[u] = 2
    return errors


def _topo_order(
    spec: PipelineSpec,
    parents: dict[str, tuple[str, ...]],
    children: dict[str, tuple[str, ...]],
) -> tuple[str, ...]:
    """Kahn's algorithm, tie-broken by spec declaration index for determinism."""
    index = {n.id: i for i, n in enumerate(spec.nodes)}
    indeg = {nid: len(p) for nid, p in parents.items()}
    heap = [(index[nid], nid) for nid in parents if indeg[nid] == 0]
    heapq.heapify(heap)

    order: list[str] = []
    while heap:
        _, u = heapq.heappop(heap)
        order.append(u)
        for v in children[u]:
            indeg[v] -= 1
            if indeg[v] == 0:
                heapq.heappush(heap, (index[v], v))
    return tuple(order)
