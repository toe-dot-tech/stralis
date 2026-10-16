"""Domain models shared across the Stralis engine.

Everything here is infrastructure-free (pydantic + stdlib) so the DAG
compiler, orchestrator, workers, API and CLI all speak the same language.
"""

from __future__ import annotations

import enum
import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, field_validator

NODE_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------
# Enums
# --------------------------------------------------------------------------


class JobStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED)


class NodeStatus(str, enum.Enum):
    PENDING = "pending"      # parents not finished yet
    READY = "ready"          # all parents succeeded, waiting for a worker
    RUNNING = "running"      # claimed by a worker (fenced by attempt)
    SUCCEEDED = "succeeded"  # execution finished successfully
    FAILED = "failed"        # execution failed (retries exhausted)
    SKIPPED = "skipped"      # blocked by a failed/terminal-unsuccessful parent

    @property
    def terminal(self) -> bool:
        return self in (
            NodeStatus.SUCCEEDED,
            NodeStatus.FAILED,
            NodeStatus.SKIPPED,
        )


# --------------------------------------------------------------------------
# Pipeline specification
# --------------------------------------------------------------------------


class NodeSpec(BaseModel):
    """A single node in a pipeline."""

    id: str = Field(description="Unique node identifier within the pipeline.")
    handler: str = Field(description="Name of the registered handler to execute.")
    name: str | None = Field(default=None, description="Human-readable label.")
    params: dict[str, Any] = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list)
    max_attempts: int | None = Field(
        default=None, description="Per-node override of the pipeline retry budget."
    )

    @field_validator("id")
    @classmethod
    def _check_id(cls, v: str) -> str:
        if not NODE_ID_RE.match(v):
            raise ValueError(
                "node id must match [A-Za-z0-9_.-]+ (no spaces/slashes)"
            )
        return v

    @field_validator("max_attempts")
    @classmethod
    def _check_attempts(cls, v: int | None) -> int | None:
        if v is not None and v < 1:
            raise ValueError("max_attempts must be >= 1")
        return v


class PipelineSpec(BaseModel):
    """A full multi-step pipeline, compiled into a DAG before execution."""

    name: str
    nodes: list[NodeSpec] = Field(min_length=1)
    max_attempts: int = Field(
        default=3, description="Default retry budget per node."
    )
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("max_attempts")
    @classmethod
    def _check_attempts(cls, v: int) -> int:
        if v < 1:
            raise ValueError("max_attempts must be >= 1")
        return v

    @field_validator("nodes")
    @classmethod
    def _check_unique_ids(cls, v: list[NodeSpec]) -> list[NodeSpec]:
        seen: set[str] = set()
        dupes: set[str] = set()
        for n in v:
            if n.id in seen:
                dupes.add(n.id)
            seen.add(n.id)
        if dupes:
            raise ValueError(f"duplicate node ids: {sorted(dupes)}")
        return v

    def node_map(self) -> dict[str, NodeSpec]:
        return {n.id: n for n in self.nodes}

    def max_attempts_for(self, node: NodeSpec) -> int:
        return node.max_attempts if node.max_attempts is not None else self.max_attempts


# --------------------------------------------------------------------------
# Transport / artifact payloads
# --------------------------------------------------------------------------


class TaskMessage(BaseModel):
    """Lightweight queue payload — never carries media bytes (claim-check)."""

    job_id: str
    node_id: str
    attempt: int = 0


class ArtifactRef(BaseModel):
    """A persisted artifact (the 'claim check' handed between nodes)."""

    name: str
    uri: str
    digest: str = Field(description="sha256 hex digest of the bytes.")
    size: int = 0
    content_type: str | None = None


class ArtifactRecord(BaseModel):
    """A persisted artifact row (claim-check provenance)."""

    job_id: str
    node_id: str
    name: str
    uri: str
    digest: str
    size: int = 0
    content_type: str | None = None
    created_at: datetime


# --------------------------------------------------------------------------
# Persistence records (repo-facing, ORM-independent)
# --------------------------------------------------------------------------


class JobRecord(BaseModel):
    id: str
    name: str
    status: JobStatus
    spec: PipelineSpec
    error: str | None = None
    created_at: datetime
    updated_at: datetime


class NodeRunRecord(BaseModel):
    job_id: str
    node_id: str
    handler: str
    status: NodeStatus
    attempt: int = 0
    max_attempts: int = 3
    node_hash: str | None = None
    outputs: dict[str, ArtifactRef] = Field(default_factory=dict)
    error: str | None = None
    queued_at: datetime | None = None
    heartbeat_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


# --------------------------------------------------------------------------
# Idempotency
# --------------------------------------------------------------------------


def compute_node_hash(
    handler: str, params: dict[str, Any], input_digests: list[str]
) -> str:
    """Deterministic content hash for a node execution.

    Same handler + same params + same upstream artifact digests ⇒ same hash.
    Workers use it to detect "already produced this output" and skip
    re-execution (idempotent nodes / checkpoint resume).
    """
    canonical = json.dumps(
        {
            "handler": handler,
            "params": params,
            "inputs": sorted(input_digests),
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
