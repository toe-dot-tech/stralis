"""Repository: the single write-path for jobs, node runs and artifacts.

Concurrency model (documented invariants):

* Every node execution is fenced by ``attempt``: transitions to RUNNING /
  SUCCEEDED only succeed when the row's attempt matches the caller's.
  A stale worker whose job was recovered mid-flight gets ``rowcount == 0``
  and must discard its result.
* Queue messages may be duplicated or lost; the DB is the source of truth.
  Lost messages are recovered by re-reading READY nodes with a stale
  ``queued_at``; duplicates are dropped at claim time.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import case, func, or_, select, update
from sqlalchemy.orm import Session, sessionmaker

from stralis.dag import compile_pipeline
from stralis.db.models import Artifact, Job, NodeRun
from stralis.models import (
    ArtifactRecord,
    ArtifactRef,
    JobRecord,
    JobStatus,
    NodeRunRecord,
    NodeStatus,
    PipelineSpec,
    utcnow,
)


def _aware(dt: datetime | None) -> datetime | None:
    """Naive datetimes (SQLite) are interpreted as UTC."""
    if dt is not None and dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _job_record(job: Job) -> JobRecord:
    return JobRecord(
        id=job.id,
        name=job.name,
        status=JobStatus(job.status),
        spec=PipelineSpec.model_validate_json(job.spec_json),
        error=job.error,
        created_at=_aware(job.created_at),  # type: ignore[arg-type]
        updated_at=_aware(job.updated_at),  # type: ignore[arg-type]
    )


def _run_record(run: NodeRun) -> NodeRunRecord:
    return NodeRunRecord(
        job_id=run.job_id,
        node_id=run.node_id,
        handler=run.handler,
        status=NodeStatus(run.status),
        attempt=run.attempt,
        max_attempts=run.max_attempts,
        node_hash=run.node_hash,
        outputs={
            k: ArtifactRef.model_validate(v) for k, v in (run.outputs or {}).items()
        },
        error=run.error,
        queued_at=_aware(run.queued_at),
        heartbeat_at=_aware(run.heartbeat_at),
        started_at=_aware(run.started_at),
        finished_at=_aware(run.finished_at),
        created_at=_aware(run.created_at),  # type: ignore[arg-type]
        updated_at=_aware(run.updated_at),  # type: ignore[arg-type]
    )


class Repo:
    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._sf = session_factory

    # ------------------------------------------------------------------ jobs

    def create_job(self, spec: PipelineSpec) -> JobRecord:
        """Validate + compile ``spec`` and persist the job with root nodes READY."""
        compiled = compile_pipeline(spec)  # raises PipelineValidationError
        job_id = str(uuid.uuid4())
        now = utcnow()
        with self._sf() as s, s.begin():
            job = Job(
                id=job_id,
                name=spec.name,
                status=JobStatus.PENDING.value,
                spec_json=spec.model_dump_json(),
                created_at=now,
                updated_at=now,
            )
            s.add(job)
            roots = set(compiled.roots())
            for node in spec.nodes:
                s.add(
                    NodeRun(
                        job_id=job_id,
                        node_id=node.id,
                        handler=node.handler,
                        status=(
                            NodeStatus.READY.value
                            if node.id in roots
                            else NodeStatus.PENDING.value
                        ),
                        attempt=0,
                        max_attempts=spec.max_attempts_for(node),
                        outputs={},
                        created_at=now,
                        updated_at=now,
                    )
                )
            s.flush()
            return _job_record(job)

    def get_job(self, job_id: str) -> JobRecord | None:
        with self._sf() as s:
            job = s.get(Job, job_id)
            return _job_record(job) if job else None

    def list_jobs(self, *, limit: int = 50, offset: int = 0) -> list[JobRecord]:
        with self._sf() as s:
            rows = s.scalars(
                select(Job)
                .order_by(Job.created_at.desc())
                .limit(limit)
                .offset(offset)
            ).all()
            return [_job_record(r) for r in rows]

    def list_active_jobs(self) -> list[JobRecord]:
        with self._sf() as s:
            rows = s.scalars(
                select(Job)
                .where(Job.status.in_([JobStatus.PENDING.value, JobStatus.RUNNING.value]))
                .order_by(Job.created_at.asc())
            ).all()
            return [_job_record(r) for r in rows]

    def set_job_status(
        self, job_id: str, status: JobStatus, error: str | None = None
    ) -> bool:
        with self._sf() as s, s.begin():
            res = s.execute(
                update(Job)
                .where(Job.id == job_id)
                .values(status=status.value, error=error, updated_at=utcnow())
            )
            return res.rowcount > 0

    # ------------------------------------------------------------- node runs

    def get_node_runs(self, job_id: str) -> list[NodeRunRecord]:
        with self._sf() as s:
            rows = s.scalars(
                select(NodeRun)
                .where(NodeRun.job_id == job_id)
                .order_by(NodeRun.created_at.asc())
            ).all()
            return [_run_record(r) for r in rows]

    def get_node_run(self, job_id: str, node_id: str) -> NodeRunRecord | None:
        with self._sf() as s:
            row = s.scalars(
                select(NodeRun).where(
                    NodeRun.job_id == job_id, NodeRun.node_id == node_id
                )
            ).one_or_none()
            return _run_record(row) if row else None

    def promote_ready_nodes(self, job_id: str) -> list[NodeRunRecord]:
        """PENDING -> READY where every parent already SUCCEEDED.

        Returns only rows this caller actually transitioned (conditional
        UPDATE), so duplicate orchestrators cannot double-report.
        """
        newly: list[NodeRunRecord] = []
        with self._sf() as s, s.begin():
            rows = s.scalars(select(NodeRun).where(NodeRun.job_id == job_id)).all()
            status_by_id = {r.node_id: r.status for r in rows}
            spec = PipelineSpec.model_validate_json(
                s.get(Job, job_id).spec_json  # type: ignore[union-attr]
            )
            for run in rows:
                if run.status != NodeStatus.PENDING.value:
                    continue
                node = spec.node_map()[run.node_id]
                if all(
                    status_by_id.get(p) == NodeStatus.SUCCEEDED.value
                    for p in node.depends_on
                ):
                    res = s.execute(
                        update(NodeRun)
                        .where(
                            NodeRun.id == run.id,
                            NodeRun.status == NodeStatus.PENDING.value,
                        )
                        .values(status=NodeStatus.READY.value, updated_at=utcnow())
                    )
                    if res.rowcount > 0:
                        run.status = NodeStatus.READY.value
                        newly.append(_run_record(run))
        return newly

    def promote_blocked_nodes(self, job_id: str) -> list[NodeRunRecord]:
        """PENDING -> SKIPPED when any parent is FAILED or SKIPPED (cascade)."""
        newly: list[NodeRunRecord] = []
        with self._sf() as s, s.begin():
            rows = s.scalars(select(NodeRun).where(NodeRun.job_id == job_id)).all()
            status_by_id = {r.node_id: r.status for r in rows}
            spec = PipelineSpec.model_validate_json(
                s.get(Job, job_id).spec_json  # type: ignore[union-attr]
            )
            for run in rows:
                if run.status != NodeStatus.PENDING.value:
                    continue
                node = spec.node_map()[run.node_id]
                if any(
                    status_by_id.get(p)
                    in (NodeStatus.FAILED.value, NodeStatus.SKIPPED.value)
                    for p in node.depends_on
                ):
                    res = s.execute(
                        update(NodeRun)
                        .where(
                            NodeRun.id == run.id,
                            NodeRun.status == NodeStatus.PENDING.value,
                        )
                        .values(
                            status=NodeStatus.SKIPPED.value,
                            finished_at=utcnow(),
                            updated_at=utcnow(),
                        )
                    )
                    if res.rowcount > 0:
                        run.status = NodeStatus.SKIPPED.value
                        newly.append(_run_record(run))
        return newly

    def due_ready_nodes(
        self, *, stale_after_s: float = 30.0, limit: int = 100, job_id: str | None = None
    ) -> list[NodeRunRecord]:
        """READY nodes whose queue message is missing or presumed lost.

        ``queued_at IS NULL`` (never enqueued) or older than ``stale_after_s``
        (enqueue lost / delivery dead). Re-enqueueing duplicates is safe —
        claims are fenced by attempt.
        """
        cutoff = utcnow() - timedelta(seconds=stale_after_s)
        with self._sf() as s:
            q = select(NodeRun).where(NodeRun.status == NodeStatus.READY.value)
            if job_id is not None:
                q = q.where(NodeRun.job_id == job_id)
            q = q.order_by(NodeRun.queued_at.is_(None).desc(), NodeRun.queued_at.asc())
            rows = s.scalars(q.limit(limit * 3)).all()
            due = [
                r
                for r in rows
                if _aware(r.queued_at) is None or _aware(r.queued_at) < cutoff  # type: ignore[operator]
            ]
            return [_run_record(r) for r in due[:limit]]

    def mark_queued(self, job_id: str, node_id: str) -> bool:
        with self._sf() as s, s.begin():
            res = s.execute(
                update(NodeRun)
                .where(
                    NodeRun.job_id == job_id,
                    NodeRun.node_id == node_id,
                    NodeRun.status == NodeStatus.READY.value,
                )
                .values(queued_at=utcnow(), updated_at=utcnow())
            )
            return res.rowcount > 0

    def claim_node(
        self, job_id: str, node_id: str, attempt: int
    ) -> NodeRunRecord | None:
        """READY(attempt) -> RUNNING. None ⇒ stale/duplicate delivery."""
        now = utcnow()
        with self._sf() as s, s.begin():
            res = s.execute(
                update(NodeRun)
                .where(
                    NodeRun.job_id == job_id,
                    NodeRun.node_id == node_id,
                    NodeRun.status == NodeStatus.READY.value,
                    NodeRun.attempt == attempt,
                )
                .values(
                    status=NodeStatus.RUNNING.value,
                    started_at=now,
                    heartbeat_at=now,
                    error=None,
                    updated_at=now,
                )
            )
            if res.rowcount == 0:
                return None
            row = s.scalars(
                select(NodeRun).where(
                    NodeRun.job_id == job_id, NodeRun.node_id == node_id
                )
            ).one()
            return _run_record(row)

    def heartbeat(self, job_id: str, node_id: str, attempt: int) -> bool:
        with self._sf() as s, s.begin():
            res = s.execute(
                update(NodeRun)
                .where(
                    NodeRun.job_id == job_id,
                    NodeRun.node_id == node_id,
                    NodeRun.status == NodeStatus.RUNNING.value,
                    NodeRun.attempt == attempt,
                )
                .values(heartbeat_at=utcnow(), updated_at=utcnow())
            )
            return res.rowcount > 0

    def complete_node(
        self,
        job_id: str,
        node_id: str,
        attempt: int,
        node_hash: str,
        outputs: dict[str, ArtifactRef],
    ) -> bool:
        """RUNNING(attempt) -> SUCCEEDED + persist artifact rows (fenced)."""
        now = utcnow()
        payload = {k: v.model_dump() for k, v in outputs.items()}
        with self._sf() as s, s.begin():
            res = s.execute(
                update(NodeRun)
                .where(
                    NodeRun.job_id == job_id,
                    NodeRun.node_id == node_id,
                    NodeRun.status == NodeStatus.RUNNING.value,
                    NodeRun.attempt == attempt,
                )
                .values(
                    status=NodeStatus.SUCCEEDED.value,
                    node_hash=node_hash,
                    outputs=payload,
                    error=None,
                    finished_at=now,
                    heartbeat_at=now,
                    updated_at=now,
                )
            )
            if res.rowcount == 0:
                return False
            s.execute(
                Artifact.__table__.delete().where(
                    Artifact.job_id == job_id, Artifact.node_id == node_id
                )
            )
            for name, ref in outputs.items():
                s.add(
                    Artifact(
                        job_id=job_id,
                        node_id=node_id,
                        name=name,
                        uri=ref.uri,
                        digest=ref.digest,
                        size=ref.size,
                        content_type=ref.content_type,
                        created_at=now,
                    )
                )
            return True

    def fail_node(
        self, job_id: str, node_id: str, attempt: int, error: str
    ) -> NodeRunRecord | None:
        """RUNNING(attempt) -> READY(attempt+1) if retries remain, else FAILED."""
        now = utcnow()
        with self._sf() as s, s.begin():
            row = s.scalars(
                select(NodeRun).where(
                    NodeRun.job_id == job_id, NodeRun.node_id == node_id
                )
            ).one_or_none()
            if (
                row is None
                or row.status != NodeStatus.RUNNING.value
                or row.attempt != attempt
            ):
                return None  # fenced — someone else owns this transition
            if attempt + 1 < row.max_attempts:
                row.status = NodeStatus.READY.value
                row.attempt = attempt + 1
                row.queued_at = None
                row.heartbeat_at = None
                row.started_at = None
                row.error = error
            else:
                row.status = NodeStatus.FAILED.value
                row.error = error
                row.finished_at = now
            row.updated_at = now
            s.flush()
            return _run_record(row)

    def stale_running(self, older_than: datetime) -> list[NodeRunRecord]:
        """RUNNING rows whose heartbeat predates ``older_than`` (worker died).

        A NULL heartbeat counts as stale — a worker that died before its first
        beat must still be recovered.
        """
        with self._sf() as s:
            rows = s.scalars(
                select(NodeRun).where(
                    NodeRun.status == NodeStatus.RUNNING.value,
                    or_(
                        NodeRun.heartbeat_at.is_(None),
                        NodeRun.heartbeat_at < older_than,
                    ),
                )
            ).all()
            return [_run_record(r) for r in rows]

    def recover_stale(
        self,
        job_id: str,
        node_id: str,
        attempt: int,
        older_than: datetime,
        reason: str,
    ) -> NodeRunRecord | None:
        """Requeue (or fail) a stale RUNNING node. Fenced by heartbeat cutoff:
        a live worker that just beat is not stolen from."""
        now = utcnow()
        with self._sf() as s, s.begin():
            row = s.scalars(
                select(NodeRun).where(
                    NodeRun.job_id == job_id, NodeRun.node_id == node_id
                )
            ).one_or_none()
            if row is None or row.status != NodeStatus.RUNNING.value or row.attempt != attempt:
                return None  # fenced — someone else owns this transition
            hb = _aware(row.heartbeat_at)
            if hb is not None and hb >= older_than:
                return None  # fenced: a live worker just refreshed its heartbeat
            if attempt + 1 < row.max_attempts:
                row.status = NodeStatus.READY.value
                row.attempt = attempt + 1
                row.queued_at = None
                row.heartbeat_at = None
                row.started_at = None
                row.error = reason
            else:
                row.status = NodeStatus.FAILED.value
                row.error = reason
                row.finished_at = now
            row.updated_at = now
            s.flush()
            return _run_record(row)

    def finalize_job_if_done(self, job_id: str) -> JobRecord | None:
        """Set the job's terminal status once every node run is terminal."""
        now = utcnow()
        with self._sf() as s, s.begin():
            job = s.get(Job, job_id)
            if job is None or job.status in (
                JobStatus.SUCCEEDED.value,
                JobStatus.FAILED.value,
                JobStatus.CANCELLED.value,
            ):
                return None
            total, done = s.execute(
                select(
                    func.count(),
                    func.sum(
                        case(
                            (
                                NodeRun.status.in_(
                                    [
                                        NodeStatus.SUCCEEDED.value,
                                        NodeStatus.FAILED.value,
                                        NodeStatus.SKIPPED.value,
                                    ]
                                ),
                                1,
                            ),
                            else_=0,
                        )
                    ),
                ).where(NodeRun.job_id == job_id)
            ).one()
            if total == 0 or (done or 0) < total:
                return None
            all_ok = s.scalars(
                select(func.count())
                .where(
                    NodeRun.job_id == job_id,
                    NodeRun.status != NodeStatus.SUCCEEDED.value,
                )
            ).one()
            if all_ok == 0:
                job.status = JobStatus.SUCCEEDED.value
                job.error = None
            else:
                job.status = JobStatus.FAILED.value
                first_bad = s.scalars(
                    select(NodeRun.error)
                    .where(
                        NodeRun.job_id == job_id,
                        NodeRun.status.in_(
                            [NodeStatus.FAILED.value, NodeStatus.SKIPPED.value]
                        ),
                    )
                    .limit(1)
                ).one_or_none()
                job.error = first_bad or "one or more nodes did not succeed"
            job.updated_at = now
            return _job_record(job)

    def retry_job(self, job_id: str) -> JobRecord | None:
        """Resume a FAILED job: successful nodes keep their checkpoints,
        failed/skipped nodes are reset for re-execution."""
        now = utcnow()
        with self._sf() as s, s.begin():
            job = s.get(Job, job_id)
            if job is None or job.status != JobStatus.FAILED.value:
                return None
            s.execute(
                update(NodeRun)
                .where(
                    NodeRun.job_id == job_id,
                    NodeRun.status.in_([NodeStatus.FAILED.value, NodeStatus.SKIPPED.value]),
                )
                .values(
                    status=NodeStatus.PENDING.value,
                    attempt=0,
                    queued_at=None,
                    heartbeat_at=None,
                    started_at=None,
                    finished_at=None,
                    error=None,
                    updated_at=now,
                )
            )
            job.status = JobStatus.RUNNING.value
            job.error = None
            job.updated_at = now
            s.flush()
            return _job_record(job)

    # ------------------------------------------------------------- artifacts

    def list_artifacts(self, job_id: str) -> list[ArtifactRecord]:
        with self._sf() as s:
            rows = s.scalars(
                select(Artifact)
                .where(Artifact.job_id == job_id)
                .order_by(Artifact.created_at.asc())
            ).all()
            return [
                ArtifactRecord(
                    job_id=r.job_id,
                    node_id=r.node_id,
                    name=r.name,
                    uri=r.uri,
                    digest=r.digest,
                    size=r.size,
                    content_type=r.content_type,
                    created_at=_aware(r.created_at),  # type: ignore[arg-type]
                )
                for r in rows
            ]


__all__ = ["Repo"]
