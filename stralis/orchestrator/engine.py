"""Orchestrator: the DAG scheduler and crash-recovery sweeper.

Each sweep (idempotent, safe to run concurrently with workers):

1. ``promote_ready_nodes``  — PENDING -> READY once every parent succeeded.
2. ``promote_blocked_nodes`` — PENDING -> SKIPPED once any parent failed/skipped.
3. enqueue READY nodes whose queue message is missing or presumed lost.
4. recover RUNNING nodes whose worker stopped heartbeating (crash/OOM).
5. finalize the job when every node is terminal.

The database — not the queue — is the source of truth, so duplicate or lost
queue messages are harmless: claims are fenced by attempt and lost messages
are re-derived from state.
"""

from __future__ import annotations

import logging
import time
from datetime import timedelta

from stralis.config import Config, default_consumer_name
from stralis.db.repo import Repo
from stralis.models import JobStatus, NodeStatus, TaskMessage, utcnow
from stralis.ports.queue import TaskQueue

log = logging.getLogger("stralis.orchestrator")

TERMINAL_JOB_STATUSES = {
    JobStatus.SUCCEEDED,
    JobStatus.FAILED,
    JobStatus.CANCELLED,
}


class Orchestrator:
    def __init__(
        self,
        repo: Repo,
        queue: TaskQueue,
        config: Config | None = None,
        *,
        enqueue_stale_after_s: float = 30.0,
        consumer: str | None = None,
    ) -> None:
        self.repo = repo
        self.queue = queue
        self.config = config or Config()
        self.enqueue_stale_after_s = enqueue_stale_after_s
        self.consumer = consumer or f"orchestrator-{default_consumer_name()}"
        self._stop = False

    # ------------------------------------------------------------------ loop

    def run_forever(self) -> None:
        self.queue.setup()
        log.info("orchestrator started")
        while not self._stop:
            try:
                self.sweep_once()
            except Exception:  # pragma: no cover - never die on one bad sweep
                log.exception("sweep failed")
            time.sleep(self.config.sweep_interval_s)

    def stop(self) -> None:
        self._stop = True

    # ----------------------------------------------------------------- sweep

    def sweep_once(self) -> dict[str, int]:
        """Run one scheduling pass over all active jobs."""
        recovered = self._recover_stale_runs()
        stats = {
            "ready": 0,
            "blocked": 0,
            "enqueued": 0,
            "recovered": recovered,
            "finalized": 0,
        }
        for job in self.repo.list_active_jobs():
            stats["ready"] += len(self.repo.promote_ready_nodes(job.id))
            stats["blocked"] += len(self.repo.promote_blocked_nodes(job.id))
            stats["enqueued"] += self._enqueue_ready(job.id)
            if self.repo.finalize_job_if_done(job.id) is not None:
                stats["finalized"] += 1
        return stats

    # ------------------------------------------------------------- internals

    def _enqueue_ready(self, job_id: str) -> int:
        due = self.repo.due_ready_nodes(
            stale_after_s=self.enqueue_stale_after_s, job_id=job_id
        )
        count = 0
        for run in due:
            self.queue.put(
                TaskMessage(job_id=run.job_id, node_id=run.node_id, attempt=run.attempt)
            )
            self.repo.mark_queued(run.job_id, run.node_id)
            self._mark_job_running_if_needed(run.job_id)
            count += 1
        return count

    def _mark_job_running_if_needed(self, job_id: str) -> None:
        job = self.repo.get_job(job_id)
        if job is not None and job.status is JobStatus.PENDING:
            self.repo.set_job_status(job_id, JobStatus.RUNNING)

    def _recover_stale_runs(self) -> int:
        cutoff = utcnow() - timedelta(seconds=self.config.heartbeat_timeout_s)
        recovered = 0
        for run in self.repo.stale_running(cutoff):
            updated = self.repo.recover_stale(
                run.job_id,
                run.node_id,
                run.attempt,
                cutoff,
                reason=(
                    f"worker heartbeat timeout after {self.config.heartbeat_timeout_s}s"
                ),
            )
            if updated is None:
                continue
            recovered += 1
            if updated.status is NodeStatus.READY:
                log.warning(
                    "recovered stale node %s/%s -> requeued attempt %s",
                    run.job_id,
                    run.node_id,
                    updated.attempt,
                )
            else:
                log.warning(
                    "recovered stale node %s/%s -> FAILED (retries exhausted)",
                    run.job_id,
                    run.node_id,
                )
        return recovered


__all__ = ["Orchestrator"]
