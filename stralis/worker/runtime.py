"""Worker runtime: claim a task, execute one DAG node, checkpoint the result.

Resilience invariants
---------------------
* **Fenced claims** — a worker only owns a node when ``claim_node`` succeeds
  for its exact ``attempt``. A recovered/requeued node (bumped attempt) makes
  every later write from the old worker a no-op, so a slow-but-alive worker
  can never corrupt state owned by a replacement.
* **Idempotent execution** — each node has a content hash over
  (handler, params, upstream artifact digests). If a prior attempt already
  produced outputs for that hash, the worker skips re-execution and simply
  re-confirms success.
* **Claim-check** — inputs are downloaded from the object store to local
  scratch; outputs are uploaded back; only URIs ever touch the queue.
"""

from __future__ import annotations

import logging
import shutil
import threading
import traceback
from pathlib import Path

from stralis.config import Config, default_consumer_name
from stralis.models import (
    ArtifactRef,
    NodeRunRecord,
    NodeStatus,
    TaskMessage,
    compute_node_hash,
)
from stralis.ports.queue import ClaimedMessage, TaskQueue
from stralis.ports.storage import ObjectStore
from stralis.db.repo import Repo
from stralis.worker.handlers import Handler, NodeContext, default_handlers, get_handler

log = logging.getLogger("stralis.worker")


class Worker:
    def __init__(
        self,
        repo: Repo,
        queue: TaskQueue,
        store: ObjectStore,
        config: Config | None = None,
        *,
        handlers: dict[str, Handler] | None = None,
        consumer: str | None = None,
    ) -> None:
        self.repo = repo
        self.queue = queue
        self.store = store
        self.config = config or Config()
        self.handlers = handlers if handlers is not None else default_handlers()
        self.consumer = consumer or default_consumer_name()
        self._stop = threading.Event()

    # ------------------------------------------------------------------ loops

    def run_forever(self) -> None:
        self.queue.setup()
        log.info("worker %s started", self.consumer)
        while not self._stop.is_set():
            self.tick(block_ms=1000)

    def run_until_empty(self) -> int:
        """Drain all currently-available messages (tests / one-shot mode)."""
        self.queue.setup()
        handled = 0
        while True:
            before = handled
            handled += self.tick(block_ms=0)
            if handled == before:
                return handled

    def stop(self) -> None:
        self._stop.set()

    def tick(self, *, block_ms: int = 1000) -> int:
        """One poll: reclaim idle messages, then read new ones."""
        handled = 0
        # Recover messages left pending by dead workers (Redis PEL).
        try:
            for claimed in self.queue.reclaim(
                self.consumer,
                min_idle_ms=int(self.config.heartbeat_timeout_s * 1000),
            ):
                self.handle(claimed)
                handled += 1
        except Exception:  # pragma: no cover - backend hiccup must not kill the loop
            log.exception("reclaim failed")

        try:
            messages = self.queue.get(self.consumer, count=1, block_ms=block_ms)
        except Exception:  # pragma: no cover
            log.exception("queue read failed")
            return handled

        for claimed in messages:
            self.handle(claimed)
            handled += 1
        return handled

    # -------------------------------------------------------------- execution

    def handle(self, claimed: ClaimedMessage) -> None:
        """Process one delivery. Always acks (at-least-once + fenced writes)."""
        msg: TaskMessage = claimed.message
        attempt = msg.attempt
        try:
            run = self.repo.claim_node(msg.job_id, msg.node_id, attempt)
            if run is None:
                # Duplicate delivery, already-finished node, or a stale attempt
                # that was recovered/requeued elsewhere. Nothing to do.
                log.debug(
                    "skip fenced/duplicate delivery %s/%s attempt=%s",
                    msg.job_id,
                    msg.node_id,
                    attempt,
                )
                return
            self._execute(run)
        except Exception as e:  # noqa: BLE001 - a node failure must never kill the worker
            log.exception("node %s/%s crashed", msg.job_id, msg.node_id)
            self._fail(msg.job_id, msg.node_id, attempt, _format_error(e))
        finally:
            self.queue.ack(claimed)

    def _execute(self, run: NodeRunRecord) -> None:
        job = self.repo.get_job(run.job_id)
        if job is None:
            self._fail(run.job_id, run.node_id, run.attempt, "job disappeared")
            return
        node = job.spec.node_map()[run.node_id]

        # --- resolve upstream artifacts (claim-check references) ------------
        parent_outputs: dict[str, ArtifactRef] = {}
        for parent_id in node.depends_on:
            parent_run = self.repo.get_node_run(run.job_id, parent_id)
            if parent_run is None or parent_run.status is not NodeStatus.SUCCEEDED:
                self._fail(
                    run.job_id, run.node_id, run.attempt,
                    f"parent {parent_id!r} is not successful",
                )
                return
            parent_outputs.update(parent_run.outputs)

        node_hash = compute_node_hash(
            node.handler, node.params, [a.digest for a in parent_outputs.values()]
        )

        # --- idempotency: reuse a checkpoint from an identical prior run ----
        if run.node_hash == node_hash and run.outputs and self._outputs_exist(run):
            log.info(
                "checkpoint hit %s/%s hash=%s - skipping execution",
                run.job_id,
                run.node_id,
                node_hash[:12],
            )
            self.repo.complete_node(
                run.job_id, run.node_id, run.attempt, node_hash, run.outputs
            )
            return

        workdir = self._workdir(run.job_id, run.node_id, run.attempt)
        stop_hb = self._start_heartbeat(run.job_id, run.node_id, run.attempt)
        try:
            inputs = self._download_inputs(parent_outputs, workdir)
            ctx = NodeContext(
                job_id=run.job_id,
                node_id=run.node_id,
                attempt=run.attempt,
                params=node.params,
                inputs=inputs,
                workdir=workdir,
                log=log,
            )
            handler = self._handler_for(node.handler)
            written = handler(ctx) or {}
            outputs = self._upload_outputs(run.job_id, run.node_id, node_hash, written)

            ok = self.repo.complete_node(
                run.job_id, run.node_id, run.attempt, node_hash, outputs
            )
            if not ok:
                log.warning(
                    "completion fenced for %s/%s attempt=%s (recovered elsewhere)",
                    run.job_id,
                    run.node_id,
                    run.attempt,
                )
        except Exception as e:  # noqa: BLE001
            self._fail(run.job_id, run.node_id, run.attempt, _format_error(e))
        finally:
            stop_hb.set()
            shutil.rmtree(workdir, ignore_errors=True)

    def _handler_for(self, name: str) -> Handler:
        if name not in self.handlers:
            get_handler(name)  # raises UnknownHandlerError with a helpful message
            raise KeyError(name)
        return self.handlers[name]

    def _fail(self, job_id: str, node_id: str, attempt: int, error: str) -> None:
        updated = self.repo.fail_node(job_id, node_id, attempt, error)
        if updated is None:
            log.debug("failure fenced for %s/%s attempt=%s", job_id, node_id, attempt)
        elif updated.status is NodeStatus.READY:
            log.info(
                "node %s/%s requeued (attempt %s/%s)",
                job_id, node_id, updated.attempt, updated.max_attempts,
            )
        else:
            log.warning("node %s/%s FAILED: %s", job_id, node_id, error)

    # ------------------------------------------------------------------ io

    def _workdir(self, job_id: str, node_id: str, attempt: int) -> Path:
        # Absolute: handlers may spawn subprocesses with cwd=workdir, so any
        # relative path we hand them would otherwise resolve against the wrong
        # directory.
        path = (
            Path(self.config.work_root).resolve()
            / job_id
            / f"{node_id}.a{attempt}.{threading.get_ident()}"
        )
        (path / "inputs").mkdir(parents=True, exist_ok=True)
        (path / "outputs").mkdir(parents=True, exist_ok=True)
        return path

    def _download_inputs(
        self, parent_outputs: dict[str, ArtifactRef], workdir: Path
    ) -> dict[str, Path]:
        inputs: dict[str, Path] = {}
        for name, ref in parent_outputs.items():
            dest = workdir / "inputs" / name.replace("/", "_")
            dest.parent.mkdir(parents=True, exist_ok=True)
            self.store.get(ref.uri, dest)
            inputs[name] = dest
        return inputs

    def _upload_outputs(
        self,
        job_id: str,
        node_id: str,
        node_hash: str,
        written: dict[str, Path],
    ) -> dict[str, ArtifactRef]:
        outputs: dict[str, ArtifactRef] = {}
        for name, path in written.items():
            key = f"{job_id}/{node_id}/{node_hash[:16]}/{name}"
            outputs[name] = self.store.put_file(key, Path(path), name=name)
        return outputs

    def _outputs_exist(self, run: NodeRunRecord) -> bool:
        try:
            return all(self.store.exists(ref.uri) for ref in run.outputs.values())
        except Exception:  # pragma: no cover - conservative: re-execute on doubt
            return False

    def _start_heartbeat(self, job_id: str, node_id: str, attempt: int) -> threading.Event:
        stop = threading.Event()
        interval = max(0.01, float(self.config.heartbeat_interval_s))

        def beat() -> None:
            while not stop.wait(interval):
                try:
                    if not self.repo.heartbeat(job_id, node_id, attempt):
                        state = self.repo.get_node_run(job_id, node_id)
                        if state is not None and state.status.terminal:
                            log.debug(
                                "heartbeat stopped (node already %s)",
                                state.status.value,
                            )
                        else:
                            log.warning(
                                "lost ownership of %s/%s attempt=%s "
                                "(recovered elsewhere); result will be discarded",
                                job_id, node_id, attempt,
                            )
                        return
                except Exception:  # pragma: no cover
                    log.exception("heartbeat error for %s/%s", job_id, node_id)

        threading.Thread(
            target=beat, name=f"hb-{node_id}", daemon=True
        ).start()
        return stop


def _format_error(exc: BaseException) -> str:
    if isinstance(exc, Exception) and exc.__class__.__name__ == "HandlerExecutionError":
        return str(exc)
    tb = traceback.format_exc(limit=6)
    return f"{type(exc).__name__}: {exc}\n{tb}"[:4000]


__all__ = ["Worker"]
