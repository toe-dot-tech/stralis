"""Crash-resume semantics: a failed pipeline restarts at the failed node only.

This is the headline behaviour from the Stralis case study: if step 4 of a
5-step pipeline OOMs, steps 1-3 are never re-run. Successful upstream nodes are
checkpointed (outputs persisted + content-hash recorded) and are reused.
"""

from __future__ import annotations

from stralis.models import JobStatus, NodeStatus, PipelineSpec

from tests.conftest import counter_handler, drive, make_worker, make_orchestrator


class ChainHandlers:
    """step1 always succeeds; step2 fails until ``flaky_attempts`` reached; step3 counts."""

    def __init__(self, flaky_attempts: int) -> None:
        self.calls: dict[str, int] = {}
        self.flaky_attempts = flaky_attempts

    def __call__(self) -> dict:
        return {
            "step1": self._step1,
            "step2": self._step2,
            "step3": self._step3,
        }

    def _write(self, ctx, name, data):
        p = ctx.output_path(name)
        p.write_text(data, encoding="utf-8")
        return p

    def _step1(self, ctx):
        self.calls["step1"] = self.calls.get("step1", 0) + 1
        return {"s1": self._write(ctx, "s1", "one")}

    def _step2(self, ctx):
        self.calls["step2"] = self.calls.get("step2", 0) + 1
        if self.calls["step2"] <= self.flaky_attempts:
            raise RuntimeError("GPU OOM")
        got = ctx.require_input("s1").read_text(encoding="utf-8")
        return {"s2": self._write(ctx, "s2", got + "+two")}

    def _step3(self, ctx):
        self.calls["step3"] = self.calls.get("step3", 0) + 1
        got = ctx.require_input("s2").read_text(encoding="utf-8")
        return {"s3": self._write(ctx, "s3", got + "+three")}


def chain_spec(max_attempts: int = 3) -> PipelineSpec:
    return PipelineSpec(
        name="five-step",
        max_attempts=max_attempts,
        nodes=[
            {"id": "step1", "handler": "step1", "params": {}},
            {"id": "step2", "handler": "step2", "params": {}, "depends_on": ["step1"]},
            {"id": "step3", "handler": "step3", "params": {}, "depends_on": ["step2"]},
        ],
    )


def test_resume_after_transient_worker_crash_reuses_upstream(repo, queue, store, config):
    h = ChainHandlers(flaky_attempts=1)
    job = repo.create_job(chain_spec(max_attempts=3))
    status = drive(repo, queue, store, config, h())

    assert status is JobStatus.SUCCEEDED
    # step1 ran exactly once: the retry after the step2 crash did NOT restart the DAG
    assert h.calls["step1"] == 1
    assert h.calls["step2"] == 2  # one failure, one success
    assert h.calls["step3"] == 1

    final = repo.get_node_run(job.id, "step3")
    assert store.read_bytes(final.outputs["s3"].uri) == b"one+two+three"


def test_global_failure_then_manual_retry_resumes_at_failure_point(repo, queue, store, config):
    """Exhausted retries -> FAILED; an operator retry must not redo step1."""
    h = ChainHandlers(flaky_attempts=99)
    job = repo.create_job(chain_spec(max_attempts=1))
    status = drive(repo, queue, store, config, h())
    assert status is JobStatus.FAILED
    assert h.calls["step1"] == 1
    assert h.calls.get("step3", 0) == 0  # downstream never ran

    st = {r.node_id: r.status for r in repo.get_node_runs(job.id)}
    assert st["step1"] is NodeStatus.SUCCEEDED
    assert st["step2"] is NodeStatus.FAILED
    assert st["step3"] is NodeStatus.SKIPPED

    step1_outputs_before = repo.get_node_run(job.id, "step1").outputs

    # operator flaps the bug and retries the job
    repo.retry_job(job.id)
    h.flaky_attempts = 0  # next attempt succeeds
    orch = make_orchestrator(repo, queue, config)
    worker = make_worker(repo, queue, store, config, h())
    for _ in range(20):
        orch.sweep_once()
        worker.run_until_empty()

    assert repo.get_job(job.id).status is JobStatus.SUCCEEDED
    assert h.calls["step1"] == 1  # 0% restart penalty — checkpoint reused
    assert h.calls["step2"] == 2  # one failure + one success after the budget reset
    assert h.calls["step3"] == 1
    assert repo.get_node_run(job.id, "step1").outputs == step1_outputs_before


def test_resume_does_not_reuse_stale_hash_when_inputs_change(repo, queue, store, config):
    """A changed upstream artifact changes the downstream hash -> must re-run."""
    spec = PipelineSpec(
        name="sensitive",
        nodes=[
            {"id": "a", "handler": "variant", "params": {}},
            {"id": "b", "handler": "count", "params": {}, "depends_on": ["a"]},
        ],
    )
    job = repo.create_job(spec)
    flag = {"v": "first"}
    counter: dict[str, int] = {}

    def variant(ctx):
        p = ctx.output_path("out")
        p.write_text(flag["v"], encoding="utf-8")
        return {"out": p}

    handlers = {"variant": variant, "count": counter_handler(counter, "b")}
    status = drive(repo, queue, store, config, handlers)
    assert status is JobStatus.SUCCEEDED
    assert counter["b"] == 1
    hash_first = repo.get_node_run(job.id, "b").node_hash

    # upstream now produces different bytes; force b back to READY and redeliver
    from sqlalchemy import update
    from stralis.db.models import NodeRun

    flag["v"] = "second"
    with repo._sf() as s, s.begin():
        s.execute(
            update(NodeRun)
            .where(NodeRun.job_id == job.id, NodeRun.node_id == "a")
            .values(status=NodeStatus.READY.value, attempt=0, queued_at=None, outputs={}, node_hash=None)
        )
        s.execute(
            update(NodeRun)
            .where(NodeRun.job_id == job.id, NodeRun.node_id == "b")
            .values(status=NodeStatus.READY.value, attempt=0, queued_at=None)
        )
    # the job had been finalized; reopen it so the orchestrator schedules again
    repo.set_job_status(job.id, JobStatus.RUNNING)

    worker = make_worker(repo, queue, store, config, handlers)
    orch = make_orchestrator(repo, queue, config)
    status = None
    for _ in range(20):
        orch.sweep_once()
        worker.run_until_empty()
        active = repo.list_active_jobs()
        if not active:
            status = repo.list_jobs()[0].status
            break

    assert status is JobStatus.SUCCEEDED
    assert counter["b"] == 2  # re-executed because the content hash changed
    assert repo.get_node_run(job.id, "b").node_hash != hash_first
