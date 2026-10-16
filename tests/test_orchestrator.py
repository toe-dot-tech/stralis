"""Orchestrator: scheduling, lost-message recovery, heartbeat-based crash recovery."""

from __future__ import annotations

from datetime import timedelta

from stralis.models import JobStatus, NodeStatus, PipelineSpec
from stralis.orchestrator import Orchestrator

from tests.conftest import counter_handler, drive, make_orchestrator, make_worker


def diamond_spec() -> PipelineSpec:
    return PipelineSpec(
        name="d",
        nodes=[
            {"id": "a", "handler": "noop", "params": {"outputs": ["a"]}},
            {"id": "b", "handler": "noop", "params": {"outputs": ["b"]}, "depends_on": ["a"]},
            {"id": "c", "handler": "noop", "params": {"outputs": ["c"]}, "depends_on": ["b"]},
        ],
    )


def test_sweep_enqueues_roots_and_marks_job_running(repo, queue, config):
    job = repo.create_job(diamond_spec())
    # production latency: a fresh enqueue is not considered lost for 30s
    orch = Orchestrator(repo, queue, config, enqueue_stale_after_s=30.0)

    stats = orch.sweep_once()
    assert stats["enqueued"] == 1
    assert repo.get_job(job.id).status is JobStatus.RUNNING

    msgs = queue.get("probe", count=10)
    assert [m.message.node_id for m in msgs] == ["a"]
    assert msgs[0].message.job_id == job.id
    # marked as queued -> a second sweep does not duplicate the message
    stats2 = orch.sweep_once()
    assert stats2["enqueued"] == 0


def test_lost_message_is_reenqueued(repo, queue, config):
    job = repo.create_job(diamond_spec())
    orch = make_orchestrator(repo, queue, config)  # enqueue_stale_after_s=0.0
    orch.sweep_once()
    # simulate the delivery being lost (e.g. redis flush right after XADD)
    orch.sweep_once()
    orch.sweep_once()
    msgs = queue.get("probe", count=10)
    assert [m.message.node_id for m in msgs] == ["a", "a", "a"]
    # duplicate deliveries are harmless: claims are fenced (see worker tests)


def test_dependents_enqueued_only_after_parent_success(repo, queue, store, config):
    job = repo.create_job(diamond_spec())
    orch = make_orchestrator(repo, queue, config)
    counter: dict[str, int] = {}
    worker = make_worker(repo, queue, store, config, {"noop": counter_handler(counter, "x")})

    for _ in range(12):
        orch.sweep_once()
        worker.run_until_empty()

    assert repo.get_job(job.id).status is JobStatus.SUCCEEDED
    st = {r.node_id: r.status for r in repo.get_node_runs(job.id)}
    assert st == {"a": NodeStatus.SUCCEEDED, "b": NodeStatus.SUCCEEDED, "c": NodeStatus.SUCCEEDED}


def test_crashed_worker_is_recovered_by_heartbeat_sweep(repo, queue, store, config):
    """Claim a node, kill the heartbeat, then let the orchestrator requeue it."""
    job = repo.create_job(diamond_spec())
    # simulate a worker that claimed the node and then died
    repo.claim_node(job.id, "a", 0)
    assert repo.get_node_run(job.id, "a").status is NodeStatus.RUNNING

    cutoff = __import__("stralis.models", fromlist=["utcnow"]).utcnow() + timedelta(seconds=1)
    # force staleness deterministically
    from stralis.db.models import NodeRun
    from sqlalchemy import update

    with repo._sf() as s, s.begin():
        s.execute(update(NodeRun).where(NodeRun.job_id == job.id).values(heartbeat_at=cutoff - timedelta(seconds=120)))

    orch = make_orchestrator(repo, queue, config)
    stats = orch.sweep_once()
    assert stats["recovered"] == 1

    run = repo.get_node_run(job.id, "a")
    assert run.status is NodeStatus.READY and run.attempt == 1

    # and the pipeline finishes normally afterwards
    counter: dict[str, int] = {}
    worker = make_worker(repo, queue, store, config, {"noop": counter_handler(counter, "x")})
    for _ in range(12):
        orch.sweep_once()
        worker.run_until_empty()
    assert repo.get_job(job.id).status is JobStatus.SUCCEEDED
    assert repo.get_node_run(job.id, "a").attempt == 1


def test_stale_recovery_exhausts_budget(repo, queue, config):
    spec = PipelineSpec(
        name="once",
        max_attempts=1,
        nodes=[{"id": "a", "handler": "noop", "params": {"outputs": ["a"]}}],
    )
    job = repo.create_job(spec)
    repo.claim_node(job.id, "a", 0)

    from sqlalchemy import update
    from stralis.db.models import NodeRun

    with repo._sf() as s, s.begin():
        s.execute(update(NodeRun).where(NodeRun.job_id == job.id).values(heartbeat_at=None))

    orch = make_orchestrator(repo, queue, config)
    orch.sweep_once()
    run = repo.get_node_run(job.id, "a")
    assert run.status is NodeStatus.FAILED
    assert "heartbeat" in run.error

    orch.sweep_once()
    assert repo.get_job(job.id).status is JobStatus.FAILED


def test_blocked_nodes_skip_and_job_fails(repo, queue, store, config):
    spec = PipelineSpec(
        name="blocked",
        max_attempts=1,
        nodes=[
            {"id": "a", "handler": "boom", "params": {}},
            {"id": "b", "handler": "noop", "params": {"outputs": ["b"]}, "depends_on": ["a"]},
        ],
    )
    job = repo.create_job(spec)

    def boom(ctx):
        raise RuntimeError("cannot proceed")

    status = drive(repo, queue, store, config, {"boom": boom})
    assert status is JobStatus.FAILED
    st = {r.node_id: r.status for r in repo.get_node_runs(job.id)}
    assert st["a"] is NodeStatus.FAILED
    assert st["b"] is NodeStatus.SKIPPED


def test_run_forever_survives_sweep_errors(repo, queue, config, monkeypatch):
    """A transient error in one sweep must not kill the supervisor loop."""
    repo.create_job(diamond_spec())
    orch = make_orchestrator(repo, queue, config)

    calls = {"n": 0}
    real = Orchestrator.sweep_once

    def flaky(self):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient db blip")
        self.stop()  # end the loop on the next successful sweep
        return real(self)

    monkeypatch.setattr(Orchestrator, "sweep_once", flaky)
    monkeypatch.setattr("time.sleep", lambda *_: None)
    orch.run_forever()
    assert calls["n"] >= 2
