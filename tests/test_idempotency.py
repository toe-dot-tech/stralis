"""Idempotency: content-hash checkpoints must eliminate duplicate work."""

from __future__ import annotations

from sqlalchemy import update

from stralis.db.models import NodeRun
from stralis.models import JobStatus, NodeStatus, PipelineSpec, TaskMessage, compute_node_hash

from tests.conftest import counter_handler, drive, make_worker


def test_node_hash_is_stable_and_sensitive():
    a = compute_node_hash("shell", {"command": ["x"], "n": 1}, ["d1", "d2"])
    b = compute_node_hash("shell", {"n": 1, "command": ["x"]}, ["d2", "d1"])
    assert a == b  # dict order and input order are irrelevant

    assert a != compute_node_hash("shell", {"command": ["y"], "n": 1}, ["d1", "d2"])
    assert a != compute_node_hash("ffmpeg.transcode", {"command": ["x"], "n": 1}, ["d1", "d2"])
    assert a != compute_node_hash("shell", {"command": ["x"], "n": 1}, ["d1", "d3"])


def test_checkpoint_hit_skips_reexecution(repo, queue, store, config):
    """If outputs already exist for the same node hash, the worker must not re-run it."""
    job = repo.create_job(
        PipelineSpec(name="cached", nodes=[{"id": "a", "handler": "count", "params": {}}])
    )
    counter: dict[str, int] = {}
    worker = make_worker(repo, queue, store, config, {"count": counter_handler(counter, "a")})

    queue.put(TaskMessage(job_id=job.id, node_id="a", attempt=0))
    worker.run_until_empty()
    assert counter["a"] == 1

    # Simulate the failure mode this protects against: the node is rescheduled
    # (e.g. a duplicate delivery that wins a race) while its checkpoint from the
    # identical execution is still valid.
    with repo._sf() as s, s.begin():
        s.execute(
            update(NodeRun)
            .where(NodeRun.job_id == job.id, NodeRun.node_id == "a")
            .values(status=NodeStatus.READY.value, attempt=0, queued_at=None)
        )

    queue.put(TaskMessage(job_id=job.id, node_id="a", attempt=0))
    handled = worker.run_until_empty()
    assert handled == 1
    assert counter["a"] == 1  # execution skipped, checkpoint reused
    assert repo.get_node_run(job.id, "a").status is NodeStatus.SUCCEEDED


def test_output_uri_is_deterministic_for_same_hash(repo, queue, store, config):
    """Deterministic object keys make re-uploads idempotent (same bytes, same key)."""
    job = repo.create_job(
        PipelineSpec(
            name="det",
            nodes=[{"id": "a", "handler": "noop", "params": {"outputs": ["x"], "content": "c"}}],
        )
    )
    from stralis.worker.handlers import default_handlers

    status = drive(repo, queue, store, config, default_handlers())
    assert status is JobStatus.SUCCEEDED
    run = repo.get_node_run(job.id, "a")
    assert run.node_hash is not None
    uri = run.outputs["x"].uri
    assert run.node_hash[:16] in uri
    assert uri.startswith(f"mem://test/{job.id}/a/")
