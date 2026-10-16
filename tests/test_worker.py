"""Worker runtime: execution, claim-check IO, failures, fencing."""

from __future__ import annotations

import time

from stralis.models import JobStatus, NodeStatus, PipelineSpec, TaskMessage
from stralis.worker.handlers import NodeContext

from tests.conftest import (
    counter_handler,
    drive,
    fail_n_times,
    make_worker,
    single_node_spec,
)


def test_executes_node_and_uploads_artifact(repo, queue, store, config):
    job = repo.create_job(single_node_spec())
    counter: dict[str, int] = {}
    status = drive(repo, queue, store, config, {"noop": counter_handler(counter, "a")})
    assert status is JobStatus.SUCCEEDED
    assert counter["a"] == 1

    run = repo.get_node_run(job.id, "a")
    assert run.status is NodeStatus.SUCCEEDED
    assert run.node_hash is not None
    ref = run.outputs["out"]
    assert store.read_bytes(ref.uri) == b"a:1"
    # artifact is also queryable through the claim-check ledger
    assert [a.uri for a in repo.list_artifacts(job.id)] == [ref.uri]


def test_duplicate_delivery_is_ignored(repo, queue, store, config):
    """A second delivery of a finished task must not re-run the handler."""
    job = repo.create_job(single_node_spec())
    counter: dict[str, int] = {}
    worker = make_worker(repo, queue, store, config, {"noop": counter_handler(counter, "a")})

    queue.put(TaskMessage(job_id=job.id, node_id="a", attempt=0))
    assert worker.run_until_empty() == 1
    assert counter["a"] == 1

    # queue redelivers the same task (at-least-once)
    queue.put(TaskMessage(job_id=job.id, node_id="a", attempt=0))
    worker.run_until_empty()
    assert counter["a"] == 1  # still once: claim was fenced
    assert repo.get_node_run(job.id, "a").status is NodeStatus.SUCCEEDED


def test_failure_retries_then_succeeds(repo, queue, store, config):
    spec = PipelineSpec(
        name="flaky",
        max_attempts=3,
        nodes=[{"id": "a", "handler": "flaky", "params": {}}],
    )
    job = repo.create_job(spec)
    counter: dict[str, int] = {}
    status = drive(repo, queue, store, config, {"flaky": fail_n_times(counter, 2)})
    assert status is JobStatus.SUCCEEDED
    assert counter["flaky"] == 3  # two failures, third attempt won
    run = repo.get_node_run(job.id, "a")
    assert run.attempt == 2 and run.status is NodeStatus.SUCCEEDED


def test_exhausted_retries_fail_the_job(repo, queue, store, config):
    spec = PipelineSpec(
        name="doomed",
        max_attempts=2,
        nodes=[{"id": "a", "handler": "flaky", "params": {}}],
    )
    job = repo.create_job(spec)
    counter: dict[str, int] = {}
    status = drive(repo, queue, store, config, {"flaky": fail_n_times(counter, 99)})
    assert status is JobStatus.FAILED
    assert counter["flaky"] == 2
    run = repo.get_node_run(job.id, "a")
    assert run.status is NodeStatus.FAILED
    assert "RuntimeError" in run.error and "boom" in run.error


def test_unknown_handler_fails_with_clear_error(repo, queue, store, config):
    spec = PipelineSpec(
        name="nohandler",
        max_attempts=1,
        nodes=[{"id": "a", "handler": "does.not.exist", "params": {}}],
    )
    job = repo.create_job(spec)
    status = drive(repo, queue, store, config, {})
    assert status is JobStatus.FAILED
    run = repo.get_node_run(job.id, "a")
    assert "does.not.exist" in run.error


def test_claim_check_roundtrip_between_nodes(repo, queue, store, config):
    """Upstream output is downloaded as a local file for the downstream node."""
    spec = PipelineSpec(
        name="pipeline",
        nodes=[
            {"id": "up", "handler": "make", "params": {}},
            {"id": "down", "handler": "consume", "params": {}, "depends_on": ["up"]},
        ],
    )
    job = repo.create_job(spec)

    seen: dict[str, str] = {}

    def make(ctx: NodeContext):
        p = ctx.output_path("payload")
        p.write_text("hello-from-upstream", encoding="utf-8")
        return {"payload": p}

    def consume(ctx: NodeContext):
        seen["data"] = ctx.require_input("payload").read_text(encoding="utf-8")
        p = ctx.output_path("echo")
        p.write_text(seen["data"].upper(), encoding="utf-8")
        return {"echo": p}

    status = drive(repo, queue, store, config, {"make": make, "consume": consume})
    assert status is JobStatus.SUCCEEDED
    assert seen["data"] == "hello-from-upstream"
    down = repo.get_node_run(job.id, "down")
    assert store.read_bytes(down.outputs["echo"].uri) == b"HELLO-FROM-UPSTREAM"


def test_handler_failure_message_is_preserved(repo, queue, store, config):
    from stralis.errors import HandlerExecutionError

    spec = PipelineSpec(
        name="handerr",
        max_attempts=1,
        nodes=[{"id": "a", "handler": "boom", "params": {}}],
    )
    job = repo.create_job(spec)

    def boom(ctx: NodeContext):
        raise HandlerExecutionError("ffmpeg exited 1: bad codec")

    status = drive(repo, queue, store, config, {"boom": boom})
    assert status is JobStatus.FAILED
    assert repo.get_node_run(job.id, "a").error == "ffmpeg exited 1: bad codec"


def test_heartbeat_keeps_node_alive_during_long_execution(repo, queue, store, config, monkeypatch):
    spec = PipelineSpec(
        name="slow",
        max_attempts=1,
        nodes=[{"id": "a", "handler": "slow", "params": {}}],
    )
    job = repo.create_job(spec)

    def slow(ctx: NodeContext):
        time.sleep(0.4)
        p = ctx.output_path("out")
        p.write_text("done", encoding="utf-8")
        return {"out": p}

    worker = make_worker(repo, queue, store, config, {"slow": slow})
    queue.put(TaskMessage(job_id=job.id, node_id="a", attempt=0))

    beats: list[float] = []
    real_heartbeat = repo.heartbeat

    def spy(job_id, node_id, attempt):
        beats.append(time.monotonic())
        return real_heartbeat(job_id, node_id, attempt)

    monkeypatch.setattr(repo, "heartbeat", spy)
    worker.run_until_empty()

    assert repo.get_node_run(job.id, "a").status is NodeStatus.SUCCEEDED
    # interval is 0.05s over a 0.4s handler -> several beats must have landed
    assert len(beats) >= 2


def test_worker_acks_even_when_job_missing(repo, queue, store, config):
    worker = make_worker(repo, queue, store, config, {})
    queue.put(TaskMessage(job_id="ghost", node_id="a", attempt=0))
    worker.run_until_empty()  # must not raise
    # the delivery was acked (reclaim only surfaces unacked messages)
    assert queue.reclaim("other", min_idle_ms=0, count=10) == []


def test_noop_handler_writes_deterministic_content(repo, queue, store, config):
    from stralis.worker.handlers import default_handlers

    spec = PipelineSpec(
        name="noop-default",
        nodes=[{"id": "a", "handler": "noop", "params": {"outputs": ["x"], "content": "fixed"}}],
    )
    job = repo.create_job(spec)
    status = drive(repo, queue, store, config, default_handlers())
    assert status is JobStatus.SUCCEEDED
    ref = repo.get_node_run(job.id, "a").outputs["x"]
    assert store.read_bytes(ref.uri) == b"fixed"
