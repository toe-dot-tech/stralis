"""Repository invariants: DAG init, attempt fencing, retries, finalization."""

from __future__ import annotations

from datetime import timedelta

from stralis.models import (
    ArtifactRef,
    JobStatus,
    NodeStatus,
    PipelineSpec,
    TaskMessage,
    utcnow,
)
from stralis.db.repo import Repo


def diamond() -> PipelineSpec:
    return PipelineSpec(
        name="diamond",
        nodes=[
            {"id": "a", "handler": "noop", "params": {"outputs": ["t"]}},
            {"id": "b", "handler": "noop", "params": {"outputs": ["b"]}, "depends_on": ["a"]},
            {"id": "c", "handler": "noop", "params": {"outputs": ["c"]}, "depends_on": ["a"]},
            {"id": "d", "handler": "noop", "params": {"outputs": ["d"]}, "depends_on": ["b", "c"]},
        ],
    )


def ref(name: str) -> ArtifactRef:
    return ArtifactRef(name=name, uri=f"mem://test/{name}", digest="0" * 64, size=1)


def statuses(repo: Repo, job_id: str) -> dict[str, NodeStatus]:
    return {r.node_id: r.status for r in repo.get_node_runs(job_id)}


def test_create_job_roots_ready_others_pending(repo: Repo):
    job = repo.create_job(diamond())
    assert job.status is JobStatus.PENDING
    st = statuses(repo, job.id)
    assert st == {
        "a": NodeStatus.READY,
        "b": NodeStatus.PENDING,
        "c": NodeStatus.PENDING,
        "d": NodeStatus.PENDING,
    }


def test_promote_ready_only_after_all_parents_succeed(repo: Repo):
    job = repo.create_job(diamond())
    assert repo.promote_ready_nodes(job.id) == []  # a is already READY, nothing pending is eligible

    # a succeeds -> b and c become READY, d stays PENDING
    assert repo.claim_node(job.id, "a", 0) is not None
    assert repo.complete_node(job.id, "a", 0, "h", {"t": ref("t")})
    promoted = {r.node_id for r in repo.promote_ready_nodes(job.id)}
    assert promoted == {"b", "c"}
    assert statuses(repo, job.id)["d"] is NodeStatus.PENDING

    # only b succeeds -> d still PENDING (c outstanding)
    repo.claim_node(job.id, "b", 0)
    repo.complete_node(job.id, "b", 0, "h", {"b": ref("b")})
    assert repo.promote_ready_nodes(job.id) == []

    repo.claim_node(job.id, "c", 0)
    repo.complete_node(job.id, "c", 0, "h", {"c": ref("c")})
    assert {r.node_id for r in repo.promote_ready_nodes(job.id)} == {"d"}


def test_claim_is_fenced_by_attempt(repo: Repo):
    job = repo.create_job(diamond())
    assert repo.claim_node(job.id, "a", 5) is None      # wrong attempt
    first = repo.claim_node(job.id, "a", 0)
    assert first is not None and first.status is NodeStatus.RUNNING
    assert repo.claim_node(job.id, "a", 0) is None      # double claim
    # heartbeat only valid for the owning attempt
    assert repo.heartbeat(job.id, "a", 0) is True
    assert repo.heartbeat(job.id, "a", 7) is False


def test_complete_and_fail_are_fenced(repo: Repo):
    job = repo.create_job(diamond())
    repo.claim_node(job.id, "a", 0)
    assert repo.complete_node(job.id, "a", 3, "h", {}) is False   # stale attempt
    assert repo.fail_node(job.id, "a", 9, "nope") is None
    assert repo.complete_node(job.id, "a", 0, "h", {"t": ref("t")}) is True
    # already terminal: further transitions are fenced
    assert repo.fail_node(job.id, "a", 0, "late") is None
    assert repo.complete_node(job.id, "a", 0, "h", {}) is False


def test_fail_node_requeues_then_fails_at_budget(repo: Repo):
    spec = PipelineSpec(
        name="retry",
        max_attempts=2,
        nodes=[{"id": "a", "handler": "noop", "params": {"outputs": ["x"]}}],
    )
    job = repo.create_job(spec)

    repo.claim_node(job.id, "a", 0)
    run = repo.fail_node(job.id, "a", 0, "boom")
    assert run is not None
    assert run.status is NodeStatus.READY
    assert run.attempt == 1
    assert run.queued_at is None
    assert run.error == "boom"

    repo.claim_node(job.id, "a", 1)
    run = repo.fail_node(job.id, "a", 1, "boom again")
    assert run is not None
    assert run.status is NodeStatus.FAILED  # budget exhausted (2 attempts)


def test_blocked_nodes_cascade_to_skipped(repo: Repo):
    spec = PipelineSpec(
        name="chain",
        max_attempts=1,
        nodes=[
            {"id": "a", "handler": "noop", "params": {"outputs": ["a"]}},
            {"id": "b", "handler": "noop", "params": {"outputs": ["b"]}, "depends_on": ["a"]},
            {"id": "c", "handler": "noop", "params": {"outputs": ["c"]}, "depends_on": ["b"]},
        ],
    )
    job = repo.create_job(spec)
    repo.claim_node(job.id, "a", 0)
    repo.fail_node(job.id, "a", 0, "dead")

    skipped_1 = {r.node_id for r in repo.promote_blocked_nodes(job.id)}
    assert skipped_1 == {"b"}
    skipped_2 = {r.node_id for r in repo.promote_blocked_nodes(job.id)}
    assert skipped_2 == {"c"}  # cascade: b is now SKIPPED -> c blocked


def test_finalize_job(repo: Repo):
    job = repo.create_job(diamond())
    assert repo.finalize_job_if_done(job.id) is None  # not done yet
    # respect the scheduler: only READY nodes may be claimed, so promote as we go
    for nid, art in [("a", "t"), ("b", "b"), ("c", "c"), ("d", "d")]:
        repo.promote_ready_nodes(job.id)
        assert repo.claim_node(job.id, nid, 0) is not None
        assert repo.complete_node(job.id, nid, 0, "h", {art: ref(art)}) is True
    done = repo.finalize_job_if_done(job.id)
    assert done is not None and done.status is JobStatus.SUCCEEDED


def test_finalize_records_failure(repo: Repo):
    spec = PipelineSpec(
        name="failjob",
        max_attempts=1,
        nodes=[
            {"id": "a", "handler": "noop", "params": {"outputs": ["a"]}},
            {"id": "b", "handler": "noop", "params": {"outputs": ["b"]}, "depends_on": ["a"]},
        ],
    )
    job = repo.create_job(spec)
    repo.claim_node(job.id, "a", 0)
    repo.fail_node(job.id, "a", 0, "exploded")
    repo.promote_blocked_nodes(job.id)
    done = repo.finalize_job_if_done(job.id)
    assert done is not None and done.status is JobStatus.FAILED
    assert done.error is not None and "exploded" in done.error


def test_retry_keeps_checkpoints_and_resets_failures(repo: Repo):
    spec = PipelineSpec(
        name="resume",
        max_attempts=1,
        nodes=[
            {"id": "a", "handler": "noop", "params": {"outputs": ["a"]}},
            {"id": "b", "handler": "noop", "params": {"outputs": ["b"]}, "depends_on": ["a"]},
        ],
    )
    job = repo.create_job(spec)
    repo.claim_node(job.id, "a", 0)
    repo.complete_node(job.id, "a", 0, "hash-a", {"a": ref("a")})
    repo.promote_ready_nodes(job.id)
    repo.claim_node(job.id, "b", 0)
    repo.fail_node(job.id, "b", 0, "kaput")
    repo.finalize_job_if_done(job.id)
    assert repo.get_job(job.id).status is JobStatus.FAILED

    retried = repo.retry_job(job.id)
    assert retried is not None and retried.status is JobStatus.RUNNING

    st = statuses(repo, job.id)
    assert st["a"] is NodeStatus.SUCCEEDED       # checkpoint preserved
    assert st["b"] is NodeStatus.PENDING         # re-execution scheduled
    runs = {r.node_id: r for r in repo.get_node_runs(job.id)}
    assert runs["a"].outputs and runs["a"].node_hash == "hash-a"
    assert runs["b"].attempt == 0 and runs["b"].error is None


def test_retry_rejected_unless_failed(repo: Repo):
    job = repo.create_job(diamond())
    assert repo.retry_job(job.id) is None


def test_due_ready_nodes_tracks_queue_freshness(repo: Repo):
    job = repo.create_job(diamond())
    due = repo.due_ready_nodes(stale_after_s=30.0)
    assert [r.node_id for r in due] == ["a"]      # never queued

    repo.mark_queued(job.id, "a")
    assert repo.due_ready_nodes(stale_after_s=30.0) == []   # fresh

    stale = repo.due_ready_nodes(stale_after_s=0.0)
    assert [r.node_id for r in stale] == ["a"]    # presumed lost -> re-enqueue


def test_stale_running_and_recovery(repo: Repo):
    job = repo.create_job(diamond())
    repo.claim_node(job.id, "a", 0)
    fresh = repo.stale_running(utcnow() - timedelta(seconds=60))
    assert fresh == []  # heartbeat is fresh

    cutoff = utcnow() + timedelta(seconds=1)  # everything looks stale past this
    stale = repo.stale_running(cutoff)
    assert [r.node_id for r in stale] == ["a"]

    recovered = repo.recover_stale(job.id, "a", 0, cutoff, "heartbeat timeout")
    assert recovered is not None
    assert recovered.status is NodeStatus.READY and recovered.attempt == 1

    # a worker that just refreshed its heartbeat is NOT stolen from
    repo.claim_node(job.id, "a", 1)
    repo.heartbeat(job.id, "a", 1)
    assert repo.recover_stale(job.id, "a", 1, utcnow() - timedelta(seconds=60), "x") is None


def test_artifacts_recorded_on_completion(repo: Repo):
    job = repo.create_job(diamond())
    repo.claim_node(job.id, "a", 0)
    repo.complete_node(job.id, "a", 0, "h", {"t": ref("t")})
    arts = repo.list_artifacts(job.id)
    assert len(arts) == 1
    assert arts[0].node_id == "a" and arts[0].name == "t"
