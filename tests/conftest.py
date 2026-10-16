"""Shared fixtures: real Repo on a tmp SQLite DB + in-memory queue/store."""

from __future__ import annotations

from pathlib import Path

import pytest

from stralis.config import Config
from stralis.db.engine import init_db, make_engine, make_session_factory
from stralis.db.repo import Repo
from stralis.models import JobStatus, PipelineSpec
from stralis.orchestrator import Orchestrator
from stralis.testing import InMemoryObjectStore, InMemoryQueue
from stralis.worker import Worker
from stralis.worker.handlers import Handler, NodeContext


@pytest.fixture
def config(tmp_path: Path) -> Config:
    return Config(
        database_url=f"sqlite:///{tmp_path / 'stralis-test.db'}",
        work_root=str(tmp_path / "work"),
        heartbeat_interval_s=0.05,
        heartbeat_timeout_s=1.0,
        sweep_interval_s=0.01,
        max_attempts=3,
    )


@pytest.fixture
def engine(config: Config):
    eng = make_engine(config.database_url)
    init_db(eng)
    return eng


@pytest.fixture
def repo(engine) -> Repo:
    return Repo(make_session_factory(engine))


@pytest.fixture
def queue() -> InMemoryQueue:
    return InMemoryQueue()


@pytest.fixture
def store() -> InMemoryObjectStore:
    return InMemoryObjectStore(bucket="test")


@pytest.fixture
def handlers() -> dict[str, Handler]:
    return {}


def make_worker(
    repo: Repo,
    queue: InMemoryQueue,
    store: InMemoryObjectStore,
    config: Config,
    handlers: dict[str, Handler] | None = None,
) -> Worker:
    return Worker(repo, queue, store, config, handlers=handlers or {}, consumer="test-worker")


def make_orchestrator(repo: Repo, queue: InMemoryQueue, config: Config) -> Orchestrator:
    return Orchestrator(repo, queue, config, enqueue_stale_after_s=0.0)


def drive(
    repo: Repo,
    queue: InMemoryQueue,
    store: InMemoryObjectStore,
    config: Config,
    handlers: dict[str, Handler] | None = None,
    *,
    max_iters: int = 100,
) -> JobStatus:
    """Alternate orchestrator sweeps and worker drains until the job is terminal."""
    orch = make_orchestrator(repo, queue, config)
    worker = make_worker(repo, queue, store, config, handlers)
    status = JobStatus.PENDING
    for _ in range(max_iters):
        orch.sweep_once()
        worker.run_until_empty()
        jobs = repo.list_active_jobs()
        if not jobs:
            break
        status = jobs[0].status
    jobs = repo.list_jobs()
    return jobs[0].status if jobs else status


# ---------------------------------------------------------------------------
# Handler helpers used across tests
# ---------------------------------------------------------------------------


def counter_handler(counter: dict[str, int], name: str):
    """A noop-style handler that records each execution."""

    def handler(ctx: NodeContext):
        counter[name] = counter.get(name, 0) + 1
        outs = ctx.params.get("outputs", [name])
        if isinstance(outs, str):
            outs = [outs]
        written = {}
        for out in outs:
            p = ctx.output_path(out)
            p.write_text(f"{name}:{counter[name]}", encoding="utf-8")
            written[out] = p
        return written

    return handler


def fail_n_times(counter: dict[str, int], fail_times: int, name: str = "flaky"):
    """Succeeds on attempt ``fail_times + 1``; counts every execution."""

    def handler(ctx: NodeContext):
        counter[name] = counter.get(name, 0) + 1
        if counter[name] <= fail_times:
            raise RuntimeError(f"boom {counter[name]}")
        p = ctx.output_path("out")
        p.write_text("ok", encoding="utf-8")
        return {"out": p}

    return handler


def single_node_spec(handler: str = "noop", *, outputs: list[str] | None = None) -> PipelineSpec:
    params = {"outputs": outputs or ["out"]}
    return PipelineSpec(
        name="single",
        nodes=[{"id": "a", "handler": handler, "params": params}],
    )
