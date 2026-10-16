"""Component wiring: build fully-configured engine parts from a :class:`Config`.

These factories are the single place where the concrete adapters (Postgres/
SQLite, Redis Streams, S3/MinIO) are chosen. Tests construct the parts directly
with in-memory doubles instead.
"""

from __future__ import annotations

from stralis.config import Config
from stralis.db.engine import init_db, make_engine, make_session_factory
from stralis.db.repo import Repo
from stralis.errors import ConfigError
from stralis.orchestrator import Orchestrator
from stralis.ports.queue import TaskQueue
from stralis.ports.storage import ObjectStore
from stralis.queue import RedisTaskQueue
from stralis.queue.sqlite_queue import SQLiteTaskQueue
from stralis.storage import S3ObjectStore
from stralis.storage.local import LocalObjectStore
from stralis.worker import Worker
from stralis.worker.handlers import Handler


def build_repo(config: Config | None = None) -> Repo:
    cfg = config or Config.from_env()
    engine = make_engine(cfg.database_url)
    init_db(engine)
    return Repo(make_session_factory(engine))


def build_queue(config: Config | None = None) -> TaskQueue:
    cfg = config or Config.from_env()
    if cfg.queue_backend == "redis":
        return RedisTaskQueue(cfg.redis_url, group=cfg.queue_group)
    if cfg.queue_backend == "sqlite":
        return SQLiteTaskQueue(cfg.database_url, group=cfg.queue_group)
    raise ConfigError(
        f"unknown STRALIS_QUEUE_BACKEND {cfg.queue_backend!r} (use redis|sqlite)"
    )


def build_store(config: Config | None = None) -> ObjectStore:
    cfg = config or Config.from_env()
    if cfg.store_backend == "local":
        return LocalObjectStore(cfg.local_store_root)
    if cfg.store_backend == "s3":
        return S3ObjectStore(
            cfg.s3_endpoint_url,
            bucket=cfg.s3_bucket,
            access_key=cfg.s3_access_key,
            secret_key=cfg.s3_secret_key,
            region=cfg.s3_region,
        )
    raise ConfigError(
        f"unknown STRALIS_STORE_BACKEND {cfg.store_backend!r} (use s3|local)"
    )


def build_worker(
    config: Config | None = None, *, handlers: dict[str, Handler] | None = None
) -> Worker:
    cfg = config or Config.from_env()
    return Worker(
        build_repo(cfg),
        build_queue(cfg),
        build_store(cfg),
        cfg,
        handlers=handlers,
    )


def build_orchestrator(config: Config | None = None) -> Orchestrator:
    cfg = config or Config.from_env()
    return Orchestrator(build_repo(cfg), build_queue(cfg), cfg)


__all__ = [
    "build_repo",
    "build_queue",
    "build_store",
    "build_worker",
    "build_orchestrator",
]
