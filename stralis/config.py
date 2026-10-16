"""Environment-driven configuration."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass, fields


@dataclass
class Config:
    # Persistence
    database_url: str = "sqlite:///./stralis.db"

    # Queue / object store backends: "redis"|"sqlite" and "s3"|"local".
    # The sqlite/local pair is a self-contained, no-services mode.
    queue_backend: str = "redis"
    store_backend: str = "s3"
    local_store_root: str = "./.stralis-store"

    # Queue (Redis Streams)
    redis_url: str = "redis://localhost:6379/0"
    queue_group: str = "stralis-workers"

    # Object store (S3-compatible, e.g. MinIO)
    s3_endpoint_url: str | None = None
    s3_bucket: str = "stralis"
    s3_access_key: str = "stralis"
    s3_secret_key: str = "stralis"
    s3_region: str = "us-east-1"

    # Execution / resilience
    heartbeat_interval_s: float = 5.0
    heartbeat_timeout_s: float = 60.0
    sweep_interval_s: float = 1.0
    max_attempts: int = 3

    # Worker scratch space (claim-check downloads land here)
    work_root: str = "./.stralis-work"

    # Misc
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Config":
        cfg = cls()
        for f in fields(cls):
            env_key = f"STRALIS_{f.name.upper()}"
            if env_key in os.environ:
                raw = os.environ[env_key]
                if f.type in ("float",):
                    setattr(cfg, f.name, float(raw))
                elif f.type in ("int",):
                    setattr(cfg, f.name, int(raw))
                else:
                    setattr(cfg, f.name, raw)
        # numeric coercion that survives `from __future__ import annotations`
        cfg.heartbeat_interval_s = float(cfg.heartbeat_interval_s)
        cfg.heartbeat_timeout_s = float(cfg.heartbeat_timeout_s)
        cfg.sweep_interval_s = float(cfg.sweep_interval_s)
        cfg.max_attempts = int(cfg.max_attempts)
        return cfg


def default_consumer_name() -> str:
    """Stable-per-process consumer identity for Redis consumer groups."""
    return f"{socket.gethostname()}-{os.getpid()}"
