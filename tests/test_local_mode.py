"""Full-stack integration in "local mode" — no Redis, no MinIO, no Docker.

Runs the real production wiring: the FastAPI app (via TestClient), the
orchestrator, and a worker built from :mod:`stralis.runtime`, talking over a
SQLite task queue and a filesystem object store. This is the exact mode the
README documents for running Stralis on a single machine.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from stralis.api.app import create_app
from stralis.config import Config
from stralis.runtime import (
    build_orchestrator,
    build_queue,
    build_repo,
    build_store,
    build_worker,
)

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"
TERMINAL = {"succeeded", "failed", "cancelled"}


@pytest.fixture
def local_env(tmp_path, monkeypatch):
    monkeypatch.setenv("STRALIS_DATABASE_URL", f"sqlite:///{tmp_path / 'stralis.db'}")
    monkeypatch.setenv("STRALIS_QUEUE_BACKEND", "sqlite")
    monkeypatch.setenv("STRALIS_STORE_BACKEND", "local")
    monkeypatch.setenv("STRALIS_LOCAL_STORE_ROOT", str(tmp_path / "objects"))
    monkeypatch.setenv("STRALIS_WORK_ROOT", str(tmp_path / "work"))
    monkeypatch.setenv("STRALIS_HEARTBEAT_INTERVAL_S", "0.05")
    monkeypatch.setenv("STRALIS_HEARTBEAT_TIMEOUT_S", "5")
    monkeypatch.setenv("STRALIS_SWEEP_INTERVAL_S", "0.01")
    return Config.from_env()


def test_local_mode_runs_a_pipeline_end_to_end(local_env, tmp_path):
    config = local_env
    assert config.queue_backend == "sqlite" and config.store_backend == "local"

    repo = build_repo(config)
    app = create_app(repo=repo)
    spec = json.loads((EXAMPLES / "demo_dag.json").read_text(encoding="utf-8"))

    with TestClient(app) as client:
        # 1. API accepts instantly and hands back a job id (HTTP 202)
        resp = client.post("/api/v1/pipelines", json=spec)
        assert resp.status_code == 202
        job_id = resp.json()["job_id"]

        # 2. independent orchestrator + worker processes (separate objects,
        #    same database/queue/store files) drive it to completion
        orchestrator = build_orchestrator(config)
        worker = build_worker(config)
        for _ in range(50):
            orchestrator.sweep_once()
            worker.run_until_empty()
            body = client.get(f"/api/v1/jobs/{job_id}").json()
            if body["job"]["status"] in TERMINAL:
                break

        detail = client.get(f"/api/v1/jobs/{job_id}").json()
        assert detail["job"]["status"] == "succeeded", detail
        assert all(n["status"] == "succeeded" for n in detail["node_runs"])

        # 3. claim-check artifacts are listed and physically on disk
        artifacts = client.get(f"/api/v1/jobs/{job_id}/artifacts").json()
        assert len(artifacts) == 4
        assert all(a["uri"].startswith("local://") for a in artifacts)
        assert all(a["digest"] and a["size"] > 0 for a in artifacts)

    stored = [p for p in (tmp_path / "objects").rglob("*") if p.is_file()]
    assert len(stored) >= 4


def test_local_mode_failure_and_retry_via_api(local_env):
    """Job fails, then an API retry resumes it (checkpoints preserved)."""
    config = local_env
    repo = build_repo(config)
    app = create_app(repo=repo)

    spec = {
        "name": "flaky-api",
        "max_attempts": 1,
        "nodes": [{"id": "boom", "handler": "does.not.exist", "params": {}}],
    }
    with TestClient(app) as client:
        job_id = client.post("/api/v1/pipelines", json=spec).json()["job_id"]
        orchestrator = build_orchestrator(config)
        worker = build_worker(config)
        for _ in range(20):
            orchestrator.sweep_once()
            worker.run_until_empty()
            if client.get(f"/api/v1/jobs/{job_id}").json()["job"]["status"] in TERMINAL:
                break

        body = client.get(f"/api/v1/jobs/{job_id}").json()
        assert body["job"]["status"] == "failed"
        assert "does.not.exist" in body["node_runs"][0]["error"]

        # operator retry: job is re-opened and the orchestrator picks it up
        resp = client.post(f"/api/v1/jobs/{job_id}/retry")
        assert resp.status_code == 202
        assert resp.json()["status"] == "running"

        # still fails (handler is genuinely missing) but the cycle works
        for _ in range(20):
            orchestrator.sweep_once()
            worker.run_until_empty()
            if client.get(f"/api/v1/jobs/{job_id}").json()["job"]["status"] in TERMINAL:
                break
        assert client.get(f"/api/v1/jobs/{job_id}").json()["job"]["status"] == "failed"


def test_invalid_backend_config_is_rejected(local_env, monkeypatch):
    from stralis.errors import ConfigError
    from stralis.runtime import build_queue, build_store

    monkeypatch.setenv("STRALIS_QUEUE_BACKEND", "carrier-pigeon")
    with pytest.raises(ConfigError):
        build_queue(Config.from_env())

    monkeypatch.setenv("STRALIS_QUEUE_BACKEND", "sqlite")
    monkeypatch.setenv("STRALIS_STORE_BACKEND", "nope")
    with pytest.raises(ConfigError):
        build_store(Config.from_env())
