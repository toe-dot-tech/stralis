"""HTTP API tests for the Stralis FastAPI app."""

from __future__ import annotations

from fastapi.testclient import TestClient

from stralis.api.app import create_app
from stralis.db.engine import init_db, make_engine, make_session_factory
from stralis.db.repo import Repo


def _app(tmp_path) -> TestClient:
    engine = make_engine(f"sqlite:///{tmp_path}/api.db")
    init_db(engine)
    repo = Repo(make_session_factory(engine))
    return TestClient(create_app(repo=repo))


DIAMOND: dict = {
    "name": "diamond",
    "nodes": [
        {"id": "a", "handler": "h"},
        {"id": "b", "handler": "h", "depends_on": ["a"]},
        {"id": "c", "handler": "h", "depends_on": ["a"]},
        {"id": "d", "handler": "h", "depends_on": ["b", "c"]},
    ],
}


def test_healthz(tmp_path):
    with _app(tmp_path) as c:
        r = c.get("/healthz")
        assert r.status_code == 200
        assert r.json() == {"status": "ok"}


def test_submit_valid_pipeline(tmp_path):
    with _app(tmp_path) as c:
        r = c.post("/api/v1/pipelines", json=DIAMOND)
        assert r.status_code == 202
        body = r.json()
        assert body["name"] == "diamond"
        assert body["status"] == "pending"
        assert body["job_id"]


def test_job_node_runs_root_ready(tmp_path):
    with _app(tmp_path) as c:
        job_id = c.post("/api/v1/pipelines", json=DIAMOND).json()["job_id"]
        r = c.get(f"/api/v1/jobs/{job_id}")
        assert r.status_code == 200
        body = r.json()
        assert body["job"]["id"] == job_id
        runs = {n["node_id"]: n["status"] for n in body["node_runs"]}
        assert runs == {"a": "ready", "b": "pending", "c": "pending", "d": "pending"}


def test_list_jobs(tmp_path):
    with _app(tmp_path) as c:
        assert c.get("/api/v1/jobs").json() == []
        job_id = c.post("/api/v1/pipelines", json=DIAMOND).json()["job_id"]
        r = c.get("/api/v1/jobs")
        assert r.status_code == 200
        jobs = r.json()
        assert len(jobs) == 1
        assert jobs[0]["id"] == job_id
        assert jobs[0]["status"] == "pending"


def test_get_job_404(tmp_path):
    with _app(tmp_path) as c:
        assert c.get("/api/v1/jobs/does-not-exist").status_code == 404


def test_submit_invalid_spec_422(tmp_path):
    with _app(tmp_path) as c:
        bad = {"name": "bad", "nodes": [{"id": "a", "handler": "h", "depends_on": ["missing"]}]}
        r = c.post("/api/v1/pipelines", json=bad)
        assert r.status_code == 422
        detail = r.json()["detail"]
        assert isinstance(detail["errors"], list) and detail["errors"]


def test_artifacts_empty_for_new_job(tmp_path):
    with _app(tmp_path) as c:
        job_id = c.post("/api/v1/pipelines", json=DIAMOND).json()["job_id"]
        r = c.get(f"/api/v1/jobs/{job_id}/artifacts")
        assert r.status_code == 200
        assert r.json() == []


def test_artifacts_404_unknown_job(tmp_path):
    with _app(tmp_path) as c:
        assert c.get("/api/v1/jobs/none/artifacts").status_code == 404


def test_retry_unknown_job_404(tmp_path):
    with _app(tmp_path) as c:
        assert c.post("/api/v1/jobs/none/retry").status_code == 404


def test_retry_non_failed_job_409(tmp_path):
    with _app(tmp_path) as c:
        job_id = c.post("/api/v1/pipelines", json=DIAMOND).json()["job_id"]
        assert c.post(f"/api/v1/jobs/{job_id}/retry").status_code == 409


def test_job_detail_includes_spec(tmp_path):
    """The UI graph is spec-driven, so job detail must carry the full spec."""
    with _app(tmp_path) as c:
        job_id = c.post("/api/v1/pipelines", json=DIAMOND).json()["job_id"]
        spec = c.get(f"/api/v1/jobs/{job_id}").json()["job"]["spec"]
        assert [n["id"] for n in spec["nodes"]] == ["a", "b", "c", "d"]
        assert spec["nodes"][1]["depends_on"] == ["a"]


def test_cors_preflight_allows_browser_origin(tmp_path):
    """Flutter web runs on a different origin, so preflight must succeed."""
    with _app(tmp_path) as c:
        r = c.options(
            "/api/v1/pipelines",
            headers={
                "Origin": "http://localhost:5173",
                "Access-Control-Request-Method": "POST",
            },
        )
        assert r.status_code == 200
        assert r.headers["access-control-allow-origin"] == "*"


def test_cors_origins_are_configurable(tmp_path):
    from stralis.config import Config

    engine = make_engine(f"sqlite:///{tmp_path}/cors.db")
    init_db(engine)
    repo = Repo(make_session_factory(engine))
    cfg = Config(cors_origins="https://ui.example.com,https://admin.example.com")
    with TestClient(create_app(config=cfg, repo=repo)) as c:
        r = c.get("/healthz", headers={"Origin": "https://ui.example.com"})
        assert r.status_code == 200
        assert r.headers["access-control-allow-origin"] == "https://ui.example.com"
