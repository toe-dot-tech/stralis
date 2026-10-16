"""FastAPI application for the Stralis control plane.

Importing this module must have **no side effects**: no database engine is
built and no tables are created at import time. When a ``repo`` is injected
(tests) it is used as-is; otherwise the engine/DB/repo are built lazily inside
the FastAPI lifespan.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

from stralis.config import Config
from stralis.db.engine import init_db, make_engine, make_session_factory
from stralis.db.repo import Repo
from stralis.errors import PipelineValidationError
from stralis.models import JobStatus, PipelineSpec


def _build_repo(config: Config) -> Repo:
    engine = make_engine(config.database_url)
    init_db(engine)
    return Repo(make_session_factory(engine))


def create_app(config: Config | None = None, repo: Repo | None = None) -> FastAPI:
    """Build the FastAPI app.

    ``repo`` short-circuits all DB construction (used by tests). ``config``
    drives lazy construction when no repo is injected.
    """
    resolved_config = config or Config.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.repo = repo if repo is not None else _build_repo(resolved_config)
        yield

    app = FastAPI(title="Stralis", version="0.1.0", lifespan=lifespan)

    # Browser clients (the Flutter web UI) are served from a different origin,
    # so opt-in CORS is required. Default is open; tighten with
    # STRALIS_CORS_ORIGINS=https://ui.example.com,...
    origins = [o.strip() for o in resolved_config.cors_origins.split(",") if o.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins or ["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    def get_repo(request: Request) -> Repo:
        return request.app.state.repo

    @app.post("/api/v1/pipelines", status_code=202)
    def submit_pipeline(spec: PipelineSpec, request: Request) -> dict[str, Any]:
        try:
            job = get_repo(request).create_job(spec)
        except PipelineValidationError as exc:
            raise HTTPException(status_code=422, detail={"errors": exc.errors})
        return {"job_id": job.id, "name": job.name, "status": job.status.value}

    @app.get("/api/v1/jobs")
    def list_jobs(
        request: Request, limit: int = 50, offset: int = 0
    ) -> list[dict[str, Any]]:
        jobs = get_repo(request).list_jobs(limit=limit, offset=offset)
        return [job.model_dump(mode="json") for job in jobs]

    @app.get("/api/v1/jobs/{job_id}")
    def get_job(job_id: str, request: Request) -> dict[str, Any]:
        repo = get_repo(request)
        job = repo.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return {
            "job": job.model_dump(mode="json"),
            "node_runs": [run.model_dump(mode="json") for run in repo.get_node_runs(job_id)],
        }

    @app.get("/api/v1/jobs/{job_id}/artifacts")
    def list_artifacts(job_id: str, request: Request) -> list[dict[str, Any]]:
        repo = get_repo(request)
        if repo.get_job(job_id) is None:
            raise HTTPException(status_code=404, detail="job not found")
        return [a.model_dump(mode="json") for a in repo.list_artifacts(job_id)]

    @app.post("/api/v1/jobs/{job_id}/retry", status_code=202)
    def retry_job(job_id: str, request: Request) -> dict[str, Any]:
        repo = get_repo(request)
        job = repo.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status != JobStatus.FAILED:
            raise HTTPException(
                status_code=409,
                detail=f"job is {job.status.value}, not failed",
            )
        updated = repo.retry_job(job_id)
        if updated is None:  # raced with another writer
            raise HTTPException(status_code=409, detail="job is not retryable")
        return updated.model_dump(mode="json")

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app


# Module-level app for ``uvicorn stralis.api.app:app``. Construction is
# side-effect free; the engine is built in the lifespan above.
app = create_app()
