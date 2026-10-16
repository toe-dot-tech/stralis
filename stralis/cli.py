"""Command-line interface for Stralis."""

from __future__ import annotations

import time
from pathlib import Path

import typer

from stralis.config import Config
from stralis.db.engine import init_db, make_engine, make_session_factory
from stralis.db.repo import Repo
from stralis.errors import PipelineValidationError
from stralis.models import JobRecord, JobStatus, PipelineSpec

app = typer.Typer(
    name="stralis",
    help="Distributed media automation engine.",
    no_args_is_help=True,
    add_completion=False,
)

_TERMINAL = {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED}


def _repo(config: Config) -> Repo:
    """Build a repo directly from stdlib pieces (no stralis.runtime dependency)."""
    engine = make_engine(config.database_url)
    init_db(engine)
    return Repo(make_session_factory(engine))


def _print_job(repo: Repo, job: JobRecord) -> None:
    typer.echo(f"{job.id}  {job.status.value}  {job.name}")
    for run in repo.get_node_runs(job.id):
        line = f"{run.node_id}  {run.status.value}  {run.attempt}/{run.max_attempts}"
        if run.error:
            line += f"  {run.error}"
        typer.echo(line)


@app.command()
def submit(path: Path) -> None:
    """Submit a pipeline spec (JSON) and print the new job id."""
    config = Config.from_env()
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        typer.echo(f"error: cannot read {path}: {exc}", err=True)
        raise typer.Exit(1)
    try:
        spec = PipelineSpec.model_validate_json(text)
    except ValueError as exc:
        typer.echo(f"error: invalid pipeline spec: {exc}", err=True)
        raise typer.Exit(1)

    try:
        job = _repo(config).create_job(spec)
    except PipelineValidationError as exc:
        for message in exc.errors:
            typer.echo(f"error: {message}", err=True)
        raise typer.Exit(1)
    typer.echo(f"job_id {job.id}")


@app.command()
def status(
    job_id: str,
    watch: bool = typer.Option(False, "--watch/--no-watch", help="Poll until terminal."),
) -> None:
    """Show a job's status and its node runs."""
    config = Config.from_env()
    repo = _repo(config)
    job = repo.get_job(job_id)
    if job is None:
        typer.echo(f"error: unknown job {job_id}", err=True)
        raise typer.Exit(1)

    _print_job(repo, job)
    if not watch:
        return

    try:
        while not job.status.terminal:
            time.sleep(1)
            job = repo.get_job(job_id)
            if job is None:
                typer.echo(f"error: unknown job {job_id}", err=True)
                raise typer.Exit(1)
            _print_job(repo, job)
    except KeyboardInterrupt:
        raise typer.Exit(130)
    typer.echo(f"final: {job.status.value}")


@app.command("list")
def list_jobs(limit: int = typer.Option(20, "--limit", help="Max jobs to show.")) -> None:
    """List recent jobs."""
    config = Config.from_env()
    repo = _repo(config)
    for job in repo.list_jobs(limit=limit):
        typer.echo(f"{job.id}  {job.status.value}  {job.name}  {job.created_at.isoformat()}")


@app.command()
def retry(job_id: str) -> None:
    """Resume a FAILED job and print its new status."""
    config = Config.from_env()
    repo = _repo(config)
    job = repo.get_job(job_id)
    if job is None:
        typer.echo(f"error: unknown job {job_id}", err=True)
        raise typer.Exit(1)

    updated = repo.retry_job(job_id)
    if updated is None:
        typer.echo(
            f"error: job {job_id} is {job.status.value}, not failed",
            err=True,
        )
        raise typer.Exit(1)
    typer.echo(f"{updated.id}  {updated.status.value}")


@app.command()
def worker() -> None:
    """Run a worker, consuming tasks from the queue."""
    config = Config.from_env()
    from stralis.runtime import build_worker

    build_worker(config).run_forever()


@app.command()
def orchestrator() -> None:
    """Run the orchestrator that drives pipeline state."""
    config = Config.from_env()
    from stralis.runtime import build_orchestrator

    build_orchestrator(config).run_forever()


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", "--host"),
    port: int = typer.Option(8000, "--port"),
) -> None:
    """Run the HTTP API server."""
    config = Config.from_env()
    import uvicorn

    from stralis.api.app import create_app

    uvicorn.run(create_app(config), host=host, port=port)


if __name__ == "__main__":
    app()
