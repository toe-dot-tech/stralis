"""CLI tests for the Stralis typer app."""

from __future__ import annotations

import json
import re

import pytest
from typer.testing import CliRunner

from stralis.cli import app

runner = CliRunner()


@pytest.fixture(autouse=True)
def _database_env(tmp_path, monkeypatch):
    monkeypatch.setenv("STRALIS_DATABASE_URL", f"sqlite:///{tmp_path}/cli.db")


def _write_spec(tmp_path, data: dict, name: str = "spec.json"):
    p = tmp_path / name
    p.write_text(json.dumps(data), encoding="utf-8")
    return p


def _submit(tmp_path, data: dict) -> str:
    res = runner.invoke(app, ["submit", str(_write_spec(tmp_path, data))])
    assert res.exit_code == 0, res.output
    match = re.search(r"job_id\s+(\S+)", res.output)
    if match is None:
        raise AssertionError(f"no job_id in output: {res.output!r}")
    return match.group(1)


PIPELINE: dict = {
    "name": "p",
    "nodes": [
        {"id": "a", "handler": "h"},
        {"id": "b", "handler": "h", "depends_on": ["a"]},
    ],
}


def test_submit_then_status(tmp_path):
    job_id = _submit(tmp_path, PIPELINE)
    res = runner.invoke(app, ["status", job_id])
    assert res.exit_code == 0, res.output
    assert "pending" in res.output
    assert "ready" in res.output


def test_status_unknown_job(tmp_path):
    res = runner.invoke(app, ["status", "no-such-job"])
    assert res.exit_code == 1


def test_list_shows_submitted_job(tmp_path):
    job_id = _submit(tmp_path, PIPELINE)
    res = runner.invoke(app, ["list"])
    assert res.exit_code == 0, res.output
    assert job_id in res.output


def test_retry_non_failed_exits_1(tmp_path):
    job_id = _submit(tmp_path, PIPELINE)
    res = runner.invoke(app, ["retry", job_id])
    assert res.exit_code == 1


def test_submit_invalid_spec(tmp_path):
    bad = {"name": "bad", "nodes": [{"id": "a", "handler": "h", "depends_on": ["missing"]}]}
    res = runner.invoke(app, ["submit", str(_write_spec(tmp_path, bad))])
    assert res.exit_code == 1
    assert res.stderr or res.output


@pytest.mark.parametrize("command", ["worker", "orchestrator", "serve"])
def test_command_help(command):
    res = runner.invoke(app, [command, "--help"])
    assert res.exit_code == 0, res.output


def test_root_help_lists_commands():
    res = runner.invoke(app, ["--help"])
    assert res.exit_code == 0, res.output
    for name in ("submit", "status", "list", "retry", "worker", "orchestrator", "serve"):
        assert name in res.output
