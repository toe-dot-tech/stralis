"""End-to-end: submit a real example pipeline and run it to completion.

Exercises the full local stack — DAG compile, orchestrator scheduling, worker
execution with the built-in ``shell`` handler, claim-check storage, and job
finalization — with no external services.
"""

from __future__ import annotations

import json
from pathlib import Path

from stralis.models import JobStatus, NodeStatus, PipelineSpec

from tests.conftest import drive

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


def load_example(name: str) -> PipelineSpec:
    data = json.loads((EXAMPLES / name).read_text(encoding="utf-8"))
    return PipelineSpec.model_validate(data)


def test_demo_diamond_runs_end_to_end(repo, queue, store, config):
    from stralis.worker.handlers import default_handlers

    spec = load_example("demo_dag.json")
    job = repo.create_job(spec)

    status = drive(repo, queue, store, config, default_handlers())
    assert status is JobStatus.SUCCEEDED

    st = {r.node_id: r.status for r in repo.get_node_runs(job.id)}
    assert all(s is NodeStatus.SUCCEEDED for s in st.values()), st

    merge = repo.get_node_run(job.id, "merge")
    assert store.read_bytes(merge.outputs["merged"].uri) == b"STRALIS|stralis-right"

    # every node produced exactly one claim-checked artifact
    assert len(repo.list_artifacts(job.id)) == 4


def test_examples_are_valid_specs():
    for name in ("demo_dag.json", "video_transcode.json", "audio_transcribe.json"):
        spec = load_example(name)
        assert spec.nodes
