"""Tests for the in-memory port implementations (no external services)."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from stralis.errors import StorageError
from stralis.models import TaskMessage
from stralis.ports.queue import TaskQueue
from stralis.ports.storage import ObjectStore
from stralis.testing import InMemoryObjectStore, InMemoryQueue


# --------------------------------------------------------------------------
# Protocol compliance
# --------------------------------------------------------------------------


def test_fakes_satisfy_protocols() -> None:
    assert isinstance(InMemoryQueue(), TaskQueue)
    assert isinstance(InMemoryObjectStore(), ObjectStore)


# --------------------------------------------------------------------------
# Queue
# --------------------------------------------------------------------------


def test_put_get_roundtrip() -> None:
    queue = InMemoryQueue()
    message = TaskMessage(job_id="job-1", node_id="render", attempt=2)

    ref = queue.put(message)
    assert ref == "1-0"

    claimed = queue.get("worker-a")
    assert len(claimed) == 1
    assert claimed[0].ref == ref
    assert claimed[0].message == message


def test_get_does_not_redeliver_before_ack() -> None:
    queue = InMemoryQueue()
    queue.put(TaskMessage(job_id="j", node_id="n"))

    first = queue.get("worker-a")
    assert len(first) == 1

    # Not acknowledged -> never handed out again as a *new* message.
    assert queue.get("worker-a") == []
    assert queue.get("worker-b") == []


def test_ack_clears_pending() -> None:
    queue = InMemoryQueue()
    queue.put(TaskMessage(job_id="j", node_id="n"))

    claimed = queue.get("worker-a")
    queue.ack(claimed[0])
    queue.ack(claimed[0])  # idempotent

    assert queue.get("worker-a") == []
    assert queue.reclaim("worker-b", min_idle_ms=0) == []


def test_reclaim_returns_idle_unacked() -> None:
    queue = InMemoryQueue()
    queue.put(TaskMessage(job_id="j", node_id="n"))

    claimed = queue.get("worker-a")
    assert len(claimed) == 1

    reclaimed = queue.reclaim("worker-b", min_idle_ms=0)
    assert len(reclaimed) == 1
    assert reclaimed[0].ref == claimed[0].ref
    assert reclaimed[0].message == claimed[0].message


def test_reclaim_respects_idle_threshold() -> None:
    queue = InMemoryQueue()
    queue.put(TaskMessage(job_id="j", node_id="n"))
    queue.get("worker-a")

    # Freshly delivered -> not idle enough.
    assert queue.reclaim("worker-b", min_idle_ms=60_000) == []


def test_reclaim_skips_acked() -> None:
    queue = InMemoryQueue()
    queue.put(TaskMessage(job_id="j", node_id="n"))

    claimed = queue.get("worker-a")
    queue.ack(claimed[0])

    assert queue.reclaim("worker-b", min_idle_ms=0) == []


def test_consumers_get_disjoint_new_messages() -> None:
    queue = InMemoryQueue()
    r1 = queue.put(TaskMessage(job_id="j", node_id="a"))
    r2 = queue.put(TaskMessage(job_id="j", node_id="b"))

    a = queue.get("worker-a", count=1)
    b = queue.get("worker-b", count=1)

    assert {a[0].ref, b[0].ref} == {r1, r2}


# --------------------------------------------------------------------------
# Object store
# --------------------------------------------------------------------------


def test_store_put_get_roundtrip(tmp_path: Path) -> None:
    store = InMemoryObjectStore()
    src = tmp_path / "src.bin"
    payload = b"stralis-media-bytes\x00\x01\x02"
    src.write_bytes(payload)

    ref = store.put_file("assets/clip.bin", src)
    assert ref.uri == "mem://test/assets/clip.bin"
    assert ref.size == len(payload)
    assert ref.digest == hashlib.sha256(payload).hexdigest()

    dest = tmp_path / "nested" / "out.bin"
    got = store.get(ref.uri, dest)

    assert dest.read_bytes() == payload
    assert got.digest == ref.digest
    assert got.size == len(payload)


def test_store_exists_and_delete(tmp_path: Path) -> None:
    store = InMemoryObjectStore()
    src = tmp_path / "f.bin"
    src.write_bytes(b"data")
    ref = store.put_file("k.bin", src)

    assert store.exists(ref.uri) is True
    store.delete(ref.uri)
    assert store.exists(ref.uri) is False


def test_store_delete_missing_is_noop() -> None:
    store = InMemoryObjectStore()
    store.delete("mem://test/nope")  # does not raise


def test_store_get_missing_raises_storage_error(tmp_path: Path) -> None:
    store = InMemoryObjectStore()
    with pytest.raises(StorageError):
        store.get("mem://test/missing.bin", tmp_path / "out.bin")


def test_store_name_defaults_to_basename(tmp_path: Path) -> None:
    store = InMemoryObjectStore()
    src = tmp_path / "f.bin"
    src.write_bytes(b"data")

    ref = store.put_file("deep/nested/clip.mp4", src)
    assert ref.name == "clip.mp4"


def test_store_read_bytes_convenience(tmp_path: Path) -> None:
    store = InMemoryObjectStore()
    src = tmp_path / "f.bin"
    src.write_bytes(b"payload")
    ref = store.put_file("k.bin", src)

    assert store.read_bytes(ref.uri) == b"payload"
