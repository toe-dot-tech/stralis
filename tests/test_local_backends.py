"""Local (no-services) backends: SQLite task queue + filesystem object store."""

from __future__ import annotations

import hashlib

import pytest

from stralis.errors import QueueError, StorageError
from stralis.models import TaskMessage
from stralis.ports.queue import TaskQueue
from stralis.ports.storage import ObjectStore
from stralis.queue.sqlite_queue import SQLiteTaskQueue, sqlite_path_from_url
from stralis.storage.local import LocalObjectStore


# --------------------------------------------------------------------- queue


@pytest.fixture
def qurl(tmp_path):
    return f"sqlite:///{tmp_path / 'tasks.db'}"


def test_sqlite_path_parsing():
    assert sqlite_path_from_url("sqlite:///./stralis.db") == "./stralis.db"
    assert sqlite_path_from_url("sqlite:////var/lib/x.db") == "/var/lib/x.db"
    assert sqlite_path_from_url("sqlite:///a.db?check_same_thread=false") == "a.db"
    with pytest.raises(QueueError):
        sqlite_path_from_url("sqlite:///:memory:")
    with pytest.raises(QueueError):
        sqlite_path_from_url("postgresql://localhost/x")


def test_sqlite_queue_protocol_and_persistence(qurl):
    q = SQLiteTaskQueue(qurl)
    assert isinstance(q, TaskQueue)
    q.setup()
    q.setup()  # idempotent

    ref = q.put(TaskMessage(job_id="j1", node_id="a", attempt=0))
    assert ref

    # a *different* instance (i.e. another process) sees the same task
    q2 = SQLiteTaskQueue(qurl)
    msgs = q2.get("worker-2", count=10)
    assert len(msgs) == 1
    assert msgs[0].message.job_id == "j1"
    assert msgs[0].ref == ref

    # new-only reads: not redelivered before ack
    assert q2.get("worker-3", count=10) == []
    # reclaim picks it up because it is unacked and idle
    reclaimed = q2.reclaim("worker-1", min_idle_ms=0)
    assert [m.ref for m in reclaimed] == [ref]
    q2.ack(msgs[0])
    assert q2.reclaim("worker-1", min_idle_ms=0) == []


def test_sqlite_queue_orders_and_counts(qurl):
    q = SQLiteTaskQueue(qurl)
    for i in range(5):
        q.put(TaskMessage(job_id="j", node_id=f"n{i}", attempt=0))
    assert q.pending_count() == 5

    first = q.get("w1", count=2)
    assert [m.message.node_id for m in first] == ["n0", "n1"]
    for m in first:
        q.ack(m)
    assert q.pending_count() == 3

    rest = q.get("w1", count=10)
    assert [m.message.node_id for m in rest] == ["n2", "n3", "n4"]
    q.prune()
    # prune drops acked rows; the 3 delivered-but-unacked rows survive and are
    # still reclaimable (that is how a crashed worker's task is recovered)
    assert q.pending_count() == 0  # nothing *undelivered* is left
    assert len(q.reclaim("w2", min_idle_ms=0, count=10)) == 3
    assert q.pending_count() == 0


def test_sqlite_queue_blocking_get_returns_after_timeout(qurl):
    import time

    q = SQLiteTaskQueue(qurl, poll_interval_s=0.01)
    start = time.monotonic()
    assert q.get("w", count=1, block_ms=100) == []
    assert time.monotonic() - start >= 0.09


# --------------------------------------------------------------------- store


def test_local_store_protocol_roundtrip(tmp_path):
    store = LocalObjectStore(tmp_path / "objects")
    assert isinstance(store, ObjectStore)

    src = tmp_path / "hello.bin"
    payload = b"media-bytes" * 1000
    src.write_bytes(payload)

    ref = store.put_file("job1/node1/abc/file.mp4", src, name="file.mp4")
    assert ref.name == "file.mp4"
    assert ref.size == len(payload)
    assert ref.digest == hashlib.sha256(payload).hexdigest()
    assert ref.uri == "local://local/job1/node1/abc/file.mp4"
    assert store.exists(ref.uri)

    dest = tmp_path / "out" / "copy.mp4"
    got = store.get(ref.uri, dest)
    assert dest.read_bytes() == payload
    assert got.digest == ref.digest and got.size == ref.size

    store.delete(ref.uri)
    assert not store.exists(ref.uri)
    store.delete(ref.uri)  # deleting again is fine
    with pytest.raises(StorageError):
        store.get(ref.uri, dest)


def test_local_store_rejects_foreign_or_unsafe_uris(tmp_path):
    store = LocalObjectStore(tmp_path / "objects")
    assert store.exists("s3://other/thing") is False
    with pytest.raises(StorageError):
        store.get("s3://other/thing", tmp_path / "x")
    with pytest.raises(StorageError):
        store.get("local://local/../../etc/passwd", tmp_path / "x")


def test_local_store_is_filesystem_backed_across_instances(tmp_path):
    root = tmp_path / "objects"
    a = LocalObjectStore(root)
    src = tmp_path / "f.txt"
    src.write_text("shared", encoding="utf-8")
    ref = a.put_file("k/f.txt", src)

    b = LocalObjectStore(root)  # a second process would do exactly this
    dest = tmp_path / "back.txt"
    b.get(ref.uri, dest)
    assert dest.read_text(encoding="utf-8") == "shared"
