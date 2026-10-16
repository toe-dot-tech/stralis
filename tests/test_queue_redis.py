"""Integration tests for :class:`RedisTaskQueue`.

Skipped unless ``STRALIS_TEST_REDIS_URL`` points at a running Redis server.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import pytest
import redis

from stralis.models import TaskMessage
from stralis.queue import RedisTaskQueue

REDIS_URL = os.environ.get("STRALIS_TEST_REDIS_URL")

pytestmark = pytest.mark.skipif(
    not REDIS_URL, reason="STRALIS_TEST_REDIS_URL not set"
)


@pytest.fixture
def stream_name() -> str:
    return f"stralis:test:{uuid.uuid4().hex}"


@pytest.fixture
def queue(stream_name: str) -> Iterator[RedisTaskQueue]:
    q = RedisTaskQueue(str(REDIS_URL), stream=stream_name)
    q.setup()
    try:
        yield q
    finally:
        q.client.delete(stream_name)


def test_setup_is_idempotent(queue: RedisTaskQueue) -> None:
    queue.setup()  # second call must not raise BUSYGROUP
    queue.setup()


def test_put_get_ack_roundtrip(queue: RedisTaskQueue) -> None:
    message = TaskMessage(job_id="job-1", node_id="render", attempt=1)
    ref = queue.put(message)
    assert isinstance(ref, str) and ref

    claimed = queue.get("worker-a", count=1, block_ms=100)
    assert len(claimed) == 1
    assert claimed[0].ref == ref
    assert claimed[0].message == message

    queue.ack(claimed[0])

    # Acked -> no new deliveries and nothing to reclaim.
    assert queue.get("worker-a", count=1, block_ms=100) == []
    assert queue.reclaim("worker-b", min_idle_ms=0) == []


def test_no_duplicate_delivery_before_ack(queue: RedisTaskQueue) -> None:
    queue.put(TaskMessage(job_id="j", node_id="n"))

    first = queue.get("worker-a", count=1, block_ms=100)
    assert len(first) == 1

    # Same consumer asking for *new* messages gets nothing.
    assert queue.get("worker-a", count=1, block_ms=100) == []


def test_reclaim_moves_idle_messages(queue: RedisTaskQueue) -> None:
    message = TaskMessage(job_id="j", node_id="n")
    queue.put(message)

    claimed = queue.get("worker-a", count=1, block_ms=100)
    assert len(claimed) == 1

    reclaimed = queue.reclaim("worker-b", min_idle_ms=0, count=1)
    assert len(reclaimed) == 1
    assert reclaimed[0].ref == claimed[0].ref
    assert reclaimed[0].message == message

    queue.ack(reclaimed[0])


def test_get_returns_immediately_when_not_blocking(queue: RedisTaskQueue) -> None:
    # block_ms <= 0 must never block regardless of stream contents.
    assert queue.get("worker-a", count=1, block_ms=0) == []


def test_redis_client_can_connect() -> None:
    client = redis.Redis.from_url(str(REDIS_URL), decode_responses=True)
    assert client.ping() is True
