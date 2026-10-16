"""Redis Streams implementation of the :class:`TaskQueue` port.

Lightweight :class:`TaskMessage` payloads travel as a single ``payload`` field
on each stream entry; media bytes stay in the object store (claim-check).
Delivery is at-least-once: a message stays pending until acknowledged and may
be reclaimed by another consumer after an idle timeout.
"""

from __future__ import annotations

from typing import Any

import redis

from stralis.errors import QueueError
from stralis.models import TaskMessage
from stralis.ports.queue import ClaimedMessage

_PAYLOAD_FIELD = "payload"


def _text(value: Any) -> str:
    """Normalise Redis field/entry values (bytes when decode_responses=False)."""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _is_busy_group(exc: BaseException) -> bool:
    """Whether ``exc`` signals an already-existing consumer group.

    redis-py does not expose a dedicated ``BusyGroupError`` on every version;
    the server answers ``BUSYGROUP`` and it surfaces as a ``ResponseError``.
    """
    if type(exc).__name__ == "BusyGroupError":
        return True
    return "BUSYGROUP" in str(exc).upper()


class RedisTaskQueue:
    """Task queue backed by Redis Streams consumer groups."""

    def __init__(
        self,
        redis_url: str,
        *,
        group: str = "stralis-workers",
        stream: str = "stralis:tasks",
        client: redis.Redis | None = None,
    ) -> None:
        self._redis_url = redis_url
        self.group = group
        self.stream = stream
        self._client = client

    @property
    def client(self) -> redis.Redis:
        """The underlying client, created lazily from ``redis_url`` on first use."""
        if self._client is None:
            self._client = redis.Redis.from_url(
                self._redis_url, decode_responses=True
            )
        return self._client

    # -- TaskQueue ---------------------------------------------------------

    def setup(self) -> None:
        """Idempotently create the stream/group (safe to call repeatedly)."""
        try:
            self.client.xgroup_create(self.stream, self.group, id="$", mkstream=True)
        except Exception as exc:  # noqa: BLE001 - narrowed by _is_busy_group
            if not _is_busy_group(exc):
                raise
            # Group already exists (possibly at a different last-id) - fine.

    def put(self, message: TaskMessage) -> str:
        entry_id = self.client.xadd(
            self.stream, {_PAYLOAD_FIELD: message.model_dump_json()}
        )
        return _text(entry_id)

    def get(
        self, consumer: str, count: int = 1, block_ms: int = 1000
    ) -> list[ClaimedMessage]:
        block = block_ms if block_ms and block_ms > 0 else None
        response = self.client.xreadgroup(
            self.group,
            consumer,
            {self.stream: ">"},
            count=count,
            block=block,
        )
        claimed: list[ClaimedMessage] = []
        for _stream, entries in response or []:
            for entry_id, fields in entries:
                claimed.append(self._to_claimed(entry_id, fields))
        return claimed

    def ack(self, claimed: ClaimedMessage) -> None:
        self.client.xack(self.stream, self.group, claimed.ref)

    def reclaim(
        self, consumer: str, min_idle_ms: int, count: int = 10
    ) -> list[ClaimedMessage]:
        # XAUTOCLAIM also transfers ownership; that is acceptable here.
        response = self.client.xautoclaim(
            self.stream,
            self.group,
            consumer,
            min_idle_ms,
            start_id="0-0",
            count=count,
        )
        # redis-py returns [next, messages] or [next, messages, deleted_ids].
        messages = response[1] if len(response) >= 2 else []
        claimed: list[ClaimedMessage] = []
        for entry_id, fields in messages:
            claimed.append(self._to_claimed(entry_id, fields))
        return claimed

    # -- helpers -----------------------------------------------------------

    def _to_claimed(self, entry_id: Any, fields: dict[Any, Any]) -> ClaimedMessage:
        payload = fields.get(_PAYLOAD_FIELD)
        if payload is None:
            payload = fields.get(_PAYLOAD_FIELD.encode("utf-8"))
        if payload is None:
            raise QueueError(f"queue entry {_text(entry_id)!r} has no payload field")
        try:
            message = TaskMessage.model_validate_json(_text(payload))
        except Exception as exc:  # noqa: BLE001 - surface as domain error
            raise QueueError(
                f"invalid task payload for entry {_text(entry_id)!r}: {exc}"
            ) from exc
        return ClaimedMessage(ref=_text(entry_id), message=message)
