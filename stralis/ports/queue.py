"""Task queue port.

The queue transports lightweight :class:`~stralis.models.TaskMessage` payloads
only — media bytes travel through the object store (claim-check pattern).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from stralis.models import TaskMessage


class ClaimedMessage:
    """A delivery handle for a message read from the queue.

    ``ref`` is backend-specific (e.g. a Redis stream entry ID) and must be
    passed back to :meth:`TaskQueue.ack` to acknowledge the delivery.
    """

    __slots__ = ("ref", "message")

    def __init__(self, ref: str, message: TaskMessage) -> None:
        self.ref = ref
        self.message = message

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"ClaimedMessage(ref={self.ref!r}, message={self.message!r})"


@runtime_checkable
class TaskQueue(Protocol):
    """Asynchronous task queue used to schedule ready DAG nodes."""

    def setup(self) -> None:
        """Idempotently create group/streams/etc. Safe to call repeatedly."""

    def put(self, message: TaskMessage) -> str:
        """Enqueue a task message. Returns a backend delivery reference."""

    def get(
        self, consumer: str, count: int = 1, block_ms: int = 1000
    ) -> list[ClaimedMessage]:
        """Read messages for ``consumer`` (long-poll up to ``block_ms``).

        Returned messages are pending until acknowledged; duplicate delivery
        of the same message is expected and must be tolerated by consumers.
        """

    def ack(self, claimed: ClaimedMessage) -> None:
        """Acknowledge a previously claimed message."""

    def reclaim(self, consumer: str, min_idle_ms: int, count: int = 10) -> list[ClaimedMessage]:
        """Move messages idle for at least ``min_idle_ms`` back to ``consumer``."""
