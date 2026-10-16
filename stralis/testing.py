"""In-memory test doubles for the Stralis ports.

These satisfy :class:`~stralis.ports.queue.TaskQueue` and
:class:`~stralis.ports.storage.ObjectStore` structurally (runtime-checkable
protocols) and need no external services, making them suitable for fast unit
tests and local development.
"""

from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from stralis.errors import StorageError
from stralis.models import ArtifactRef, TaskMessage
from stralis.ports.queue import ClaimedMessage


@dataclass
class _Entry:
    ref: str
    message: TaskMessage
    delivered_to: str | None = None
    delivered_at: float | None = None
    acked: bool = False


class InMemoryQueue:
    """A minimal, thread-safe, at-least-once queue for tests.

    ``get`` only ever yields not-yet-delivered messages (like Redis ``>``) and
    never blocks; ``reclaim`` returns unacked messages idle for long enough.
    """

    def __init__(self) -> None:
        self._entries: list[_Entry] = []
        self._counter = 0
        self._lock = threading.Lock()

    # -- TaskQueue ---------------------------------------------------------

    def setup(self) -> None:  # noqa: D102 - idempotent no-op
        return None

    def put(self, message: TaskMessage) -> str:
        with self._lock:
            self._counter += 1
            ref = f"{self._counter}-0"
            self._entries.append(_Entry(ref=ref, message=message))
            return ref

    def get(
        self, consumer: str, count: int = 1, block_ms: int = 1000
    ) -> list[ClaimedMessage]:
        del block_ms  # never block
        now = time.time()
        claimed: list[ClaimedMessage] = []
        with self._lock:
            for entry in self._entries:
                if len(claimed) >= count:
                    break
                if entry.delivered_at is not None or entry.acked:
                    continue
                entry.delivered_to = consumer
                entry.delivered_at = now
                claimed.append(ClaimedMessage(ref=entry.ref, message=entry.message))
        return claimed

    def ack(self, claimed: ClaimedMessage) -> None:
        with self._lock:
            entry = self._find(claimed.ref)
            if entry is not None:
                entry.acked = True

    def reclaim(
        self, consumer: str, min_idle_ms: int, count: int = 10
    ) -> list[ClaimedMessage]:
        now = time.time()
        claimed: list[ClaimedMessage] = []
        with self._lock:
            for entry in self._entries:
                if len(claimed) >= count:
                    break
                if entry.acked or entry.delivered_at is None:
                    continue
                idle_ms = (now - entry.delivered_at) * 1000.0
                if idle_ms < min_idle_ms:
                    continue
                entry.delivered_to = consumer
                entry.delivered_at = now
                claimed.append(ClaimedMessage(ref=entry.ref, message=entry.message))
        return claimed

    # -- test helpers ------------------------------------------------------

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def refs(self) -> list[str]:
        with self._lock:
            return [e.ref for e in self._entries]

    def _find(self, ref: str) -> _Entry | None:
        for entry in self._entries:
            if entry.ref == ref:
                return entry
        return None


class InMemoryObjectStore:
    """A dict-backed object store using the ``mem://`` URI scheme."""

    def __init__(self, *, bucket: str = "test") -> None:
        self._bucket = bucket
        self._objects: dict[str, bytes] = {}
        self._content_types: dict[str, str | None] = {}
        self._lock = threading.Lock()

    # -- ObjectStore -------------------------------------------------------

    def put_file(
        self,
        key: str,
        path: Path,
        *,
        name: str | None = None,
        content_type: str | None = None,
    ) -> ArtifactRef:
        data = Path(path).read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        uri = self._uri(key)
        with self._lock:
            self._objects[uri] = data
            self._content_types[uri] = content_type
        return ArtifactRef(
            name=name or _basename(key),
            uri=uri,
            digest=digest,
            size=len(data),
            content_type=content_type,
        )

    def get(self, uri: str, dest: Path) -> ArtifactRef:
        key = self._key(uri)
        with self._lock:
            data = self._objects.get(uri)
            content_type = self._content_types.get(uri)
        if data is None:
            raise StorageError(f"object not found: {uri!r}")
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        return ArtifactRef(
            name=_basename(key),
            uri=uri,
            digest=hashlib.sha256(data).hexdigest(),
            size=len(data),
            content_type=content_type,
        )

    def exists(self, uri: str) -> bool:
        with self._lock:
            return uri in self._objects

    def delete(self, uri: str) -> None:
        with self._lock:
            self._objects.pop(uri, None)
            self._content_types.pop(uri, None)

    # -- convenience -------------------------------------------------------

    def read_bytes(self, uri: str) -> bytes:
        with self._lock:
            data = self._objects.get(uri)
        if data is None:
            raise StorageError(f"object not found: {uri!r}")
        return data

    # -- helpers -----------------------------------------------------------

    def _uri(self, key: str) -> str:
        return f"mem://{self._bucket}/{key}"

    def _key(self, uri: str) -> str:
        prefix = f"mem://{self._bucket}/"
        if not uri.startswith(prefix):
            raise StorageError(f"object URI not served by this store: {uri!r}")
        return uri[len(prefix):]


def _basename(key: str) -> str:
    return key.rsplit("/", 1)[-1]
