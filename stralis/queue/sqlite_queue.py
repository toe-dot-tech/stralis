"""SQLite-backed task queue — the zero-dependency local/edge backend.

Behaviourally equivalent to the Redis Streams adapter for the engine's needs:
new-only reads (like ``XREADGROUP >``), ack, and idle reclaim. It exists so the
full stack (API + orchestrator + workers, in separate processes) can run with
no external services, which also makes it a nice fit for single-node edge
deployments.

Concurrency: SQLite in WAL mode with ``BEGIN IMMEDIATE`` claim transactions and
a busy timeout, so multiple worker processes can poll the same file safely.
"""

from __future__ import annotations

import sqlite3
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from stralis.errors import QueueError
from stralis.models import TaskMessage
from stralis.ports.queue import ClaimedMessage

_SCHEMA = """
CREATE TABLE IF NOT EXISTS stralis_tasks (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ref          TEXT NOT NULL UNIQUE,
    payload      TEXT NOT NULL,
    consumer     TEXT,
    delivered_at REAL,
    acked        INTEGER NOT NULL DEFAULT 0,
    created_at   REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_stralis_tasks_pending
    ON stralis_tasks (acked, consumer, id);
"""


def sqlite_path_from_url(database_url: str) -> str:
    """Map a SQLAlchemy sqlite URL to a filesystem path.

    ``sqlite:///rel.db`` -> ``rel.db``; ``sqlite:////abs.db`` -> ``/abs.db``.
    The ``:memory:`` database is rejected: it is per-connection, so it cannot
    back a cross-process queue.
    """
    if not database_url.startswith("sqlite"):
        raise QueueError(
            f"SQLiteTaskQueue requires a sqlite:// URL, got {database_url!r}"
        )
    tail = database_url[len("sqlite://"):].split("?", 1)[0]
    if tail.startswith("/"):
        tail = tail[1:]
    if tail in ("", ":memory:"):
        raise QueueError(
            "SQLiteTaskQueue needs a file-backed database "
            "(sqlite:///path.db), not :memory:"
        )
    return tail


class SQLiteTaskQueue:
    def __init__(
        self,
        database_url: str,
        *,
        group: str = "stralis-workers",
        stream: str = "stralis-tasks",
        poll_interval_s: float = 0.05,
        busy_timeout_s: float = 30.0,
    ) -> None:
        self.database_url = database_url
        self.group = group  # accepted for parity with RedisTaskQueue
        self.stream = stream
        self.poll_interval_s = poll_interval_s
        self.busy_timeout_s = busy_timeout_s
        self._path = sqlite_path_from_url(database_url)
        self._setup_done = False

    # ------------------------------------------------------------- plumbing

    def _connect(self) -> sqlite3.Connection:
        path = self._path
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            path,
            timeout=self.busy_timeout_s,
            isolation_level=None,  # explicit transactions
        )
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(f"PRAGMA busy_timeout={int(self.busy_timeout_s * 1000)}")
        return conn

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:  # pragma: no cover
                pass
            raise
        finally:
            conn.close()

    def setup(self) -> None:
        if self._setup_done:
            return
        conn = self._connect()
        try:
            conn.executescript(_SCHEMA)
        finally:
            conn.close()
        self._setup_done = True

    # ------------------------------------------------------- TaskQueue API

    def put(self, message: TaskMessage) -> str:
        self.setup()
        ref = uuid.uuid4().hex
        with self._tx() as c:
            c.execute(
                "INSERT INTO stralis_tasks (ref, payload, created_at) VALUES (?,?,?)",
                (ref, message.model_dump_json(), time.time()),
            )
        return ref

    def get(
        self, consumer: str, count: int = 1, block_ms: int = 1000
    ) -> list[ClaimedMessage]:
        self.setup()
        deadline = time.monotonic() + (block_ms / 1000.0 if block_ms > 0 else 0.0)
        while True:
            claimed = self._claim_new(consumer, count)
            if claimed or time.monotonic() >= deadline:
                return claimed
            time.sleep(self.poll_interval_s)

    def _claim_new(self, consumer: str, count: int) -> list[ClaimedMessage]:
        now = time.time()
        with self._tx() as c:
            rows = c.execute(
                "SELECT id, ref, payload FROM stralis_tasks "
                "WHERE acked = 0 AND consumer IS NULL "
                "ORDER BY id LIMIT ?",
                (count,),
            ).fetchall()
            if rows:
                c.executemany(
                    "UPDATE stralis_tasks SET consumer = ?, delivered_at = ? WHERE id = ?",
                    [(consumer, now, r[0]) for r in rows],
                )
        return [self._claimed(r[1], r[2]) for r in rows]

    def ack(self, claimed: ClaimedMessage) -> None:
        self.setup()
        with self._tx() as c:
            c.execute(
                "UPDATE stralis_tasks SET acked = 1 WHERE ref = ?", (claimed.ref,)
            )

    def reclaim(
        self, consumer: str, min_idle_ms: int, count: int = 10
    ) -> list[ClaimedMessage]:
        self.setup()
        cutoff = time.time() - (min_idle_ms / 1000.0)
        now = time.time()
        with self._tx() as c:
            rows = c.execute(
                "SELECT id, ref, payload FROM stralis_tasks "
                "WHERE acked = 0 AND consumer IS NOT NULL AND delivered_at < ? "
                "ORDER BY id LIMIT ?",
                (cutoff, count),
            ).fetchall()
            if rows:
                c.executemany(
                    "UPDATE stralis_tasks SET consumer = ?, delivered_at = ? WHERE id = ?",
                    [(consumer, now, r[0]) for r in rows],
                )
        return [self._claimed(r[1], r[2]) for r in rows]

    # --------------------------------------------------------------- misc

    def _claimed(self, ref: str, payload: str) -> ClaimedMessage:
        try:
            message = TaskMessage.model_validate_json(payload)
        except Exception as e:  # pragma: no cover - corrupt row
            raise QueueError(f"corrupt task payload {ref!r}: {e}") from e
        return ClaimedMessage(ref=ref, message=message)

    def pending_count(self) -> int:
        """Undelivered (not-yet-consumed) task count."""
        self.setup()
        conn = self._connect()
        try:
            return conn.execute(
                "SELECT COUNT(*) FROM stralis_tasks WHERE acked = 0 AND consumer IS NULL"
            ).fetchone()[0]
        finally:
            conn.close()

    def prune(self) -> None:
        """Drop acked rows (keeps the local file small)."""
        self.setup()
        with self._tx() as c:
            c.execute("DELETE FROM stralis_tasks WHERE acked = 1")


__all__ = ["SQLiteTaskQueue", "sqlite_path_from_url"]
