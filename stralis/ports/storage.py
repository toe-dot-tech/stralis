"""Object store port (claim-check pattern).

Media artifacts are persisted in an S3-compatible object store; only the
resulting URI travels through the task queue.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable

from stralis.models import ArtifactRef


@runtime_checkable
class ObjectStore(Protocol):
    """S3-compatible blob store for intermediate/final pipeline artifacts."""

    def put_file(
        self,
        key: str,
        path: Path,
        *,
        name: str | None = None,
        content_type: str | None = None,
    ) -> ArtifactRef:
        """Upload ``path`` under ``key``; returns an :class:`ArtifactRef`.

        The digest (sha256) of the stored bytes must be computed on upload.
        """

    def get(self, uri: str, dest: Path) -> ArtifactRef:
        """Download the object at ``uri`` to local path ``dest``."""

    def exists(self, uri: str) -> bool:
        """Return True when an object exists at ``uri``."""

    def delete(self, uri: str) -> None:
        """Delete the object at ``uri`` (no error when absent)."""
