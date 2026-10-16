"""Object store adapters."""

from __future__ import annotations

from stralis.storage.local import LocalObjectStore
from stralis.storage.s3 import S3ObjectStore

__all__ = ["S3ObjectStore", "LocalObjectStore"]
