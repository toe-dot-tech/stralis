"""Filesystem object store — the zero-dependency local/edge backend.

Same contract as the S3 adapter, with ``local://<bucket>/<key>`` URIs and files
under a configured root directory. Works across processes on one machine, so
the orchestrator and workers can exchange claim-check artifacts with no MinIO.
"""

from __future__ import annotations

import hashlib
import os
import shutil
from pathlib import Path

from stralis.errors import StorageError
from stralis.models import ArtifactRef

_CHUNK = 8 * 1024 * 1024


class LocalObjectStore:
    def __init__(self, root: str | Path, *, bucket: str = "local") -> None:
        self.root = Path(root)
        self.bucket = bucket
        self.root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------ plumbing

    def _uri(self, key: str) -> str:
        return f"local://{self.bucket}/{key.lstrip('/')}"

    def _key(self, uri: str) -> str:
        prefix = f"local://{self.bucket}/"
        if not uri.startswith(prefix):
            raise StorageError(f"object URI not served by this store: {uri!r}")
        key = uri[len(prefix):]
        if not key or ".." in Path(key).parts:
            raise StorageError(f"unsafe object key: {key!r}")
        return key

    def _path(self, key: str) -> Path:
        return self.root / key

    # -------------------------------------------------------- ObjectStore

    def put_file(
        self,
        key: str,
        path: Path,
        *,
        name: str | None = None,
        content_type: str | None = None,
    ) -> ArtifactRef:
        src = Path(path)
        if not src.is_file():
            raise StorageError(f"no such file to store: {src}")

        dest = self._path(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".part")
        digest = hashlib.sha256()
        size = 0
        with open(src, "rb") as fin, open(tmp, "wb") as fout:
            while chunk := fin.read(_CHUNK):
                digest.update(chunk)
                size += len(chunk)
                fout.write(chunk)
        os.replace(tmp, dest)  # atomic on the same filesystem

        return ArtifactRef(
            name=name or dest.name,
            uri=self._uri(key),
            digest=digest.hexdigest(),
            size=size,
            content_type=content_type,
        )

    def get(self, uri: str, dest: Path) -> ArtifactRef:
        key = self._key(uri)
        src = self._path(key)
        if not src.is_file():
            raise StorageError(f"object not found: {uri!r}")
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".part")
        shutil.copyfile(src, tmp)
        os.replace(tmp, dest)

        digest = hashlib.sha256()
        size = 0
        with open(dest, "rb") as f:
            while chunk := f.read(_CHUNK):
                digest.update(chunk)
                size += len(chunk)
        return ArtifactRef(
            name=src.name, uri=uri, digest=digest.hexdigest(), size=size
        )

    def exists(self, uri: str) -> bool:
        try:
            return self._path(self._key(uri)).is_file()
        except StorageError:
            return False

    def delete(self, uri: str) -> None:
        try:
            self._path(self._key(uri)).unlink(missing_ok=True)
        except StorageError:
            return

    # ------------------------------------------------------------- extras

    def read_bytes(self, uri: str) -> bytes:
        return self._path(self._key(uri)).read_bytes()


__all__ = ["LocalObjectStore"]
