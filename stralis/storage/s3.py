"""S3-compatible implementation of the :class:`ObjectStore` port.

Works against MinIO (path-style addressing) as well as real AWS S3. Media
bytes are streamed in bounded chunks so multi-GB files never load fully into
memory; only sha256 digests and ``s3://`` URIs travel onward (claim-check).
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from stralis.errors import StorageError
from stralis.models import ArtifactRef

_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MiB
_NOT_FOUND_CODES = frozenset({"404", "404 Not Found", "NoSuchKey", "NoSuchBucket"})


def _error_code(exc: ClientError) -> str:
    error = exc.response.get("Error", {})
    code = error.get("Code")
    if code:
        return str(code)
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return str(status) if status is not None else ""


def _is_not_found(exc: ClientError) -> bool:
    if _error_code(exc) in _NOT_FOUND_CODES:
        return True
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return status == 404


def _hash_stream(handle: Any) -> tuple[str, int]:
    """Return ``(sha256_hex, byte_count)`` reading ``handle`` in chunks."""
    digest = hashlib.sha256()
    size = 0
    while True:
        chunk = handle.read(_CHUNK_SIZE)
        if not chunk:
            break
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


class S3ObjectStore:
    """Blob store on an S3-compatible endpoint (MinIO in dev)."""

    def __init__(
        self,
        endpoint_url: str | None,
        *,
        bucket: str = "stralis",
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
    ) -> None:
        self._bucket = bucket
        self._region = region
        self._bucket_ready = False
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=Config(
                s3={"addressing_style": "path"},
                signature_version="s3v4",
            ),
        )

    # -- bucket lifecycle --------------------------------------------------

    def ensure_bucket(self) -> None:
        """Head the bucket, creating it on first use (lazy, once per store)."""
        if self._bucket_ready:
            return
        try:
            self._client.head_bucket(Bucket=self._bucket)
        except ClientError as exc:
            if not _is_not_found(exc):
                raise StorageError(
                    f"cannot head bucket {self._bucket!r}: {exc}"
                ) from exc
            self._create_bucket()
        self._bucket_ready = True

    def _create_bucket(self) -> None:
        try:
            if self._region == "us-east-1":
                # us-east-1 must NOT carry a LocationConstraint.
                self._client.create_bucket(Bucket=self._bucket)
            else:
                self._client.create_bucket(
                    Bucket=self._bucket,
                    CreateBucketConfiguration={"LocationConstraint": self._region},
                )
        except ClientError as exc:
            if _error_code(exc) in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                return
            raise StorageError(
                f"cannot create bucket {self._bucket!r}: {exc}"
            ) from exc

    # -- ObjectStore -------------------------------------------------------

    def put_file(
        self,
        key: str,
        path: Path,
        *,
        name: str | None = None,
        content_type: str | None = None,
    ) -> ArtifactRef:
        path = Path(path)
        self.ensure_bucket()
        with path.open("rb") as handle:
            digest, size = _hash_stream(handle)
            handle.seek(0)
            extra: dict[str, str] = {}
            if content_type is not None:
                extra["ContentType"] = content_type
            self._client.upload_fileobj(
                handle,
                self._bucket,
                key,
                ExtraArgs=extra or None,
            )
        return ArtifactRef(
            name=name or key.rsplit("/", 1)[-1],
            uri=self._uri(key),
            digest=digest,
            size=size,
            content_type=content_type,
        )

    def get(self, uri: str, dest: Path) -> ArtifactRef:
        bucket, key = self._parse_uri(uri)
        self.ensure_bucket()
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._client.download_file(bucket, key, str(dest))
        except ClientError as exc:
            raise StorageError(f"cannot download {uri!r}: {exc}") from exc
        with dest.open("rb") as handle:
            digest, size = _hash_stream(handle)
        content_type: str | None = None
        try:
            head = self._client.head_object(Bucket=bucket, Key=key)
            content_type = head.get("ContentType")
        except ClientError:
            content_type = None
        return ArtifactRef(
            name=key.rsplit("/", 1)[-1],
            uri=uri,
            digest=digest,
            size=size,
            content_type=content_type,
        )

    def exists(self, uri: str) -> bool:
        bucket, key = self._parse_uri(uri)
        try:
            self._client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            if _is_not_found(exc):
                return False
            raise StorageError(f"cannot head {uri!r}: {exc}") from exc

    def delete(self, uri: str) -> None:
        bucket, key = self._parse_uri(uri)
        try:
            self._client.delete_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            if _is_not_found(exc):
                return
            raise StorageError(f"cannot delete {uri!r}: {exc}") from exc

    # -- helpers -----------------------------------------------------------

    def _uri(self, key: str) -> str:
        return f"s3://{self._bucket}/{key}"

    def _parse_uri(self, uri: str) -> tuple[str, str]:
        prefix = "s3://"
        if not uri or not uri.startswith(prefix):
            raise StorageError(f"malformed object URI (expected {prefix}...): {uri!r}")
        bucket, _, key = uri[len(prefix):].partition("/")
        if not bucket or not key:
            raise StorageError(f"malformed object URI: {uri!r}")
        if bucket != self._bucket:
            raise StorageError(
                f"foreign bucket {bucket!r} (this store serves {self._bucket!r})"
            )
        return bucket, key
