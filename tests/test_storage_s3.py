"""Integration tests for :class:`S3ObjectStore`.

Skipped unless ``STRALIS_TEST_S3_ENDPOINT_URL`` (and friends) are set, e.g.
against a local MinIO.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path

import pytest

from stralis.errors import StorageError
from stralis.storage import S3ObjectStore

ENDPOINT_URL = os.environ.get("STRALIS_TEST_S3_ENDPOINT_URL")
ACCESS_KEY = os.environ.get("STRALIS_TEST_S3_ACCESS_KEY")
SECRET_KEY = os.environ.get("STRALIS_TEST_S3_SECRET_KEY")
BUCKET = os.environ.get("STRALIS_TEST_S3_BUCKET", "stralis-test")

pytestmark = pytest.mark.skipif(
    not (ENDPOINT_URL and ACCESS_KEY and SECRET_KEY),
    reason="STRALIS_TEST_S3_* env vars not set",
)


@pytest.fixture
def store() -> S3ObjectStore:
    assert ENDPOINT_URL and ACCESS_KEY and SECRET_KEY
    return S3ObjectStore(
        ENDPOINT_URL,
        bucket=BUCKET,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
    )


@pytest.fixture
def key_prefix() -> str:
    return f"tests/{uuid.uuid4().hex}"


def test_put_get_roundtrip(store: S3ObjectStore, key_prefix: str, tmp_path: Path) -> None:
    payload = b"s3-roundtrip-\x00\x01\x02" * 1024
    src = tmp_path / "src.bin"
    src.write_bytes(payload)

    ref = store.put_file(f"{key_prefix}/clip.bin", src, content_type="application/octet-stream")
    assert ref.uri.endswith(f"{key_prefix}/clip.bin")
    assert ref.size == len(payload)
    assert ref.digest == hashlib.sha256(payload).hexdigest()

    dest = tmp_path / "out.bin"
    got = store.get(ref.uri, dest)

    assert dest.read_bytes() == payload
    assert got.digest == ref.digest
    assert got.size == len(payload)
    assert got.name == "clip.bin"

    store.delete(ref.uri)


def test_exists_and_delete(store: S3ObjectStore, key_prefix: str, tmp_path: Path) -> None:
    src = tmp_path / "f.bin"
    src.write_bytes(b"data")
    ref = store.put_file(f"{key_prefix}/f.bin", src)

    assert store.exists(ref.uri) is True

    store.delete(ref.uri)
    assert store.exists(ref.uri) is False

    # Deleting something absent must not raise.
    store.delete(ref.uri)


def test_missing_object_not_exists(store: S3ObjectStore, key_prefix: str) -> None:
    assert store.exists(f"s3://{BUCKET}/{key_prefix}/missing.bin") is False


def test_foreign_bucket_rejected(store: S3ObjectStore, tmp_path: Path) -> None:
    with pytest.raises(StorageError):
        store.get("s3://some-other-bucket/key.bin", tmp_path / "out.bin")
