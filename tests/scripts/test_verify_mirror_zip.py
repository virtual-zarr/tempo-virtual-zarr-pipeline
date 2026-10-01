"""Tests for the mirror zip equivalence check.

The file layer runs against moto; the store layer compares two local
repositories, since Icechunk's own S3 client cannot see moto.
"""

import shutil
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import icechunk
import numpy as np
import pytest
import verify_mirror_zip
import zarr
from test_mirror_to_source_coop import SRC_BUCKET, SRC_PREFIX, build_repo, mock_s3, run


@pytest.fixture()
def mirrored(tmp_path: Path) -> Iterator[tuple[Any, zipfile.ZipFile]]:
    """A moto client holding the source store, and the zip mirrored from it."""
    build_repo(tmp_path / "source")
    with mock_s3(tmp_path / "source") as client:
        path = run(client, tmp_path / "copy", upload_copy=False)
        assert path is not None
        with zipfile.ZipFile(path) as archive:
            yield client, archive


def repos(tmp_path: Path, archive: zipfile.ZipFile) -> tuple[Any, Any]:
    archive.extractall(tmp_path / "unpacked")
    return tuple(
        icechunk.Repository.open(
            icechunk.local_filesystem_storage(str(tmp_path / name))
        )
        for name in ("source", "unpacked")
    )


def files(client: Any, archive: zipfile.ZipFile, **kwargs: Any) -> list[str]:
    return verify_mirror_zip.compare_files(
        client, SRC_BUCKET, SRC_PREFIX, archive, **kwargs
    )


def test_a_fresh_mirror_is_equivalent(
    tmp_path: Path, mirrored: tuple[Any, zipfile.ZipFile]
) -> None:
    client, archive = mirrored
    assert files(client, archive) == []
    assert files(client, archive, quick=True) == []
    assert verify_mirror_zip.compare_repos(*repos(tmp_path, archive)) == []


def test_file_differences_are_reported(mirrored: tuple[Any, zipfile.ZipFile]) -> None:
    client, archive = mirrored
    chunk = next(n for n in archive.namelist() if n.startswith("chunks/"))
    manifest = next(n for n in archive.namelist() if n.startswith("manifests/"))
    original = client.get_object(Bucket=SRC_BUCKET, Key=SRC_PREFIX + chunk)[
        "Body"
    ].read()
    flipped = bytes([original[0] ^ 1]) + original[1:]
    client.put_object(Bucket=SRC_BUCKET, Key=SRC_PREFIX + chunk, Body=flipped)
    client.put_object(Bucket=SRC_BUCKET, Key=SRC_PREFIX + manifest, Body=b"short")

    size = archive.getinfo(manifest).file_size
    assert files(client, archive, quick=True) == [
        f"{manifest}: {size} bytes in the zip, 5 in the store"
    ]
    client.delete_object(Bucket=SRC_BUCKET, Key=SRC_PREFIX + manifest)
    assert files(client, archive, quick=True) == [
        f"{manifest}: in the zip, not in the store"
    ]
    client.put_object(
        Bucket=SRC_BUCKET, Key=SRC_PREFIX + manifest, Body=archive.read(manifest)
    )
    assert files(client, archive) == [f"{chunk}: bytes differ"]


def test_store_differences_are_reported(
    tmp_path: Path, mirrored: tuple[Any, zipfile.ZipFile]
) -> None:
    source, mirror = repos(tmp_path, mirrored[1])

    # The store moves on: reported, but the shared snapshot still compares.
    session = source.writable_session("main")
    zarr.open_array(session.store, path="x")[:] = np.arange(1000) + 9
    session.commit("commit 3")
    assert verify_mirror_zip.compare_repos(source, mirror) == [
        "the store's main is 1 commit(s) ahead of the zip"
    ]

    # A zip from some other store.
    shutil.copytree(tmp_path / "unpacked", tmp_path / "other")
    stranger = icechunk.Repository.open(
        icechunk.local_filesystem_storage(str(tmp_path / "other"))
    )
    session = stranger.writable_session("main")
    zarr.open_array(session.store, path="x").attrs["note"] = "edited"
    session.commit("diverge")
    tip = stranger.lookup_branch("main")
    assert verify_mirror_zip.compare_repos(source, stranger) == [
        f"the zip's main ({tip}) is not in the store's main history"
    ]
