"""Store routing and retry policy for the inventory header reads.

No network: store classes are substituted through the ``_s3_store_cls`` /
``_https_store_cls`` indirections, and the credential provider is stubbed.
"""

import build_backfill_inventory as bbi
import pytest


@pytest.fixture(autouse=True)
def token_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EARTHDATA_TOKEN", "test-token")
    # The store builders are lru_cached; a cached real store from another
    # test run must not leak into these assertions.
    bbi._store_for_s3.cache_clear()
    bbi._store_for_https.cache_clear()


def test_s3_url_routes_to_s3store_with_edl_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = {}
    sentinel = object()

    class FakeS3Store:
        @classmethod
        def from_url(cls, base: str, credential_provider: object = None) -> object:
            captured["base"] = base
            captured["provider"] = credential_provider
            return cls()

    monkeypatch.setattr(bbi, "_s3_store_cls", lambda: FakeS3Store)
    monkeypatch.setattr(
        "virtualizarr_processor.granule.s3_credential_provider",
        lambda bucket: sentinel if bucket == "some-bucket" else None,
    )

    store, path = bbi._store_and_path("s3://some-bucket/TEMPO/a/b.nc")
    assert isinstance(store, FakeS3Store)
    assert captured["base"] == "s3://some-bucket"
    assert captured["provider"] is sentinel
    assert path == "TEMPO/a/b.nc"


def test_https_url_routes_to_httpstore_with_bearer_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = {}

    class FakeHTTPStore:
        @classmethod
        def from_url(cls, base: str, client_options: dict | None = None) -> object:
            captured["base"] = base
            captured["options"] = client_options
            return cls()

    monkeypatch.setattr(bbi, "_https_store_cls", lambda: FakeHTTPStore)

    store, path = bbi._store_and_path("https://host.example/prefix/g.nc")
    assert isinstance(store, FakeHTTPStore)
    assert captured["base"] == "https://host.example"
    assert captured["options"]["default_headers"]["Authorization"] == (
        "Bearer test-token"
    )
    assert path == "prefix/g.nc"


def test_other_schemes_are_rejected() -> None:
    with pytest.raises(bbi.InventoryError, match="scheme"):
        bbi._store_and_path("file:///tmp/g.nc")


def test_permanent_auth_error_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 401/403 is configuration, not transient: one attempt, no sleeps."""
    monkeypatch.setattr(
        "build_backfill_inventory.time_module.sleep",
        lambda _: pytest.fail("must not sleep/retry on an auth error"),
    )
    attempts = []

    def unauthorized(url: str) -> float:
        attempts.append(url)
        raise RuntimeError("HTTP status client error (401 Unauthorized)")

    with pytest.raises(RuntimeError, match="401"):
        bbi._with_retries(unauthorized, "s3://bucket/g.nc")
    assert len(attempts) == 1


def test_transient_error_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("build_backfill_inventory.time_module.sleep", lambda _: None)
    attempts = []

    def flaky(url: str) -> float:
        attempts.append(url)
        raise OSError("connection reset")

    with pytest.raises(OSError):
        bbi._with_retries(flaky, "s3://bucket/g.nc")
    assert len(attempts) == bbi.READ_ATTEMPTS
