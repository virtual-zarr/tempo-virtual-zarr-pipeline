"""Tests for granule parsing and the flatten/promote transform."""

import pathlib

import h5py
import numpy as np
import pytest
import xarray as xr
from tempo_fixtures import TINY_LAT, TINY_LON, write_tempo_granule
from virtualizarr.manifests import ManifestArray
from virtualizarr_processor.collection import CollectionConfig, load_collection
from virtualizarr_processor.granule import (
    granule_time,
    make_registry,
    open_flat_granule,
)
from virtualizarr_processor.store_template import GranuleValidationError

TIME_0 = 1471196538.0244286

# The synthetic fixtures only carry a subset of the real groups' variables.
TINY_CONFIG_UPDATE = {
    "flatten_groups": ("product", "geolocation", "support_data"),
}


@pytest.fixture()
def tiny_config() -> CollectionConfig:
    return load_collection("hcho").model_copy(update=TINY_CONFIG_UPDATE)


def granule_url(directory: pathlib.Path, **kwargs: object) -> str:
    path = write_tempo_granule(directory / "granule.nc", **kwargs)  # type: ignore[arg-type]
    return f"file://{path}"


def test_flatten_and_promote(
    tempo_granule_dir: pathlib.Path, tiny_config: CollectionConfig
) -> None:
    vds = open_flat_granule(
        granule_url(tempo_granule_dir, time_value=TIME_0), tiny_config
    )
    assert set(vds.data_vars) == {
        "weight",
        "vertical_column",
        "main_data_quality_flag",
        "solar_zenith_angle",
        "surface_pressure",
    }
    for name, var in vds.data_vars.items():
        assert var.dims == ("time", "latitude", "longitude"), name
        assert isinstance(var.data, ManifestArray), f"{name} should stay virtual"
    assert set(vds.coords) == {"time", "latitude", "longitude"}
    assert vds.attrs["collection_shortname"] == "TEMPO_HCHO_L3"
    # Variable attrs survive the flatten.
    assert vds["vertical_column"].attrs["units"] == "molecules/cm^2"
    np.testing.assert_array_equal(vds["latitude"].values, TINY_LAT)
    np.testing.assert_array_equal(vds["longitude"].values, TINY_LON)


def test_missing_group_raises(
    tempo_granule_dir: pathlib.Path, tiny_config: CollectionConfig
) -> None:
    config = tiny_config.model_copy(
        update={"flatten_groups": ("product", "qa_statistics")}
    )
    with pytest.raises(GranuleValidationError, match="qa_statistics"):
        open_flat_granule(granule_url(tempo_granule_dir, time_value=TIME_0), config)


def test_name_collision_raises(
    tempo_granule_dir: pathlib.Path, tiny_config: CollectionConfig
) -> None:
    url = granule_url(tempo_granule_dir, time_value=TIME_0)
    with h5py.File(url.removeprefix("file://"), "a") as f:
        f["product"]["weight"] = np.zeros(3, dtype="float32")
    with pytest.raises(GranuleValidationError, match="weight"):
        open_flat_granule(url, tiny_config)


def test_unpromoted_time_invariant_variable_raises(
    tempo_granule_dir: pathlib.Path, tiny_config: CollectionConfig
) -> None:
    """A data variable without the time dim that the config does not promote
    would silently freeze one scan's values under concat — reject it."""
    url = granule_url(tempo_granule_dir, time_value=TIME_0)
    with h5py.File(url.removeprefix("file://"), "a") as f:
        ds = f["support_data"].create_dataset(
            "static_map", data=np.zeros((4, 6), dtype="float32")
        )
        ds.dims[0].attach_scale(f["latitude"])
        ds.dims[1].attach_scale(f["longitude"])
    with pytest.raises(GranuleValidationError, match="static_map"):
        open_flat_granule(url, tiny_config)


def test_drop_variables(
    tempo_granule_dir: pathlib.Path, tiny_config: CollectionConfig
) -> None:
    config = tiny_config.model_copy(update={"drop_variables": ("solar_zenith_angle",)})
    vds = open_flat_granule(granule_url(tempo_granule_dir, time_value=TIME_0), config)
    assert "solar_zenith_angle" not in vds


def test_granule_time_exact(
    tempo_granule_dir: pathlib.Path, tiny_config: CollectionConfig
) -> None:
    vds = open_flat_granule(
        granule_url(tempo_granule_dir, time_value=TIME_0), tiny_config
    )
    assert granule_time(vds) == TIME_0


def test_granule_time_epoch_attr_mismatch_raises(
    tempo_granule_dir: pathlib.Path, tiny_config: CollectionConfig
) -> None:
    url = granule_url(tempo_granule_dir, time_value=TIME_0)
    with h5py.File(url.removeprefix("file://"), "a") as f:
        f.attrs["time_coverage_start_since_epoch"] = np.array([TIME_0 + 1.0])
    vds = open_flat_granule(url, tiny_config)
    with pytest.raises(GranuleValidationError, match="time_coverage_start_since_epoch"):
        granule_time(vds)


def test_granule_time_missing_epoch_attr_raises(
    tempo_granule_dir: pathlib.Path,
) -> None:
    vds = xr.Dataset(
        {"foo": (("time",), np.zeros(1))}, coords={"time": ("time", [TIME_0])}
    )
    with pytest.raises(GranuleValidationError, match="time_coverage_start_since_epoch"):
        granule_time(vds)


def test_granule_time_requires_single_step() -> None:
    vds = xr.Dataset(
        {"foo": (("time",), np.zeros(2))},
        coords={"time": ("time", [TIME_0, TIME_0 + 1])},
        attrs={"time_coverage_start_since_epoch": np.array([TIME_0])},
    )
    with pytest.raises(GranuleValidationError, match="exactly one"):
        granule_time(vds)


def test_make_registry_schemes(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = make_registry("file:///tmp/x.nc")
    store, path = registry.resolve("file:///tmp/x.nc")
    assert path == "tmp/x.nc"
    monkeypatch.setenv("EARTHDATA_TOKEN", "token123")
    registry = make_registry("https://data.asdc.earthdata.nasa.gov/a/b.nc")
    store, path = registry.resolve("https://data.asdc.earthdata.nasa.gov/a/b.nc")
    assert path == "a/b.nc"
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "y")
    registry = make_registry("s3://asdc-prod-protected/TEMPO/g.nc")
    store, path = registry.resolve("s3://asdc-prod-protected/TEMPO/g.nc")
    assert path == "TEMPO/g.nc"
    with pytest.raises(ValueError, match="scheme"):
        make_registry("ftp://nope/g.nc")


# --- Earthdata credential resolution ---


@pytest.fixture()
def no_edl_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for name in (
        "EARTHDATA_TOKEN",
        "EARTHDATA_USERNAME",
        "EARTHDATA_PASSWORD",
        "EARTHDATA_SECRET_ARN",
        "EARTHDATA_S3_CREDENTIALS_ENDPOINT",
    ):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def _secret_arn(secret: str) -> str:
    import boto3

    client = boto3.client("secretsmanager", region_name="us-east-1")
    return str(client.create_secret(Name="edl", SecretString=secret)["ARN"])


def test_earthdata_auth_resolution(no_edl_env: pytest.MonkeyPatch) -> None:
    from virtualizarr_processor.granule import _earthdata_auth

    assert _earthdata_auth() is None

    _earthdata_auth.cache_clear()
    no_edl_env.setenv("EARTHDATA_USERNAME", "user")
    no_edl_env.setenv("EARTHDATA_PASSWORD", "pass")
    assert _earthdata_auth() == ("user", "pass")

    _earthdata_auth.cache_clear()
    no_edl_env.setenv("EARTHDATA_TOKEN", "tok")  # token beats username/password
    assert _earthdata_auth() == "tok"


def test_earthdata_auth_from_secrets_manager(no_edl_env: pytest.MonkeyPatch) -> None:
    from moto import mock_aws
    from virtualizarr_processor.granule import _earthdata_auth

    no_edl_env.setenv("AWS_DEFAULT_REGION", "us-east-1")  # Lambda sets AWS_REGION
    with mock_aws():
        no_edl_env.setenv(
            "EARTHDATA_SECRET_ARN",
            _secret_arn('{"username": "user", "password": "pass"}'),
        )
        assert _earthdata_auth() == ("user", "pass")

    _earthdata_auth.cache_clear()
    with mock_aws():
        no_edl_env.setenv("EARTHDATA_SECRET_ARN", _secret_arn('{"token": "tok"}'))
        assert _earthdata_auth() == "tok"

    _earthdata_auth.cache_clear()
    with mock_aws():
        no_edl_env.setenv("EARTHDATA_SECRET_ARN", _secret_arn("bare-token"))
        assert _earthdata_auth() == "bare-token"


def test_s3_credential_provider_selection(no_edl_env: pytest.MonkeyPatch) -> None:
    from virtualizarr_processor.granule import _s3_credential_provider

    # No EDL material: ambient IAM, no provider — even for known buckets.
    assert _s3_credential_provider("asdc-prod-protected") is None

    _s3_credential_provider.cache_clear()
    no_edl_env.setenv("EARTHDATA_TOKEN", "tok")
    from virtualizarr_processor.granule import _earthdata_auth

    _earthdata_auth.cache_clear()
    # EDL material + a bucket with a known s3credentials endpoint: provider.
    assert _s3_credential_provider("asdc-prod-protected") is not None
    # Unknown bucket (tests, staging): ambient IAM.
    assert _s3_credential_provider("my-staging-bucket") is None
    # ...unless an endpoint override says otherwise.
    no_edl_env.setenv(
        "EARTHDATA_S3_CREDENTIALS_ENDPOINT", "https://example.test/s3credentials"
    )
    _s3_credential_provider.cache_clear()
    assert _s3_credential_provider("my-staging-bucket") is not None


def test_icechunk_virtual_credentials_without_edl_fall_back_to_env(
    no_edl_env: pytest.MonkeyPatch,
) -> None:
    """No EDL material: readers rely on ambient IAM, matching the worker path."""
    import icechunk
    from virtualizarr_processor.granule import icechunk_virtual_credentials

    creds = icechunk_virtual_credentials("asdc-prod-protected")
    assert isinstance(creds, icechunk.S3Credentials.FromEnv)


def test_icechunk_virtual_credentials_with_edl_are_refreshable(
    no_edl_env: pytest.MonkeyPatch,
) -> None:
    import icechunk
    from virtualizarr_processor.granule import icechunk_virtual_credentials

    no_edl_env.setenv("EARTHDATA_TOKEN", "tok")
    creds = icechunk_virtual_credentials("asdc-prod-protected")
    assert isinstance(creds, icechunk.S3Credentials.Refreshable)


def test_earthdata_fetcher_converts_provider_credentials(
    no_edl_env: pytest.MonkeyPatch,
) -> None:
    """The fetcher survives the pickling icechunk applies and maps the
    obstore credential dict onto icechunk's static credentials."""
    import pickle
    from datetime import datetime, timezone

    from virtualizarr_processor import granule

    fetcher = pickle.loads(
        pickle.dumps(granule.EarthdataIcechunkCredentialFetcher("asdc-prod-protected"))
    )

    # No EDL material resolvable at refresh time: fail with a real message.
    with pytest.raises(RuntimeError, match="asdc-prod-protected"):
        fetcher()

    expires = datetime(2026, 8, 21, 12, tzinfo=timezone.utc)
    no_edl_env.setattr(
        granule,
        "_s3_credential_provider",
        lambda bucket: lambda: {
            "access_key_id": "AKID",
            "secret_access_key": "SECRET",
            "token": "SESSION",
            "expires_at": expires,
        },
    )
    creds = fetcher()
    assert creds.access_key_id == "AKID"
    assert creds.secret_access_key == "SECRET"
    assert creds.session_token == "SESSION"
    assert creds.expires_after == expires


# --- Real-granule integration (skipped when the context data is absent) ---


def test_real_granules_flatten(real_data_dir: pathlib.Path) -> None:
    hcho_path = sorted(real_data_dir.glob("TEMPO_HCHO_L3_*.nc"))[0]
    no2_path = sorted(real_data_dir.glob("TEMPO_NO2_L3_*.nc"))[0]

    def expected_names(path: pathlib.Path, groups: tuple[str, ...]) -> set[str]:
        with h5py.File(path) as f:
            names = {"weight"}
            for group in groups:
                names |= set(f[group].keys())
        return names

    for name, path in [("hcho", hcho_path), ("no2", no2_path)]:
        config = load_collection(name)
        vds = open_flat_granule(f"file://{path}", config)
        assert set(vds.data_vars) == expected_names(path, config.flatten_groups)
        for var_name, var in vds.data_vars.items():
            assert var.dims == ("time", "latitude", "longitude"), var_name
        assert granule_time(vds) == float(vds["time"].values[0])
        assert vds.sizes["latitude"] == 2950
        assert vds.sizes["longitude"] == 7750
