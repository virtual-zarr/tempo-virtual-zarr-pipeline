"""The synthetic granule builder is faithful where the pipeline depends on it."""

import pathlib

import h5py
import numpy as np
from tempo_fixtures import expected_vertical_column, write_tempo_granule


def test_fixture_matches_real_file_invariants(tempo_granule_dir: pathlib.Path) -> None:
    time_value = 1471196538.0244286
    path = write_tempo_granule(tempo_granule_dir / "g0.nc", time_value=time_value)
    with h5py.File(path) as f:
        assert f["time"][0] == time_value
        # As in the real files: the epoch attr equals /time[0] bit-exactly.
        assert float(f.attrs["time_coverage_start_since_epoch"][0]) == time_value
        assert f["weight"].shape == (4, 6)  # per-scan, no time dimension
        vc = f["product/vertical_column"][:]
        np.testing.assert_array_equal(vc, expected_vertical_column(time_value))
        assert f["product/vertical_column"].compression == "gzip"
