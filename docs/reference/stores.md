# The virtual stores

Each store presents one collection as a single dataset: all variables from
every group, flattened into the root group (titiler-multidim needs this
layout), concatenated along `time`.

Layout and encoding:

- Every 3-D variable has per-granule dims `(time, latitude, longitude)` =
  (1, 2950, 7750) and keeps the source files' shuffle + deflate(1) codecs.
- Chunks are (1, 738, 1938) for float64 and (1, 984, 2584) for everything
  else.
- The 1-D coordinates are contiguous in the source netCDF-4, so they're
  loaded and stored as native chunks — `[native]` below.
- Everything else is virtual references into the source files.

**`TEMPO_HCHO_L3` V04**:

```
/                                       dims: time (append dim), latitude=2950, longitude=7750
├── time         (time)                 float64, seconds since 1980-01-06T00:00:00Z  [native]
├── latitude     (latitude)             float32  [native]
├── longitude    (longitude)            float32  [native]
├── weight       (time, latitude, longitude)  float32  # promoted; stored per scan without a time dim
├── vertical_column                          float64
├── vertical_column_uncertainty              float64
├── main_data_quality_flag                   int16
├── solar_zenith_angle                       float32
├── viewing_zenith_angle                     float32
├── relative_azimuth_angle                   float32
├── num_vertical_column_samples              int32
├── min_vertical_column_sample               float64
├── max_vertical_column_sample               float64
├── fitted_slant_column                      float64
├── fitted_slant_column_uncertainty          float64
├── albedo                                   float32
├── amf                                      float32
├── eff_cloud_fraction                       float32
├── amf_cloud_fraction                       float32
├── amf_cloud_pressure                       float32
├── surface_pressure                         float32
├── terrain_height                           int16
├── snow_ice_fraction                        float32
└── pbl_height                               int16
```

**`TEMPO_NO2_L3` V04** has the same coordinates and
layout, with the NO2 variable set: `vertical_column_troposphere`,
`vertical_column_stratosphere`, `vertical_column_total` and their
uncertainties, twelve `qa_statistics` min/max/count variables,
`amf_total`/`amf_troposphere`/`amf_stratosphere`, `tropopause_pressure`, and
the same geolocation and ancillary variables as HCHO. 36 data variables in
all.

Things worth knowing before you build on these:

- The two time axes are independent. A handful of scans exist in only one
  collection, and both grow separately — that's why each collection gets its
  own repository. Joint analysis aligns at read time.
- `latitude`/`longitude` are bit-identical between the two products and fixed
  across scans.
- `weight` varies per scan but the source files store it without a time
  dimension. The pipeline promotes it to `(time, latitude, longitude)` at
  ingest; without that, concatenation would silently keep only the first
  scan's values.
- Production stores reference `s3://asdc-prod-protected/...` in us-west-2.
  Readers authorize the virtual chunk container with temporary credentials
  from <https://data.asdc.earthdata.nasa.gov/s3credentials>. EDL-authed HTTPS
  also works but CloudFront rate-limits it.
