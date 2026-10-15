# The virtual stores

There is one store per collection. Each store presents its collection as a
single dataset: all variables from every group, flattened into the root
group and concatenated along `time`. titiler-multidim needs the flat layout.

## Where they are

The pipeline writes each store to its own prefix in a private bucket. Public
copies live on [Source Cooperative](https://source.coop/pangeo/tempo-virtual-icechunk):

| Collection | Public copy |
|---|---|
| NO2 | `s3://us-west-2.opendata.source.coop/pangeo/tempo-virtual-icechunk/tempo/no2/v04/` |
| HCHO | `s3://us-west-2.opendata.source.coop/pangeo/tempo-virtual-icechunk/tempo/hcho/v04/` |

The public copies are refreshed by hand, so they are only as current as the
last run of [the mirror runbook](runbook-mirror-to-source-coop.md). A zip of
each copy sits beside it as `v04.zip`. It unpacks to a directory that opens
with `icechunk.local_filesystem_storage`.

## Layout and encoding

- Every 3-D variable has per-granule dims `(time, latitude, longitude)` of
  (1, 2950, 7750) and keeps the source files' shuffle + deflate(1) codecs.
- Chunks are (1, 738, 1938) for float64 and (1, 984, 2584) for everything
  else.
- The 1-D coordinates are contiguous in the source netCDF-4 files, so they
  are loaded and stored as native chunks. They are marked `[native]` below.
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

**`TEMPO_NO2_L3` V04** has the same coordinates and layout, with the NO2
variable set: `vertical_column_troposphere`, `vertical_column_stratosphere`,
`vertical_column_total` and their uncertainties, twelve `qa_statistics`
min/max/count variables, `amf_total`, `amf_troposphere`, `amf_stratosphere`,
`tropopause_pressure`, and the same geolocation and ancillary variables as
HCHO. That is 36 data variables in all.

## Things to know before you build on them

- The two time axes are independent. A handful of scans exist in only one
  collection, and both axes grow separately. That is why each collection
  gets its own repository. Joint analysis aligns the two at read time.
- `latitude` and `longitude` are bit-identical between the two products and
  fixed across scans.
- `weight` varies per scan, but the source files store it without a time
  dimension. The pipeline promotes it to `(time, latitude, longitude)` at
  ingest. Without that, concatenation would silently keep only the first
  scan's values.
- `main_data_quality_flag` is 0 for normal retrievals. The quickstarts show
  a typical screen.

## Reading the chunks

The store holds references, not bytes. Reading data means fetching byte
ranges from ASDC's `asdc-prod-protected` bucket, which needs an
[Earthdata Login](https://urs.earthdata.nasa.gov/) account. There are two
ways in.

**In AWS us-west-2.** Exchange the login for temporary S3 credentials at
<https://data.asdc.earthdata.nasa.gov/s3credentials> and read the bucket
directly. The credentials work only from us-west-2. This is the fast path.
[Quickstart: in AWS us-west-2](../examples/quickstart.ipynb) does it with
earthaccess-auth.

**From anywhere.** The DAAC's HTTPS distribution at
`https://data.asdc.earthdata.nasa.gov/asdc-prod-protected/` serves the same
keys as the bucket. It accepts an Earthdata bearer token, so it works from a
laptop. It sits behind CloudFront, which rate-limits it, so expect it to be
slower. [Quickstart: from anywhere](../examples/quickstart-https.ipynb)
shows how.

The second path works because references are stored relative to a virtual
chunk container named `asdc`, as `vcc://asdc/TEMPO/...`. The store's own
config points that container at the S3 bucket. A reader can point a
container with the same name somewhere else, and every reference follows.
Stores built before this change hold absolute `s3://` references, which only
the S3 container can resolve. Migrate them with
[the relativize runbook](runbook-relativize-virtual-refs.md).
