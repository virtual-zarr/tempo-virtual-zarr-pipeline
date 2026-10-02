# Development

```bash
./scripts/setup.sh          # set up the environment
uv run pytest               # tests
uv run ruff check . && uv run ruff format --check .
uv run mypy
uv run --env-file .env_hcho --env-file .env.local cdk synth   # review infrastructure before deploying
uv run --env-file .env_hcho --env-file .env.local cdk deploy
```

The `Processor` class in
[`virtualizarr_processor/processor.py`](https://github.com/virtual-zarr/tempo-virtual-zarr-pipeline/blob/main/lambda/virtualizarr-processor/virtualizarr_processor/processor.py)
is the sole (non-polymorphic) implementation. The template's synthetic
reference implementation lives on as `tests/stub_processor.py` and still
exercises the generic fork/merge mechanics.

## Exploration

`exploration/` holds standalone [PEP 723](https://peps.python.org/pep-0723/)
scripts used to characterize the source data and to build test stores. Run
them with `uv run exploration/<script>.py`; each declares its own
dependencies. All take `--collection {hcho,no2}` (default `hcho`) plus
`--concept-id` for any other collection, and need Earthdata Login credentials
in `~/.netrc`.

- [`tempo_dataset_info.py`](https://github.com/virtual-zarr/tempo-virtual-zarr-pipeline/blob/main/exploration/tempo_dataset_info.py) — CMR/UMM-C
  collection report: extents, granule count, distribution info, most recent
  granule.
- [`inspect_granule_metadata.py`](https://github.com/virtual-zarr/tempo-virtual-zarr-pipeline/blob/main/exploration/inspect_granule_metadata.py) —
  HDF5 structure dump of granules: chunk layouts, codecs, fill values,
  attributes, and a cross-granule comparison of what varies.
- [`combine_twenty_spread_virtual.py`](https://github.com/virtual-zarr/tempo-virtual-zarr-pipeline/blob/main/exploration/combine_twenty_spread_virtual.py) —
  virtualizes N granules spread across the collection's temporal extent,
  combines them in an in-memory Icechunk store, and reads data back to prove
  the path end to end.
- [`build_titiler_test_store.py`](https://github.com/virtual-zarr/tempo-virtual-zarr-pipeline/blob/main/exploration/build_titiler_test_store.py) —
  small local store (12 recent granules) for titiler-multidim smoke tests.
- [`build_s3_test_store.py`](https://github.com/virtual-zarr/tempo-virtual-zarr-pipeline/blob/main/exploration/build_s3_test_store.py) —
  realistic S3-hosted store (100 recent granules, credential-less virtual
  chunk container); run on in-region compute.

The production inventory builder
([`scripts/build_backfill_inventory.py`](https://github.com/virtual-zarr/tempo-virtual-zarr-pipeline/blob/main/scripts/build_backfill_inventory.py),
described in [Backfill inventory](pipeline.md#backfill-inventory)) lives in `scripts/` with the other production tooling; it
follows the same PEP 723 + `--collection` conventions.

### titiler-multidim smoke test takeaways (2026-08-06)

A 12-granule store per collection was served through titiler-multidim's
`feat/http-virtual-chunk-auth` branch; all checked endpoints (`/variables`,
`/info`, `/tiles`, `/point`) passed for both collections. What carries
forward:

- Flat-at-root layout is required: titiler-xarray does not walk nested groups
  or resolve group-inherited coordinates. The production stores use it.
- Clients must always select a time step (`sel=time=...&sel_method=nearest`).
  A multi-time variable reaching the renderer fails with "Source data must be
  1 band".
- Tile latency is dominated by per-request virtual-chunk fetches and scales
  with the area a tile covers: z2 tiles took 3–7 s, z4/z6 tiles 1.5–2.7 s
  over HTTPS. A dataset/session cache is advisable in production, and
  Lambda's 1024 file-descriptor limit is a real constraint under tile bursts.
- Per-scan coverage is inherently partial (single east-west scans,
  daylight-only retrieval, occasional short rapid-scan slices). Portal time
  sliders and "latest available" defaults need to account for it.
- Map clients should set `noWrap`/`maxBounds`; tiles crossing ±180 hit an
  antimeridian error upstream in rio-tiler.
- CMR publication behavior shaped the forward-processing design: publication
  order routinely diverges from scan order (the ~43% figure quoted under
  [Forward processing](pipeline.md#forward-processing) came from an August 2026 14-day
  window, with the historical archive back-filling at ~1,000 granules/week),
  republication is rare (~0.3%) and short-window, and median production lag
  is ~3 h.
