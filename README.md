# tempo-virtual-zarr-pipeline

This pipeline builds virtual Zarr / Icechunk stores for TEMPO Level 3 gridded
products. It backs data delivery for the AIR4US portal
([NASA-IMPACT/veda-odd#438](https://github.com/NASA-IMPACT/veda-odd/issues/438))
and targets two collections hosted at NASA ASDC:

| Collection | Concept ID | DOI |
|---|---|---|
| `TEMPO_HCHO_L3` V04 — gridded formaldehyde total column | `C3685897141-LARC_CLOUD` | [10.5067/IS-40e/TEMPO/HCHO_L3.004](https://doi.org/10.5067/IS-40e/TEMPO/HCHO_L3.004) |
| `TEMPO_NO2_L3` V04 — gridded NO2 tropospheric and stratospheric columns | `C3685896708-LARC_CLOUD` | [10.5067/IS-40E/TEMPO/NO2_L3.004](https://doi.org/10.5067/IS-40E/TEMPO/NO2_L3.004) |

The repo was instantiated from the
[virtualizarr-data-pipelines](https://github.com/developmentseed/virtualizarr-data-pipelines)
template, which provides the AWS CDK infrastructure. Each collection gets its
own Icechunk repository, deployed as a separate instance of the same stack.
Improvements that aren't TEMPO-specific belong in the template, not here.

## Documentation

The design, deployment, operations, and runbooks are documented in
[`docs/`](docs/index.md), built as a site with
`uv run --group docs mkdocs serve`.
