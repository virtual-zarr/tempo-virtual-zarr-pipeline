# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "earthaccess>=0.14",
#     "h5py",
#     "aiohttp",
# ]
# ///
"""Print the full HDF5 structure and metadata of an original TEMPO L3 file, via h5py.

Opens one granule of the selected collection (HCHO by default, ``--collection no2``
for NO2; by default the first granule by start date) remotely with h5py over
an Earthdata-authed fsspec file and walks the native HDF5 hierarchy, printing for
every group and dataset: shape, maxshape, dtype, chunk layout, the raw HDF5 filter
pipeline (filter IDs, names, and client data — i.e. the actual codecs in the file),
fill value, storage size vs. logical size (compression ratio), dimension scales,
and all attributes at file, group, and dataset level.

Requires Earthdata Login credentials in ~/.netrc.

Usage:
    uv run exploration/inspect_granule_metadata.py
    uv run exploration/inspect_granule_metadata.py --collection no2
    uv run exploration/inspect_granule_metadata.py --index 100   # 101st by start date
    uv run exploration/inspect_granule_metadata.py --url https://data.asdc.earthdata.nasa.gov/...nc
"""

import argparse
import sys
from typing import Any

import earthaccess
import h5py
from tempo_collections import add_collection_argument, resolve_concept_id

INDENT = "    "

LAYOUTS = {
    h5py.h5d.COMPACT: "COMPACT",
    h5py.h5d.CONTIGUOUS: "CONTIGUOUS",
    h5py.h5d.CHUNKED: "CHUNKED",
    getattr(h5py.h5d, "VIRTUAL", -1): "VIRTUAL",
}

# https://support.hdfgroup.org/services/contributions.html
FILTER_NAMES = {
    1: "deflate (zlib)",
    2: "shuffle",
    3: "fletcher32",
    4: "szip",
    5: "nbit",
    6: "scaleoffset",
    32000: "lzf",
    32001: "blosc",
    32004: "lz4",
    32008: "bitshuffle",
    32015: "zstd",
}


def open_remote(url: str | None, index: int, concept_id: str) -> Any:
    if url:
        return earthaccess.open([url])[0]
    granules = earthaccess.search_data(
        concept_id=concept_id, count=index + 1, sort_key="start_date"
    )
    granule = granules[index]
    print(f"Inspecting {granule.data_links(access='external')[0]}\n")
    return earthaccess.open([granule])[0]


def format_value(value: object) -> str:
    text = str(value)
    if len(text) > 500:
        text = text[:500] + f"...[{len(text)} chars total]"
    return text.replace("\n", "\n" + INDENT * 4)


def print_attrs(obj: h5py.Group | h5py.Dataset, depth: int) -> None:
    pad = INDENT * depth
    if obj.attrs:
        print(f"{pad}attributes:")
        for key, value in obj.attrs.items():
            print(f"{pad}{INDENT}{key} = {format_value(value)}")


def describe_dtype(ds: h5py.Dataset) -> str:
    type_id = ds.id.get_type()
    order = (
        {h5py.h5t.ORDER_LE: "little-endian", h5py.h5t.ORDER_BE: "big-endian"}.get(
            type_id.get_order(), ""
        )
        if isinstance(type_id, (h5py.h5t.TypeIntegerID, h5py.h5t.TypeFloatID))
        else ""
    )
    string_info = h5py.check_string_dtype(ds.dtype)
    if string_info:
        return f"string (encoding={string_info.encoding}, length={string_info.length})"
    return f"{ds.dtype} ({order})" if order else str(ds.dtype)


def print_dataset(name: str, ds: h5py.Dataset, depth: int) -> None:
    pad = INDENT * depth
    print(f"{pad}DATASET {name}")
    pad2 = pad + INDENT
    print(f"{pad2}shape: {ds.shape}, maxshape: {ds.maxshape}")
    print(f"{pad2}dtype: {describe_dtype(ds)}")

    dcpl = ds.id.get_create_plist()
    layout = LAYOUTS.get(dcpl.get_layout(), f"unknown({dcpl.get_layout()})")
    print(f"{pad2}layout: {layout}" + (f", chunks: {ds.chunks}" if ds.chunks else ""))

    nfilters = dcpl.get_nfilters()
    if nfilters:
        print(f"{pad2}filter pipeline ({nfilters}):")
        for i in range(nfilters):
            code, flags, values, fname = dcpl.get_filter(i)
            label = FILTER_NAMES.get(code, fname.decode(errors="replace") or "unknown")
            optional = " [optional]" if flags & h5py.h5z.FLAG_OPTIONAL else ""
            print(f"{pad2}{INDENT}id={code} {label}{optional} cd_values={values}")
    else:
        print(f"{pad2}filter pipeline: none")

    print(f"{pad2}fill value: {ds.fillvalue!r}")

    logical = ds.size * ds.dtype.itemsize
    storage = ds.id.get_storage_size()
    if storage and logical:
        print(
            f"{pad2}storage: {storage:,} B for {logical:,} B logical "
            f"(ratio {logical / storage:.1f}x)"
        )
    else:
        print(f"{pad2}storage: {storage:,} B")

    scales = [
        (dim_index, scale_name or "(unnamed)")
        for dim_index, dim in enumerate(ds.dims)
        for scale_name in [s[0] for s in dim.items()]
    ]
    if scales:
        print(f"{pad2}dimension scales: {scales}")

    print_attrs(ds, depth + 1)


def walk(group: h5py.Group, path: str, depth: int) -> None:
    print(f"{INDENT * depth}GROUP {path}")
    print_attrs(group, depth + 1)
    datasets = [(n, o) for n, o in group.items() if isinstance(o, h5py.Dataset)]
    subgroups = [(n, o) for n, o in group.items() if isinstance(o, h5py.Group)]
    for name, ds in datasets:
        print_dataset(name, ds, depth + 1)
    print()
    for name, subgroup in subgroups:
        walk(subgroup, f"{path.rstrip('/')}/{name}", depth)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", help="Granule URL to inspect (skips CMR search)")
    parser.add_argument(
        "--index", type=int, default=0, help="Granule index by start date"
    )
    add_collection_argument(parser)
    args = parser.parse_args()

    earthaccess.login(strategy="netrc")
    fobj = open_remote(args.url, args.index, resolve_concept_id(args))

    with h5py.File(fobj, "r") as f:
        print(f"HDF5 file: {f.filename}")
        print(f"libver bounds: {f.libver}, userblock: {f.userblock_size} B\n")
        walk(f["/"], "/", 0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
