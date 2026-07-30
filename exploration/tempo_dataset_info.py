# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "earthaccess>=0.14",
# ]
# ///
"""Report everything CMR knows about a TEMPO collection via earthaccess.

Targets the selected TEMPO L3 collection (HCHO by default, ``--collection no2``
for NO2, or any collection via --concept-id / --short-name).
Prints the collection-level metadata shown on Earthdata Search (description,
citation, platforms/instruments, spatial/temporal extents, data partner, data
state, processing level, science keywords, granule count), plus the direct-S3
distribution information and a sample granule, which matter for building the
virtual Zarr pipeline.

Usage:
    uv run exploration/tempo_dataset_info.py
    uv run exploration/tempo_dataset_info.py --collection no2
    uv run exploration/tempo_dataset_info.py --concept-id C3685897141-LARC_CLOUD
    uv run exploration/tempo_dataset_info.py --short-name TEMPO_HCHO_L3 --version V04
    uv run exploration/tempo_dataset_info.py --json  # dump raw UMM-C + meta as JSON
"""

import argparse
import json
import sys
import textwrap

import earthaccess

from tempo_collections import add_collection_argument, resolve_concept_id


def wrap(text: str, indent: str = "    ") -> str:
    return textwrap.fill(
        " ".join(text.split()), width=96, initial_indent=indent, subsequent_indent=indent
    )


def section(title: str) -> None:
    print(f"\n{title}\n{'-' * len(title)}")


def format_citation(citation: dict) -> str:
    parts = []
    for key in ("Creator", "ReleaseDate", "Title", "Version", "Publisher", "OtherCitationDetails"):
        value = citation.get(key)
        if value:
            parts.append(f"{key}: {value}")
    return "; ".join(parts) if parts else json.dumps(citation)


def temporal_extent(umm: dict) -> list[str]:
    lines = []
    for extent in umm.get("TemporalExtents", []):
        for range_ in extent.get("RangeDateTimes", []):
            begin = range_.get("BeginningDateTime", "?")
            end = range_.get("EndingDateTime")
            if end is None and extent.get("EndsAtPresentFlag"):
                end = "Present"
            lines.append(f"Temporal extent: {begin} to {end or '?'}")
        resolution = extent.get("TemporalResolution")
        if resolution:
            lines.append(f"Temporal resolution: {resolution.get('Value')} {resolution.get('Unit')}")
    return lines


def science_keywords(umm: dict) -> list[str]:
    paths = []
    for keyword in umm.get("ScienceKeywords", []):
        levels = ("Category", "Topic", "Term", "VariableLevel1", "VariableLevel2", "VariableLevel3")
        path = " > ".join(keyword[level] for level in levels if keyword.get(level))
        if path not in paths:
            paths.append(path)
    return paths


def print_report(collection: dict, granule_count: int | None, sample: dict | None) -> None:
    umm = collection["umm"]
    meta = collection["meta"]

    print(f"{umm.get('ShortName')} {umm.get('Version', '')} — {umm.get('EntryTitle')}")

    section("Description")
    print(wrap(umm.get("Abstract", "(no abstract)")))

    section("Citation")
    doi = umm.get("DOI", {}).get("DOI")
    if doi:
        print(f"    DOI: {doi}")
    for citation in umm.get("CollectionCitations", []):
        print(wrap(format_citation(citation)))

    section("Product summary")
    platforms = umm.get("Platforms", [])
    print(f"    Platforms: {', '.join(p.get('ShortName', '?') for p in platforms)}")
    instruments = [i for p in platforms for i in p.get("Instruments", [])]
    print(f"    Instruments: {', '.join(i.get('ShortName', '?') for i in instruments)}")
    print(f"    Projects: {', '.join(p.get('ShortName', '?') for p in umm.get('Projects', []))}")
    locations = [k.get("Type") or k.get("Category", "?") for k in umm.get("LocationKeywords", [])]
    print(f"    Location: {', '.join(locations)}")
    spatial = umm.get("SpatialExtent", {})
    geometry = spatial.get("HorizontalSpatialDomain", {}).get("Geometry", {})
    print(f"    Coordinate system: {geometry.get('CoordinateSystem')}")
    for rect in geometry.get("BoundingRectangles", []):
        print(
            f"    Bounding rectangle: lon [{rect.get('WestBoundingCoordinate')}, "
            f"{rect.get('EastBoundingCoordinate')}], lat [{rect.get('SouthBoundingCoordinate')}, "
            f"{rect.get('NorthBoundingCoordinate')}]"
        )
    print(f"    Granule spatial representation: {spatial.get('GranuleSpatialRepresentation')}")
    for line in temporal_extent(umm):
        print(f"    {line}")
    for center in umm.get("DataCenters", []):
        roles = ", ".join(center.get("Roles", []))
        print(f"    Data center ({roles}): {center.get('LongName') or center.get('ShortName')}")
    print(f"    Concept ID: {meta.get('concept-id')}")
    print(f"    Data state: {umm.get('CollectionProgress')}")
    if granule_count is not None:
        print(f"    Number of granules: {granule_count}")
    print(f"    Processing level: {umm.get('ProcessingLevel', {}).get('Id')}")
    for date in umm.get("DataDates", []):
        print(f"    Data date ({date.get('Type')}): {date.get('Date')}")
    print(f"    CMR revision date: {meta.get('revision-date')}")
    print(f"    Science keywords: {'; '.join(science_keywords(umm))}")

    archive_info = umm.get("ArchiveAndDistributionInformation", {})
    file_info = archive_info.get("FileDistributionInformation", [])
    if file_info:
        section("File distribution")
        for entry in file_info:
            print(f"    {json.dumps(entry)}")

    direct = umm.get("DirectDistributionInformation")
    if direct:
        section("Direct S3 distribution")
        print(f"    Region: {direct.get('Region')}")
        for prefix in direct.get("S3BucketAndObjectPrefixNames", []):
            print(f"    S3 prefix: {prefix}")
        print(f"    Credentials API: {direct.get('S3CredentialsAPIEndpoint')}")

    if sample:
        section("Sample granule (most recent)")
        sample_umm = sample.get("umm", {})
        print(f"    GranuleUR: {sample_umm.get('GranuleUR')}")
        for temporal in [sample_umm.get("TemporalExtent", {}).get("RangeDateTime", {})]:
            print(
                f"    Temporal: {temporal.get('BeginningDateTime')} to "
                f"{temporal.get('EndingDateTime')}"
            )
        for granule_size in sample_umm.get("DataGranule", {}).get("ArchiveAndDistributionInformation", []):
            print(f"    File: {granule_size.get('Name')} ({granule_size.get('Size')} {granule_size.get('SizeUnit')})")
        for url in sample_umm.get("RelatedUrls", []):
            if url.get("Type") == "GET DATA":
                print(f"    Data URL: {url.get('URL')}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    add_collection_argument(parser)
    parser.add_argument("--short-name", help="Collection short name, e.g. TEMPO_HCHO_L3")
    parser.add_argument("--version", help="Collection version, e.g. V04 (with --short-name)")
    parser.add_argument("--json", action="store_true", help="Dump raw UMM-C + meta JSON instead")
    args = parser.parse_args()

    query: dict = {}
    if args.short_name:
        query["short_name"] = args.short_name
        if args.version:
            query["version"] = args.version
    else:
        query["concept_id"] = resolve_concept_id(args)

    collections = earthaccess.search_datasets(**query)
    if not collections:
        print(f"No collections found for {query}", file=sys.stderr)
        return 1

    for collection in collections:
        concept_id = collection["meta"]["concept-id"]
        try:
            granule_count = earthaccess.granule_query().concept_id(concept_id).hits()
        except Exception as error:
            print(f"Granule count query failed: {error}", file=sys.stderr)
            granule_count = None
        try:
            granules = earthaccess.search_data(concept_id=concept_id, count=1, sort_key="-start_date")
            sample = granules[0] if granules else None
        except Exception as error:
            print(f"Sample granule query failed: {error}", file=sys.stderr)
            sample = None

        if args.json:
            print(json.dumps({"meta": collection["meta"], "umm": collection["umm"],
                              "granule_count": granule_count}, indent=2))
        else:
            print_report(collection, granule_count, sample)
    return 0


if __name__ == "__main__":
    sys.exit(main())
