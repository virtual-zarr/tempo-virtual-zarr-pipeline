"""Shared TEMPO L3 collection definitions for the exploration scripts.

Importable from sibling scripts because ``uv run exploration/<script>.py`` puts the
script's directory on ``sys.path``.
"""

import argparse

COLLECTIONS = {
    # TEMPO_HCHO_L3 V04, gridded formaldehyde total column
    "hcho": "C3685897141-LARC_CLOUD",
    # TEMPO_NO2_L3 V04, gridded NO2 tropospheric/stratospheric columns
    "no2": "C3685896708-LARC_CLOUD",
}
DEFAULT_COLLECTION = "hcho"


def add_collection_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--collection",
        choices=sorted(COLLECTIONS),
        default=DEFAULT_COLLECTION,
        help=f"TEMPO L3 collection to target (default {DEFAULT_COLLECTION})",
    )
    parser.add_argument(
        "--concept-id",
        help="Explicit CMR collection concept ID (overrides --collection)",
    )


def resolve_concept_id(args: argparse.Namespace) -> str:
    return args.concept_id or COLLECTIONS[args.collection]
