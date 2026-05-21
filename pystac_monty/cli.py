#!/usr/bin/env python3
"""CLI: batch export of Monty STAC to a directory tree (where a source implements it)."""

import argparse
import logging
from pathlib import Path

from pystac_monty.exporter import BATCH_EXPORTS, run_batch


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    epilog = (
        "Only sources registered in BATCH_EXPORTS (see exporter.py) are available. "
        "Other sources use the library: build a MontyDataTransformer, collect items, then "
        "export_monty_items_to_role_subcatalogs() or export_collected_items() in exporter."
    )
    parser = argparse.ArgumentParser(
        description="Convert local source inputs to on-disk STAC (Monty).",
        epilog=epilog,
    )
    parser.add_argument("source", choices=sorted(BATCH_EXPORTS), metavar="SOURCE", help="Registered source name")
    parser.add_argument(
        "-i",
        "--input",
        type=Path,
        required=True,
        help="Source-specific input path (directory, file, etc.; see each source's convert_* docstring)",
    )
    parser.add_argument("-o", "--output", type=Path, required=True, help="Output directory for STAC JSON")

    args = parser.parse_args()
    run_batch(args.source, args.input, args.output)


if __name__ == "__main__":
    main()
