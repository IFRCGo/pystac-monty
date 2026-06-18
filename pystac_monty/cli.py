#!/usr/bin/env python3
"""CLI: batch export of Monty STAC to a directory tree."""

import argparse
import logging
import sys
from pathlib import Path

from pystac_monty.sources.batch_export import BATCH_EXPORTS, run_batch


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if not BATCH_EXPORTS:
        print(
            "No batch sources registered. Use export_collected_items() from pystac_monty.exporter.",
            file=sys.stderr,
        )
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Convert local source inputs to on-disk STAC (Monty).")
    parser.add_argument("source", choices=sorted(BATCH_EXPORTS), metavar="SOURCE")
    parser.add_argument(
        "-i",
        "--input",
        type=Path,
        required=True,
        help="Source-specific input path (see the source convert_* docstring)",
    )
    parser.add_argument("-o", "--output", type=Path, required=True, help="Output directory for STAC JSON")
    args = parser.parse_args()
    run_batch(args.source, args.input, args.output)


if __name__ == "__main__":
    main()
