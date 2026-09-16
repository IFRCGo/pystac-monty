import itertools
import json
import logging
import math
import os
import re
import tempfile
from enum import Enum
from typing import Any, Callable, Iterable, Iterator, List, Optional

import ijson

from pystac_monty.extension import (
    MontyImpactExposureCategory,
    MontyImpactType,
)

logger = logging.getLogger(__name__)


def phrase_to_dashed(phrase: str) -> str:
    return re.sub(r"[^\w]+", "-", phrase).strip("-").lower()


def save_json_data_into_tmp_file(data: dict) -> tempfile._TemporaryFileWrapper:
    tmpfile = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
    data = json.dumps(data).encode("utf-8")
    tmpfile.write(data)
    tmpfile.close()
    return tmpfile


class IDMCUtils:
    """IDMC GIDD and IDU utils"""

    class DisplacementType(Enum):
        """Displacement Types for GIDD and IDU sources"""

        DISASTER_TYPE = "Disaster"
        CONFLICT_TYPE = "Conflict"
        OTHER_TYPE = "Other"

    # TODO: For other types e.g. FORCED_TO_FLEE, IN_RELIEF_CAMP, DESTROYED_HOUSING,
    # PARTIALLY_DESTROYED_HOUSING, UNINHABITABLE_HOUSING, RETURNS, MULTIPLE_OR_OTHER
    # Handle them later.
    """All Impact Mappings for GIDD and IDU sources"""
    mappings = {
        "evacuated": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.EVACUATED),
        "displaced": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.INTERNALLY_DISPLACED_PERSONS),
        "relocated": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.RELOCATED),
        "sheltered": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.EMERGENCY_SHELTERED),
        "homeless": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.HOMELESS),
        "affected": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.TOTAL_AFFECTED),
        "IDPs": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.INTERNALLY_DISPLACED_PERSONS),
        "Internal Displacements": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.INTERNALLY_DISPLACED_PERSONS),
        "Deaths": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.DEATH),
        "People displaced across borders": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.EXTERNALLY_DISPLACED_PERSONS),
    }

    """Utils for IDMC GIDD and IDU"""

    @staticmethod
    def hazard_codes_mapping(hazard: tuple) -> list[str]:
        """Map IDU hazards to UNDRR-ISC 2020 Hazard Codes"""
        hazard = tuple((item.lower() if item else item for item in hazard))
        hazard_mapping = {
            ("geophysical", "geophysical", "earthquake", "earthquake"): ["GH0101", "nat-geo-ear-gro", "EQ"],
            ("geophysical", "geophysical", "earthquake", "tsunami"): ["MH0705", "nat-geo-ear-tsu", "TS"],
            ("geophysical", "geophysical", "mass movement", "dry mass movement"): ["GH0300", "nat-geo-mmd-lan", "LS"],
            ("geophysical", "geophysical", "mass movement", "sinkhole"): ["GH0308", "nat-geo-mmd-sub", "OT"],
            ("geophysical", "geophysical", "volcanic activity", "volcanic activity"): ["GH0205", "nat-geo-vol-vol", "VO"],
            ("mixed disasters", "mixed disasters", "mixed disasters", "mixed disasters"): ["mix-mix-mix-mix"],
            ("weather related", "climatological", "desertification", "desertification"): ["EN0206", "nat-geo-env-des", "OT"],
            ("weather related", "climatological", "drought", "drought"): ["MH0401", "nat-cli-dro-dro", "DR"],
            ("weather related", "climatological", "erosion", "erosion"): ["GH0403", "nat-geo-env-soi", "OT"],
            ("weather related", "climatological", "salinisation", "salinization"): ["EN0303", "nat-geo-env-slr", "OT"],
            ("weather related", "climatological", "sea level rise", "sea level rise"): ["EN0303", "nat-geo-env-slr", "OT"],
            ("weather related", "climatological", "wildfire", "wildfire"): ["EN0205", "nat-cli-wil-wil", "WF"],
            ("weather related", "hydrological", "flood", "dam release flood"): ["TL0009", "tec-mis-col-col", "FL"],
            ("weather related", "hydrological", "flood", "flood"): ["MH0600", "nat-hyd-flo-flo", "FL"],
            ("weather related", "hydrological", "mass movement", "avalanche"): ["MH0801", "nat-geo-mmd-ava", "AV"],
            ("weather related", "hydrological", "mass movement", "landslide/wet mass movement"): [
                "GH0300",
                "nat-geo-mmd-lan",
                "LS",
            ],
            ("weather related", "hydrological", "wave action", "rogue wave"): ["MH0701", "nat-hyd-wav-rog", "OT"],
            ("weather related", "meteorological", "extreme temperature", "cold wave"): ["MH0502", "nat-met-ext-col", "CW"],
            ("weather related", "meteorological", "extreme temperature", "heat wave"): ["MH0501", "nat-met-ext-hea", "HT"],
            ("weather related", "meteorological", "storm", "hailstorm"): ["MH0404", "nat-met-sto-hai", "ST"],
            ("weather related", "meteorological", "storm", "sand/dust storm"): ["MH0201", "nat-met-sto-san", "VW"],
            ("weather related", "meteorological", "storm", "storm surge"): ["MH0703", "nat-met-sto-sur", "SS"],
            ("weather related", "meteorological", "storm", "storm"): ["MH0301", "nat-met-sto-sto", "VW"],
            ("weather related", "meteorological", "storm", "tornado"): ["MH0305", "nat-met-sto-tor", "TO"],
            ("weather related", "meteorological", "storm", "typhoon/hurricane/cyclone"): ["MH0309", "nat-met-sto-tro", "TC"],
            ("weather related", "meteorological", "storm", "winter storm/blizzard"): ["MH0403", "nat-met-sto-bli", "OT"],
        }
        if hazard not in hazard_mapping:
            raise KeyError(f"Hazard {hazard} not found.")
        return hazard_mapping.get(hazard, [])


def partition_json_array_by_key(
    filepath: str,
    ijson_prefix: str,
    key_fn: Callable[[dict], Any],
    target_bucket_bytes: int = 64 * 1024 * 1024,
    min_buckets: int = 16,
    max_buckets: int = 1024,
) -> List[str]:
    """Split a large top-level JSON array into on-disk NDJSON buckets, hashed by ``key_fn``.

    Records that need to be grouped by key (e.g. all figures for one event) can then be
    processed a bucket at a time instead of loading or sorting the whole array in memory.
    A single ``jq sort_by(...)`` (or any full-array sort) needs several times the file's
    size in RAM because it has to build the whole document as its in-memory tree before it
    can sort anything -- that scales with total file size no matter how the result is
    consumed afterwards. Bucketing bounds peak memory to one bucket's size instead, and
    ijson parses the source file incrementally rather than loading it whole.
    """
    file_size = os.path.getsize(filepath)
    num_buckets = min(max_buckets, max(min_buckets, math.ceil(file_size / target_bucket_bytes)))

    buckets = [tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".ndjson") for _ in range(num_buckets)]
    try:
        with open(filepath, "rb") as f:
            # use_float avoids ijson's default Decimal, which json.dumps can't serialize below
            for item in ijson.items(f, ijson_prefix, use_float=True):
                bucket = buckets[hash(key_fn(item)) % num_buckets]
                bucket.write(json.dumps(item))
                bucket.write("\n")
    finally:
        for bucket in buckets:
            bucket.close()

    return [bucket.name for bucket in buckets]


def iter_grouped_json_buckets(
    bucket_paths: Iterable[str],
    key_fn: Callable[[dict], Any],
    filter_fn: Optional[Callable[[dict], bool]] = None,
) -> Iterator[List[dict]]:
    """Read back buckets written by :func:`partition_json_array_by_key`, grouping each
    bucket's records by ``key_fn``. Each bucket is deleted once fully processed.

    Grouping compares ``str(key_fn(item))`` rather than the raw key so a stray record
    missing the key field (falling back to e.g. ``""``) can't blow up the sort by mixing
    incomparable types with the rest -- record order across groups doesn't matter to
    callers, only that every record sharing a key ends up in the same group.
    """
    for path in bucket_paths:
        try:
            with open(path) as f:
                items = [json.loads(line) for line in f]
            if filter_fn is not None:
                items = [item for item in items if filter_fn(item)]

            def sort_key(item: dict) -> str:
                return str(key_fn(item))

            items.sort(key=sort_key)
            for _, group in itertools.groupby(items, key=sort_key):
                yield list(group)
        finally:
            os.unlink(path)
