import logging
import re
from copy import deepcopy
from datetime import datetime

import requests
from pystac import Item

from pystac_monty.sources.common import PROCESSING_SCHEMA_URI, PROCESSING_SOFTWARE_NAME

logger = logging.getLogger(__name__)

ISO3_PATTERN = re.compile(r"^[A-Z]{3}$")
BLOCK_PATTERN = re.compile(r"^\d+$")


def validate_correlation_id(correlation_id: str, hazard_code: str):
    """Validate correlation id"""
    parts = correlation_id.split("-")

    if len(parts) != 6:
        raise ValueError(f"Invalid correlation_id format: {correlation_id}. Should have 6 parts")

    date_str, country_code, block_id, hazard, episode, _ = parts

    # ---- Date validation ----
    try:
        datetime.strptime(date_str, "%Y%m%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format (YYYYMMDD): {date_str}") from e

    # ---- Country code ----
    if not ISO3_PATTERN.match(country_code):
        raise ValueError(f"Invalid country code: {country_code}")

    # ---- Block ID ----
    if not BLOCK_PATTERN.match(block_id):
        raise ValueError(f"Invalid block id: {block_id}")

    # ---- Hazard code ----
    if hazard != hazard_code:
        raise ValueError("Hazard codes do not match")

    # ---- Episode number ----
    if not episode.isdigit() or int(episode) < 1:
        raise ValueError(f"Invalid episode number: {episode}")


def request_for_schema(url: str):
    """Validate if the schema exists"""
    resp = requests.get(url=url)
    assert resp.status_code == 200


def assert_processing_extension_fields(item: Item) -> None:
    """Assert *item* carries the automated ``processing:version``/``processing:software`` stamp."""
    assert PROCESSING_SCHEMA_URI in (item.stac_extensions or []), f"{item.id} missing {PROCESSING_SCHEMA_URI}"
    assert "processing:version" in item.properties, f"{item.id} missing processing:version"
    software = item.properties.get("processing:software")
    assert isinstance(software, dict) and PROCESSING_SOFTWARE_NAME in software, (
        f"{item.id} missing processing:software[{PROCESSING_SOFTWARE_NAME}]"
    )


def normalize_processing_version_fields(item_doc: dict) -> dict:
    """Return a copy of *item_doc* with its stamped ``processing:version``/``processing:software`` blanked out.

    STAC Collection documents don't carry the processing stamp and are returned
    as-is (deep-copied).
    """
    if item_doc.get("type") != "Feature":
        return deepcopy(item_doc)

    properties = item_doc.get("properties", {})
    assert "processing:version" in properties, f"{item_doc.get('id')} missing processing:version"
    software = properties.get("processing:software")
    assert isinstance(software, dict) and PROCESSING_SOFTWARE_NAME in software, (
        f"{item_doc.get('id')} missing processing:software[{PROCESSING_SOFTWARE_NAME}]"
    )

    normalized = deepcopy(item_doc)
    normalized_properties = normalized["properties"]
    normalized_properties["processing:version"] = "IGNORED"
    normalized_properties["processing:software"][PROCESSING_SOFTWARE_NAME] = "IGNORED"
    return normalized
