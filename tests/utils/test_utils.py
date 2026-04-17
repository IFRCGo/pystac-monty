import logging
import re
from datetime import datetime

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
