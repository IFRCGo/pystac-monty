import json
import logging
import re
from datetime import date

from pydantic import BaseModel, ConfigDict, field_validator

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class BaseModelWithExtra(BaseModel):
    model_config = ConfigDict(extra="ignore")


class IDUSourceValidator(BaseModelWithExtra):
    """Source validator fields"""

    id: int
    iso3: str
    latitude: float
    longitude: float
    centroid: str  # Custom validation required
    displacement_type: str
    qualifier: str
    figure: int
    displacement_date: date
    displacement_start_date: date
    displacement_end_date: date
    year: int
    event_id: int
    event_name: str
    event_codes: str
    event_start_date: date
    event_end_date: date
    category: str
    subcategory: str
    type: str
    subtype: str
    standard_popup_text: str
    sources: str
    source_url: str
    locations_name: str

    @field_validator("latitude")
    @classmethod
    def validate_latitude(cls, value: float) -> float | None:
        """Validation for Latitude field"""
        if not (-90 <= value <= 90):
            logger.warning("Latitude must be between -90 and 90.")
            return None
        return value

    @field_validator("longitude")
    @classmethod
    def validate_longitude(cls, value: float) -> float | None:
        """Validation for Longitude field"""
        if not (-180 <= value <= 180):
            logger.warning("Longitude must be between -180 and 180.")
            return None
        return value

    @field_validator("centroid")
    @classmethod
    def validate_centroid(cls, value: str) -> str | None:
        """Validation for centroid field"""
        try:
            coords = json.loads(value)  # Parse JSON format list
            if not isinstance(coords, list) or len(coords) != 2:
                logger.warning("Centroid must be a list with two values: [latitude, longitude].")
                return None
            latitude, longitude = coords
            if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
                logger.warning("Invalid centroid coordinates.")
                return None
        except (json.JSONDecodeError, ValueError):
            logger.warning("Invalid centroid format. Must be a JSON string representing [latitude, longitude].")
            return None
        return value

    @field_validator(
        "displacement_date", "displacement_start_date", "displacement_end_date", "event_start_date", "event_end_date"
    )
    @classmethod
    def validate_date(cls, value: date) -> date | None:
        """Validation for date field"""
        if not isinstance(value, date):
            logger.warning(f"Invalid date format: {value}. Expected YYYY-MM-DD.")
            return None
        return value

    @field_validator("source_url")
    @classmethod
    def validate_source_url(cls, value: str) -> str | None:
        """Validation for source_url field"""
        url_regex = r"^(https?://)?([a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+)+)(/[^\s]*)?$"
        if not re.match(url_regex, value):
            logger.warning(f"Invalid URL: {value}")
            return None
        return value
