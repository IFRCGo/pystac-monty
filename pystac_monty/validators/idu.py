from pydantic import BaseModel, field_validator, model_validator
from typing import Optional
from datetime import date, datetime
import re
import json


class IDUSourceValidator(BaseModel):
    id: int
    country: str
    iso3: str
    latitude: float
    longitude: float
    centroid: str  # Custom validation required
    role: str
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
    event_code_types: str
    event_start_date: date
    event_end_date: date
    category: str
    subcategory: str
    type: str
    subtype: str
    standard_popup_text: str
    standard_info_text: str
    old_id: Optional[int] = None
    sources: str
    source_url: str
    locations_name: str
    locations_coordinates: str
    locations_accuracy: str
    locations_type: str
    displacement_occurred: str
    created_at: str  # Keeping it as a string to validate separately

    @field_validator("latitude")
    @classmethod
    def validate_latitude(cls, value: float) -> float:
        if not (-90 <= value <= 90):
            raise ValueError("Latitude must be between -90 and 90.")
        return value

    @field_validator("longitude")
    @classmethod
    def validate_longitude(cls, value: float) -> float:
        if not (-180 <= value <= 180):
            raise ValueError("Longitude must be between -180 and 180.")
        return value

    @field_validator("centroid")
    @classmethod
    def validate_centroid(cls, value: str) -> str:
        try:
            coords = json.loads(value)  # Parse JSON format list
            if not isinstance(coords, list) or len(coords) != 2:
                raise ValueError("Centroid must be a list with two values: [latitude, longitude].")
            latitude, longitude = coords
            if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
                raise ValueError("Invalid centroid coordinates.")
        except (json.JSONDecodeError, ValueError):
            raise ValueError("Invalid centroid format. Must be a JSON string representing [latitude, longitude].")
        return value

    @field_validator("displacement_date", "displacement_start_date", "displacement_end_date", "event_start_date", "event_end_date")
    @classmethod
    def validate_date(cls, value: date) -> date:
        if not isinstance(value, date):
            raise ValueError(f"Invalid date format: {value}. Expected YYYY-MM-DD.")
        return value

    @field_validator("source_url")
    @classmethod
    def validate_source_url(cls, value: str) -> str:
        url_regex = r'^(https?://)?([a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+)+)(/[^\s]*)?$'
        if not re.match(url_regex, value):
            raise ValueError(f"Invalid URL: {value}")
        return value

    @field_validator("locations_coordinates")
    @classmethod
    def validate_locations_coordinates(cls, value: str) -> str:
        try:
            lat, lon = map(float, value.split(","))
            if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                raise ValueError("Invalid coordinates format. Expected 'latitude,longitude'.")
        except ValueError:
            raise ValueError(f"Invalid locations_coordinates: {value}")
        return value

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))  # Handle UTC "Z" format
        except ValueError:
            raise ValueError(f"Invalid datetime format: {value}. Expected ISO 8601.")
        return value

    @model_validator(mode="after")
    def validate_date_range(self):
        if self.displacement_start_date > self.displacement_end_date:
            raise ValueError("displacement_start_date cannot be after displacement_end_date.")
        return self
