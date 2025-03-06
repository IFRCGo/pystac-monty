import logging
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, field_validator

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class BaseModelWithExtra(BaseModel):
    model_config = ConfigDict(extra="ignore")


class IBTracsdataValidator(BaseModelWithExtra):
    SID: str
    SEASON: str  # this should be an integer
    BASIN: str
    NAME: str
    ISO_TIME: datetime
    LAT: float  # = Field(..., ge=-90, le=90)  # Latitude range validation
    LON: float  # = Field(...,ge=-180, le=180)  # Longitude range validation
    WMO_WIND: str  # = Field(...,ge=0)  # Wind speed should be non-negative
    WMO_PRES: str  # = Field(ge=800, le=1100)  # Reasonable pressure range in hPa
    USA_WIND: str  # = Field(ge=0)
    USA_PRES: str  # = Field(ge=800, le=1100)
    TRACK_TYPE: str
    DIST2LAND: Optional[int]  # = Field(ge=0)  # Distance to land should be non-negative
    LANDFALL: Optional[str]  # = Field(ge=0, le=1) # Should be 0 or 1
    USA_SSHS: Optional[str]
    USA_STATUS: Optional[str]

    @field_validator("BASIN")
    def validate_basin(cls, value: str):
        if value not in ["NA", "SA", "EP", "SP", "WP", "SI", "NI"]:
            logger.error("Invalid basin code.")
            return False
        return value

    @classmethod
    def validate_event(cls, data: dict):
        try:
            validated_data = cls(**data)  # This will trigger the validators
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return None
        # If all field validators return True, we consider it valid
        return validated_data
