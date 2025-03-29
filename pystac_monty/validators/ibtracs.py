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
    SEASON: str | None  # this could be an integer
    BASIN: str | None
    NAME: str | None
    ISO_TIME: datetime | None
    LAT: float  # = Field(..., ge=-90, le=90)  # Latitude range validation
    LON: float  # = Field(...,ge=-180, le=180)  # Longitude range validation
    WMO_WIND: str | None  # = Field(...,ge=0)  # Wind speed should be non-negative
    WMO_PRES: str | None  # = Field(ge=800, le=1100)  # Reasonable pressure range in hPa
    USA_WIND: str | None  # = Field(ge=0)
    USA_PRES: str | None  # = Field(ge=800, le=1100)
    TRACK_TYPE: str
    DIST2LAND: Optional[int]  # = Field(ge=0)  # Distance to land should be non-negative
    LANDFALL: Optional[str]  # = Field(ge=0, le=1) # Should be 0 or 1
    USA_SSHS: Optional[str]
    USA_STATUS: Optional[str]

    @field_validator("BASIN")
    def validate_basin(cls, value: str):
        if value not in ["NA", "SA", "EP", "SP", "WP", "SI", "NI"]:
            # FIXME: Add log extra
            logger.warning("Invalid basin code.")
            return False
        return value

    @field_validator("SID")
    def validate_sid(cls, value: str):
        if value == " ":
            # FIXME: Add log extra
            logger.warning("Invalid SID")
            return False
        return value
