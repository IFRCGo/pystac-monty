import logging
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class BaseModelWithExtra(BaseModel):
    model_config = ConfigDict(extra="ignore")


class GlideSetValidator(BaseModelWithExtra):
    comments: Optional[str]
    year: int  # Restricting reasonable year range
    docid: int
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    homeless: int = Field(..., ge=0)
    source: Optional[str]
    idsource: Optional[str]
    killed: int = Field(..., ge=0)
    affected: int = Field(..., ge=0)
    duration: int = Field(..., ge=0)
    number: str
    injured: int = Field(..., ge=0)
    month: int = Field(..., ge=1, le=12)
    geocode: str
    location: Optional[str]
    magnitude: Optional[str]
    time: Optional[str]
    id: Optional[str]
    event: str  # Ensuring event is uppercase letters
    day: int = Field(..., ge=1, le=31)
    status: str

    @field_validator("event")
    def validate_enum(cls, value):
        if value not in [
            "EQ",
            "TC",
            "FL",
            "DR",
            "WF",
            "VO",
            "TS",
            "CW",
            "EP",
            "EC",
            "ET",
            "FR",
            "FF",
            "HT",
            "IN",
            "LS",
            "MS",
            "ST",
            "SL",
            "AV",
            "SS",
            "AC",
            "TO",
            "VW",
            "WV",
        ]:
            raise ValueError(f"Event type {value} is not valid.")
        return value
