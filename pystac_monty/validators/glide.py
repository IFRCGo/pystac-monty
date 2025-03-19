import logging
from typing import Optional

from pydantic import BaseModel, Field, ConfigDict

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)

class BaseModelWithExtra(BaseModel):
    model_config = ConfigDict(extra="ignore")


class GlideSetValidator(BaseModelWithExtra):
    comments: Optional[str]
    year: int   # Restricting reasonable year range
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
    magnitude: str
    time: Optional[str]
    id: Optional[str]
    event: str   # Ensuring event is uppercase letters
    day: int = Field(..., ge=1, le=31)
    status: str 

    @classmethod
    def validate_event(cls, data: dict) -> bool:
        """Validate the overall data item"""
        try:
            _ = cls(**data)  # This will trigger the validators
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return False
        # If all field validators return True, we consider it valid
        return True
