import logging
from typing import Optional

from pydantic import BaseModel

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class GlideSetValidator(BaseModel):
    comments: str
    year: int
    docid: int
    latitude: float
    homeless: int
    source: str
    idsource: str
    killed: int
    affected: int
    duration: int
    number: str
    injured: int
    month: int
    geocode: str
    location: str
    magnitude: str
    time: Optional[str] = None
    id: Optional[str] = None
    event: str
    day: int
    status: str
    longitude: float

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
