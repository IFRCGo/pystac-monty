import logging
from typing import Optional

from pydantic import BaseModel, ConfigDict, field_validator

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class BaseModelWithExtra(BaseModel):
    model_config = ConfigDict(extra="ignore")


class EmdatDataValidator(BaseModelWithExtra):
    disno: str
    classif_key: str
    group: str
    subgroup: str
    type: str
    subtype: str
    external_ids: Optional[str] = None
    name: Optional[str] = None
    iso: str
    country: str
    subregion: str
    region: str
    location: str
    origin: Optional[str] = None
    associated_types: Optional[str] = None
    ofda_response: bool
    appeal: bool
    declaration: bool
    aid_contribution: Optional[str] = None
    magnitude: Optional[float] = None
    magnitude_scale: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    river_basin: Optional[str] = None
    start_year: int
    start_month: int
    start_day: int
    end_year: int
    end_month: int
    end_day: int
    total_deaths: int
    no_injured: Optional[int] = None
    no_affected: Optional[int] = None
    no_homeless: Optional[int] = None
    total_affected: Optional[int] = None
    reconstr_dam: Optional[float] = None
    reconstr_dam_adj: Optional[float] = None
    insur_dam: Optional[float] = None
    insur_dam_adj: Optional[float] = None
    total_dam: Optional[float] = None
    total_dam_adj: Optional[float] = None
    cpi: Optional[float] = None
    admin_units: Optional[str] = None
    entry_date: str
    last_update: str

    @field_validator("total_deaths")
    def validate_total_deaths(cls, value):
        if value < 0:
            raise ValueError("Total deaths cannot be negative.")
        return value

    @classmethod
    def validate_event(cls, data: dict) -> bool:
        """Validate the overall data item"""
        try:
            _ = cls(**data)  # This will trigger the validators
        except Exception as e:
            logger.error(
                "Emdat validation failed",
                exc_info=True,
                # extra=log_extra(e),
            )
            return False
        # If all field validators return True, we consider it valid
        return True
