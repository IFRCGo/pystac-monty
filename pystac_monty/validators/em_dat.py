import logging
import math
import typing
from typing import Optional

from pydantic import BaseModel, ConfigDict, field_validator
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class BaseModelWithExtra(BaseModel):
    model_config = ConfigDict(extra="ignore")


class EmdatDataValidator(BaseModelWithExtra):
    disno: str
    classif_key: str
    # group: str
    # subgroup: str
    type: str | None = None
    subtype: str | None = None
    # external_ids: Optional[str] = None
    name: str | None = None
    iso: str | None
    country: str | None = None
    # subregion: str
    # region: str
    location: str | None = None
    # origin: Optional[str] = None
    # associated_types: Optional[str] = None
    # ofda_response: bool
    # appeal: bool
    # declaration: bool
    # aid_contribution: Optional[str] = None
    magnitude: float | None = None
    magnitude_scale: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    # river_basin: Optional[str] = None

    start_year: int | None
    start_month: int | None
    start_day: int | None
    end_year: int | None
    end_month: int | None
    end_day: int | None

    total_deaths: float | None = None
    no_injured: float | None = None
    no_affected: float | None = None
    no_homeless: float | None = None
    total_affected: float | None = None
    # reconstr_dam: Optional[float] = None
    # reconstr_dam_adj: Optional[float] = None
    # insur_dam: Optional[float] = None
    # insur_dam_adj: Optional[float] = None
    total_dam: float | None = None
    # total_dam_adj: Optional[float] = None
    # cpi: Optional[float] = None
    admin_units: list[typing.Any] | None = None
    # entry_date: Optional[str]
    # last_update: Optional[str]

    @field_validator("total_deaths")
    def validate_total_deaths(cls, value):
        if value and value < 0:
            raise ValueError("Total deaths cannot be negative.")
        return value

    @classmethod
    def validate_event(cls, data: dict) -> bool:
        """Validate the overall data item"""
        try:
            _ = cls(**data)  # This will trigger the validators
        except Exception as e:
            logger.warning(f"Validation failed: {e}")
            return False
        # If all field validators return True, we consider it valid
        return True

    @field_validator(
        "*",
        mode="before"
    )
    def replace_nan_with_none(cls, value):
        if pd.isna(value):
            return None
        return value
