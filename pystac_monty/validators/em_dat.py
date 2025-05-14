import logging
from typing import Any, Optional, Union

import pandas as pd
from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class BaseModelWithExtra(BaseModel):
    model_config = ConfigDict(extra="ignore")


class Admin1(BaseModelWithExtra):
    adm1_code: int
    adm1_name: str


class Admin2(BaseModelWithExtra):
    adm2_code: int
    adm2_name: str


class EmdatDataValidator(BaseModelWithExtra):
    disno: str
    classif_key: str
    type: str | None = None
    subtype: str | None = None
    name: str | None = None
    iso: str | None
    country: str | None = None
    location: str | None = None
    magnitude: float | None = None
    magnitude_scale: str | None = None
    latitude: float | None = None
    longitude: float | None = None
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
    total_dam: float | None = None
    admin_units: Optional[list[Union[Admin1, Admin2]]]

    @field_validator("total_deaths")
    def validate_total_deaths(cls, value):
        if value and value < 0:
            raise ValueError("Total deaths cannot be negative.")
        return value

    @field_validator("*", mode="before")
    def replace_nan_with_none(cls, value: Any, info: ValidationInfo):
        # Ignore the check for admin_units as it will be an ambiguous check (being a list of dicts)
        if info.field_name in {"admin_units"}:
            if pd.isna(value):
                return None
            return value
        if pd.isna(value):
            return None
        return value
