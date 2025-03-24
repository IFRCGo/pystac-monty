import logging
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, field_validator

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class DisasterType(BaseModel):
    id: int
    name: str
    summary: str
    translation_module_original_language: str


class Country(BaseModel):
    iso: str
    iso3: str
    id: int
    record_type: int
    record_type_display: str
    region: int
    independent: bool
    is_deprecated: bool
    fdrs: Optional[str]
    average_household_size: Optional[float]
    society_name: str
    name: str
    translation_module_original_language: str


class Appeal(BaseModel):
    aid: str
    num_beneficiaries: int
    amount_requested: float
    code: str
    amount_funded: float
    status: int
    status_display: str
    start_date: datetime
    atype: int
    atype_display: str
    id: int
    translation_module_original_language: str


class Contact(BaseModel):
    ctype: str
    name: str
    title: str
    email: str
    phone: str
    id: int


class FieldReport(BaseModel):
    status: int
    contacts: List[Contact]
    countries: List[Country]
    created_at: datetime
    updated_at: datetime
    report_date: datetime
    id: int
    is_covid_report: bool
    num_assisted: Optional[int]
    num_displaced: Optional[int]
    gov_num_dead: Optional[int]
    gov_num_injured: Optional[int]
    other_num_displaced: Optional[int]
    affected_pop_centres: Optional[str]
    gov_affected_pop_centres: Optional[str]
    other_affected_pop_centres: Optional[str]
    description: str
    summary: str
    translation_module_original_language: str


class IFRCsourceValidator(BaseModel):
    dtype: DisasterType
    countries: List[Country]
    num_affected: Optional[int]
    ifrc_severity_level: int
    ifrc_severity_level_display: str
    glide: Optional[str]
    disaster_start_date: datetime
    created_at: datetime
    auto_generated: bool
    appeals: List[Appeal]
    is_featured: bool
    is_featured_region: bool
    field_reports: List[FieldReport]
    updated_at: datetime
    id: int
    slug: Optional[str]
    parent_event: Optional[str]
    tab_one_title: Optional[str]
    tab_two_title: Optional[str]
    tab_three_title: Optional[str]
    emergency_response_contact_email: Optional[str]
    active_deployments: int
    name: str
    summary: str
    translation_module_original_language: str

    @field_validator("ifrc_severity_level")
    def validate_severity_level(cls, value):
        if value not in range(0, 4):
            raise ValueError("Invalid severity level, must be between 0 and 3")
        return value

    @classmethod
    def validate_event(cls, data: dict) -> bool:
        """Validate the overall data item"""
        try:
            _ = cls(**data)  # This will trigger the validators
        except Exception as e:
            logger.error(
                "Ifrc validation failed",
                exc_info=True,
                # extra=e,
            )
            return False
        # If all field validators return True, we consider it valid
        return True
