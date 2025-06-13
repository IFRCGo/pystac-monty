import logging
from datetime import datetime
from typing import List

from pydantic import BaseModel, field_validator

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class DisasterType(BaseModel):
    # id: int
    name: str
    # summary: str
    # translation_module_original_language: str


class Country(BaseModel):
    # iso: str
    iso3: str
    # id: int
    # record_type: int
    # record_type_display: str
    # region: int
    # independent: bool
    # is_deprecated: bool
    # fdrs: Optional[str]
    # average_household_size: Optional[float]
    # society_name: str
    name: str
    # translation_module_original_language: str


class FieldReport(BaseModel):
    # status: int
    # contacts: List[Contact]
    # countries: List[Country]
    # created_at: datetime
    # updated_at: datetime
    # report_date: datetime
    # id: int
    # is_covid_report: bool
    # num_assisted: int | None
    # num_displaced: int | None
    # gov_num_dead: int | None
    # gov_num_injured: int | None
    # other_num_displaced: int | None
    # affected_pop_centres: int | None
    # gov_affected_pop_centres: int | None
    # other_affected_pop_centres: int | None
    # description: str
    # summary: str
    # translation_module_original_language: str

    num_dead: int | None
    gov_num_dead: int | None
    other_num_dead: int | None
    num_displaced: int | None
    gov_num_displaced: int | None
    other_num_displaced: int | None
    num_injured: int | None
    gov_num_injured: int | None
    other_num_injured: int | None
    num_missing: int | None
    gov_num_missing: int | None
    other_num_missing: int | None
    num_affected: int | None
    gov_num_affected: int | None
    other_num_affected: int | None
    num_assisted: int | None
    gov_num_assisted: int | None
    other_num_assisted: int | None
    num_potentially_affected: int | None
    gov_num_potentially_affected: int | None
    other_num_potentially_affected: int | None
    num_highest_risk: int | None
    gov_num_highest_risk: int | None
    other_num_highest_risk: int | None


class IFRCsourceValidator(BaseModel):
    dtype: DisasterType
    countries: List[Country]
    # num_affected: Optional[int]
    # ifrc_severity_level: int
    # ifrc_severity_level_display: str
    # glide: Optional[str]
    disaster_start_date: datetime
    # created_at: datetime
    # auto_generated: bool
    # appeals: List[Appeal]
    # is_featured: bool
    # is_featured_region: bool
    field_reports: List[FieldReport]
    # updated_at: datetime
    id: int
    # slug: Optional[str]
    # parent_event: Optional[str]
    # tab_one_title: Optional[str]
    # tab_two_title: Optional[str]
    # tab_three_title: Optional[str]
    # emergency_response_contact_email: Optional[str]
    # active_deployments: int
    name: str
    summary: str
    # translation_module_original_language: str

    # @field_validator("ifrc_severity_level")
    # def validate_severity_level(cls, value):
    #     if value not in range(0, 4):
    #         raise ValueError("Invalid severity level, must be between 0 and 3")
    #     return value

    @field_validator("countries")
    @classmethod
    def validate_countries(cls, value: list) -> list | None:
        """Validate whether the countries field is a empty list"""
        if not value:
            logger.warning("Empty country list.")
            return None
        return value
