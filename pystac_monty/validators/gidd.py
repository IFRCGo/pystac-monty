import logging
from datetime import date
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class BaseModelWithExtra(BaseModel):
    """A base model that ignores extra fields"""

    model_config = ConfigDict(extra="ignore")


class Geometry(BaseModelWithExtra):
    type: str
    coordinates: List[List[float]]  # List of [longitude, latitude] pairs


class Properties(BaseModelWithExtra):
    ID: int
    ISO3: str
    Country: str
    Geographical_region: str = Field(..., alias="Geographical region")
    Figure_cause: str = Field(..., alias="Figure cause")
    Year: int
    Figure_category: str = Field(..., alias="Figure category")
    Figure_unit: str = Field(..., alias="Figure unit")
    Reported_figures: int = Field(..., alias="Reported figures")
    Household_size: float = Field(..., alias="Household size")
    Total_figures: int = Field(..., alias="Total figures")
    Hazard_category: str = Field(..., alias="Hazard category")
    Hazard_sub_category: str = Field(..., alias="Hazard sub category")
    Hazard_type: str = Field(..., alias="Hazard type")
    Hazard_sub_type: str = Field(..., alias="Hazard sub type")
    Stock_reporting_date: date = Field(None, alias="Stock reporting date")
    Start_date: date = Field(None, alias="Start date")
    Start_date_accuracy: str = Field(None, alias="Start date accuracy")
    End_date: date = Field(None, alias="End date")
    Start_reporting_date: date = Field(None, alias="Start reporting date")
    Publishers: List[str]
    Sources: List[str]
    Sources_type: List[str] = Field(..., alias="Sources type")
    Event_ID: int = Field(..., alias="Event ID")
    Event_name: str = Field(..., alias="Event name")
    Event_cause: str = Field(..., alias="Event cause")
    Event_main_trigger: str = Field(..., alias="Event main trigger")
    Event_start_date: date = Field(..., alias="Event start date")
    Event_end_date: date = Field(..., alias="Event end date")
    Event_start_date_accuracy: str = Field(..., alias="Event start date accuracy")
    Event_end_date_accuracy: str = Field(..., alias="Event end date accuracy")
    Is_housing_destruction: str = Field(..., alias="Is housing destruction")
    Event_codes_Code_Type: List[List[str]] = Field(..., alias="Event codes (Code:Type)")
    Locations_name: List[str] = Field(..., alias="Locations name")
    Locations_accuracy: List[str] = Field(..., alias="Locations accuracy")
    Locations_type: List[str] = Field(..., alias="Locations type")
    Displacement_occurred: Optional[str] = None

    @field_validator("Figure_cause")
    def check_figure_cause(cls, value: str) -> str | None:
        if value not in ["Conflict", "Disaster", "Other"]:
            logger.error("Figure cause must be either 'Conflict' or 'Disaster'.")
            return None
        return value


class GiddValidator(BaseModelWithExtra):
    geometry: Geometry
    properties: Properties
