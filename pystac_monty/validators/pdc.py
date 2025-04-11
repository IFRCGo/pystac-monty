from pydantic import BaseModel, ConfigDict
from typing import List, Optional


class BaseModelWithExtra(BaseModel):
    model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)

class ValueDetails(BaseModelWithExtra):
    value: float | None

class Capital(BaseModelWithExtra):
    total: ValueDetails
    school: ValueDetails
    hospital: ValueDetails

class Population(BaseModelWithExtra):
    total: ValueDetails
    households: ValueDetails
    total0_14: ValueDetails
    total15_64: ValueDetails
    total65_Plus: ValueDetails
    total0_4: ValueDetails
    total5_9: ValueDetails
    total10_14: ValueDetails
    total15_19: ValueDetails
    total20_24: ValueDetails
    total25_29: ValueDetails
    total30_34: ValueDetails
    total35_39: ValueDetails
    total40_44: ValueDetails
    total45_49: ValueDetails
    total50_54: ValueDetails
    total55_59: ValueDetails
    total60_64: ValueDetails
    total65_69: ValueDetails

class AdminData(BaseModelWithExtra):
    country: str
    capital: Capital
    foodNeeds: ValueDetails
    foodNeedsUnit: str
    waterNeeds: ValueDetails
    waterNeedsUnit: str
    wasteNeeds: ValueDetails
    wasteNeedsUnit: str
    shelterNeeds: ValueDetails
    shelterNeedsUnit: str
    population: Population
    admin0: str
    admin1: Optional[str]
    admin2: Optional[str]

class ExposureDetailValidator(BaseModelWithExtra):
    totalByAdmin: List[AdminData]
    totalByCountry: List[AdminData]

# Example Usage:
# validated_data = TotalByAdmin(**your_json_data)
class HazardEventValidator(BaseModelWithExtra):
    category_ID: str
    create_Date: str
    end_Date: str
    hazard_ID: int
    hazard_Name: str
    latitude: float
    longitude: float
    severity_ID: str
    snc_url: str | None
    start_Date: str
    status: str
    type_ID: str
    uuid: str
    description: str