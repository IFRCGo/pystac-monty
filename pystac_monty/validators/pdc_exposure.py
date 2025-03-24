from pydantic import BaseModel, Field, ConfigDict, HttpUrl
from typing import List, Optional

class ValueDetails(BaseModel):
    valueFormatted: str
    valueFormattedNoTrunc: str
    valueRounded: float
    value: float

class Capital(BaseModel):
    total: ValueDetails
    school: ValueDetails
    hospital: ValueDetails

class Population(BaseModel):
    total: ValueDetails
    vulnerable: ValueDetails
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
    total70_74: ValueDetails
    total75_79: ValueDetails
    total80_84: ValueDetails
    total85_89: ValueDetails
    total90_94: ValueDetails
    total95_99: ValueDetails
    total100AndOver: ValueDetails
    vulnerable0_14: ValueDetails
    vulnerable15_64: ValueDetails
    vulnerable65_Plus: ValueDetails

class AdminData(BaseModel):
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

class TotalByAdmin(BaseModel):
    totalByAdmin: List[AdminData]

# Example Usage:
# validated_data = TotalByAdmin(**your_json_data)
class HazardEventValidator(BaseModel):
    app_ID: int
    app_IDs: str
    autoexpire: str
    category_ID: str
    charter_Uri: str
    comment_Text: str
    create_Date: str
    creator: str
    end_Date: str
    glide_Uri: str
    hazard_ID: int
    hazard_Name: str
    last_Update: str
    latitude: float
    longitude: float
    master_Incident_ID: str
    message_ID: str
    org_ID: int
    severity_ID: str
    snc_url: HttpUrl
    start_Date: str
    status: str
    type_ID: str
    update_Date: str
    update_User: str
    product_total: str
    uuid: str
    in_Dashboard: str
    areabrief_url: Optional[str]
    description: str
    roles: List[str]
