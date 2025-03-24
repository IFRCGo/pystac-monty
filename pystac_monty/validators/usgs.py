import logging
from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class BaseModelWithExtra(BaseModel):
    model_config = ConfigDict(extra="ignore")


class ContentItem(BaseModel):
    contentType: str
    lastModified: int
    length: int
    url: str


class LossProperties(BaseModel):
    alertlevel: str
    depth: str
    eventsource: str
    eventsourcecode: str
    eventtime: datetime
    latitude: str
    longitude: str
    magnitude: str
    maxmmi: str
    pdl_client_version: str = Field(alias="pdl-client-version")
    review_status: str = Field(alias="review-status")


class ShakemapProperties(BaseModel):
    depth: float
    event_description: str = Field(alias="event-description")
    event_type: str = Field(alias="event-type")
    eventsource: str
    eventsourcecode: str
    eventtime: datetime
    gmice: str
    latitude: float
    longitude: float
    magnitude: float
    map_status: str = Field(alias="map-status")
    maximum_latitude: float = Field(alias="maximum-latitude")
    maximum_longitude: float = Field(alias="maximum-longitude")
    maxmmi: float
    maxmmi_grid: float = Field(alias="maxmmi-grid")
    maxpga: float
    maxpga_grid: float = Field(alias="maxpga-grid")
    maxpgv: float
    maxpgv_grid: float = Field(alias="maxpgv-grid")
    maxpsa03: float
    maxpsa03_grid: float = Field(alias="maxpsa03-grid")
    maxpsa10: float
    maxpsa10_grid: float = Field(alias="maxpsa10-grid")
    maxpsa30: float
    maxpsa30_grid: float = Field(alias="maxpsa30-grid")
    minimum_latitude: float = Field(alias="minimum-latitude")
    minimum_longitude: float = Field(alias="minimum-longitude")
    pdl_client_version: str = Field(alias="pdl-client-version")
    process_timestamp: datetime = Field(alias="process-timestamp")
    review_status: str = Field(alias="review-status")
    shakemap_code_version: str = Field(alias="shakemap-code-version")
    version: int


class Losspager(BaseModel):
    indexid: int
    indexTime: int
    id: str
    type: str
    code: str
    source: str
    updateTime: int
    status: str
    properties: LossProperties
    preferredWeight: int
    contents: Dict[str, ContentItem]


class Shakemap(BaseModel):
    indexid: int
    indexTime: int
    id: str
    type: str
    code: str
    source: str
    updateTime: int
    status: str
    properties: ShakemapProperties
    preferredWeight: int
    contents: Dict[str, ContentItem]


class Products(BaseModelWithExtra):
    losspager: List[Losspager]
    shakemap: List[Shakemap]


class BaseProperties(BaseModel):
    mag: float
    place: str
    time: int
    # updated: int
    # tz: Optional[int] = None
    # url: HttpUrl
    detail: HttpUrl
    # felt: Optional[int] = None
    # cdi: Optional[float] = None
    mmi: Optional[float] = None
    alert: Optional[str] = None
    status: str
    tsunami: int
    sig: int
    # net: str
    code: str
    # ids: str
    # sources: str
    # types: str
    # nst: Optional[int] = None
    # dmin: Optional[float] = None
    # rms: Optional[float] = None
    # gap: Optional[float] = None
    magType: str
    type: str
    title: str
    products: Dict[str, Products]


class Geometry(BaseModel):
    type: str
    coordinates: List[float]

    @field_validator("coordinates")
    def validate_coordinates(cls, value):
        if len(value) != 3:
            logger.error("Coordinates must have exactly three elements (longitude, latitude, depth)")
        return value


class USGSValidator(BaseModel):
    type: str
    properties: BaseProperties
    geometry: Geometry
    id: str

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


class CountryFatalities(BaseModel):
    country_code: str
    rates: List[float]


class CountryDollars(BaseModel):
    country_code: str
    rates: List[float]
    us_dollars: int


class EmpiricalFatality(BaseModel):
    total_fatalities: int
    country_fatalities: List[CountryFatalities]


class EmpiricalEconomic(BaseModel):
    total_dollars: int
    country_dollars: List[CountryDollars]


class EmpiricalValidator(BaseModelWithExtra):
    empirical_fatality: EmpiricalFatality
    empirical_economic: EmpiricalEconomic
