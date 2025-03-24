import logging
from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class BaseModelWithExtra(BaseModel):
    model_config = ConfigDict(extra="ignore")


class ContentDetail(BaseModel):
    url: str


class ContentItem(BaseModel):
    download_pin_thumbnail: ContentDetail = Field(alias="download/pin-thumbnail.png")


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
    contents: ContentItem


class Products(BaseModelWithExtra):
    shakemap: List[Shakemap]


class BaseProperties(BaseModel):
    mag: float
    place: str
    time: int
    felt: Optional[int] = None
    mmi: Optional[float] = None
    alert: Optional[str] = None
    status: str
    tsunami: int
    sig: int
    code: str
    magType: str
    type: str
    title: str
    products: Products


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


class CountryFatalities(BaseModel):
    country_code: str
    rates: List[float]
    fatalities: int


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
