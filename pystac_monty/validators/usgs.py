from curses.ascii import ETB
from attr import dataclass
from pydantic import BaseModel, HttpUrl, Field, field_validator, ConfigDict
from typing import List, Optional, Dict, Any, Union
from datetime import datetime

import logging

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
    pdl_client_version: str
    review_status: str

class ShakemapProperties(BaseModel):
    depth: float
    event_description: str
    event_type: str
    eventsource: str
    eventsourcecode: str
    eventtime: datetime
    gmice: str
    latitude: float
    longitude: float
    magnitude: float
    map_status: str
    maximum_latitude: float
    maximum_longitude: float
    maxmmi: float
    maxmmi_grid: float
    maxpga: float
    maxpga_grid: float
    maxpgv: float
    maxpgv_grid: float
    maxpsa03: float
    maxpsa03_grid: float
    maxpsa10: float
    maxpsa10_grid: float
    maxpsa30: float
    maxpsa30_grid: float
    minimum_latitude: float
    minimum_longitude: float
    pdl_client_version: str
    process_timestamp: datetime
    review_status: str
    shakemap_code_version: str
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
    #updated: int
    #tz: Optional[int] = None
    #url: HttpUrl
    detail: HttpUrl
    #felt: Optional[int] = None
    #cdi: Optional[float] = None
    mmi: Optional[float] = None
    alert: Optional[str] = None
    status: str
    tsunami: int
    sig: int
    #net: str
    code: str
    #ids: str
    #sources: str
    #types: str
    #nst: Optional[int] = None
    #dmin: Optional[float] = None
    #rms: Optional[float] = None
    #gap: Optional[float] = None
    magType: str
    type: str
    title: str
    products: Dict[str,Products]

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