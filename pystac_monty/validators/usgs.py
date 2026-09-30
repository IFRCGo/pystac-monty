import logging
from typing import List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class BaseModelWithExtra(BaseModel):
    model_config = ConfigDict(extra="ignore")


class ContentDetail(BaseModelWithExtra):
    url: str
    content_type: Optional[str] = Field(default=None, alias="contentType")


class Product(BaseModelWithExtra):
    contents: dict[str, ContentDetail] = Field(default_factory=dict)


class ShakemapProperties(BaseModel):
    depth: float
    maximum_latitude: float = Field(alias="maximum-latitude")
    maximum_longitude: float = Field(alias="maximum-longitude")
    minimum_latitude: float = Field(alias="minimum-latitude")
    minimum_longitude: float = Field(alias="minimum-longitude")


class Shakemap(Product):
    properties: ShakemapProperties


class Products(BaseModelWithExtra):
    shakemap: list[Shakemap] | None = None
    dyfi: list[Product] | None = None
    losspager: list[Product] | None = None
    ground_failure: list[Product] | None = Field(default=None, alias="ground-failure")
    finite_fault: list[Product] | None = Field(default=None, alias="finite-fault")
    moment_tensor: list[Product] | None = Field(default=None, alias="moment-tensor")
    origin: list[Product] | None = None
    phase_data: list[Product] | None = Field(default=None, alias="phase-data")


class BaseProperties(BaseModel):
    mag: float
    place: str
    time: int
    felt: Optional[int] = None
    status: str
    tsunami: int
    magType: str
    title: str
    products: Products


class Geometry(BaseModel):
    type: str
    coordinates: List[Union[float, None]]

    @field_validator("coordinates")
    def validate_coordinates(cls, value):
        if len(value) != 3:
            logger.warning("Coordinates must have exactly three elements (longitude, latitude, depth)")
        return value


class USGSValidator(BaseModel):
    type: str
    properties: BaseProperties
    geometry: Geometry
    id: str


class CountryFatalities(BaseModel):
    country_code: str
    fatalities: int


class CountryDollars(BaseModel):
    country_code: str
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


class AlertBin(BaseModelWithExtra):
    color: str
    min: float
    max: float
    probability: float


class AlertDesc(BaseModelWithExtra):
    type: str
    units: str
    gvalue: float
    summary: bool
    level: str
    bins: List[AlertBin]


class AlertValidator(BaseModelWithExtra):
    fatality: AlertDesc
    economic: AlertDesc
