import logging
from datetime import datetime
from typing import List, Union

from pydantic import BaseModel, ConfigDict, HttpUrl

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class BaseModelWithExtra(BaseModel):
    model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)


# Define the schema for affected countries
class AffectedCountry(BaseModelWithExtra):
    iso2: str
    iso3: str
    countryname: str


# Define the schema for severity data
class SeverityData(BaseModelWithExtra):
    severity: Union[float, int]
    severitytext: str
    severityunit: str


# Define the schema for URLs
class EventUrls(BaseModelWithExtra):
    geometry: Union[str, HttpUrl]
    report: Union[str, HttpUrl]
    details: Union[str, HttpUrl]


# Define the schema for properties (event properties)
class Properties(BaseModelWithExtra):
    eventtype: str
    eventid: int
    episodeid: int
    eventname: str
    glide: str
    name: str
    description: str
    htmldescription: str
    icon: Union[str, HttpUrl]
    iconoverall: Union[str, HttpUrl]
    url: EventUrls
    alertlevel: str
    alertscore: Union[float, int]
    episodealertlevel: str
    episodealertscore: float
    istemporary: str
    iscurrent: str
    country: str
    fromdate: Union[str, datetime]
    todate: Union[str, datetime]
    datemodified: Union[str, datetime]
    iso3: str
    source: str
    sourceid: str
    polygonlabel: str
    Class: str
    affectedcountries: List[AffectedCountry]
    severitydata: SeverityData


# Define the schema for geometry (point)
class Geometry(BaseModelWithExtra):
    type: str
    coordinates: List


class Feature(BaseModelWithExtra):
    type: str
    bbox: List[float]
    geometry: Geometry
    properties: Properties


# Define the schema for the feature collection
class GdacsGeometryDataValidator(BaseModelWithExtra):
    type: str
    features: List[Feature]
