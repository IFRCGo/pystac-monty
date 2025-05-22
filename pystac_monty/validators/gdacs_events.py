import logging
from datetime import datetime
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, HttpUrl

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class BaseModelWithExtra(BaseModel):
    model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)


class URLLinks(BaseModelWithExtra):
    geometry: HttpUrl
    report: HttpUrl
    media: Optional[HttpUrl] = None
    detail: Optional[HttpUrl] = None


class SeverityData(BaseModelWithExtra):
    severity: Union[float, int]
    severitytext: str
    severityunit: str


class Sendai(BaseModelWithExtra):
    latest: bool
    sendaitype: str
    sendainame: str
    sendaivalue: str
    country: str
    region: str
    dateinsert: datetime
    description: str
    onset_date: datetime
    expires_date: datetime
    effective_date: Optional[datetime] = None


class AffectedCountry(BaseModelWithExtra):
    iso2: Optional[str] = None
    iso3: str
    countryname: str


class Geometry(BaseModelWithExtra):
    type: str
    coordinates: Union[List[float], List[List[float]], List[List[List[float]]]]


class Properties(BaseModelWithExtra):
    eventtype: str
    eventid: int
    episodeid: int
    glide: str
    name: str
    description: str
    htmldescription: str
    icon: Union[str, HttpUrl]
    url: URLLinks
    alertlevel: str
    episodealertlevel: str
    episodealertscore: float
    country: str
    fromdate: Union[str, datetime]
    todate: Union[str, datetime]
    iso3: str
    source: str
    sourceid: str
    Class: str
    affectedcountries: List[AffectedCountry]
    severitydata: SeverityData
    episodes: Optional[List[Dict[str, HttpUrl]]] = None
    sendai: Optional[List[Sendai]] = None
    impacts: Optional[List[Dict]] = []
    additionalinfos: Optional[Dict] = None
    documents: Optional[Dict] = None


class GdacsEventDataValidator(BaseModelWithExtra):
    type: str
    bbox: List[float]
    geometry: Geometry
    properties: Properties
