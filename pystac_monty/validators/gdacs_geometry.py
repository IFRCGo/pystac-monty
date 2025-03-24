import logging
from datetime import datetime
from typing import List, Union

from pydantic import BaseModel, ConfigDict, HttpUrl, field_validator

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class BaseModelWithExtra(BaseModel):
    model_config = ConfigDict(extra="ignore")


# Define the schema for affected countries
class AffectedCountry(BaseModelWithExtra):
    iso2: str
    iso3: str
    countryname: str


# Define the schema for severity data
class SeverityData(BaseModelWithExtra):
    severity: int
    severitytext: str
    severityunit: str


# Define the schema for URLs
class EventUrls(BaseModelWithExtra):
    geometry: Union[str, HttpUrl]
    report: Union[str, HttpUrl]
    details: Union[str, HttpUrl]


# Define the schema for properties (event properties)
class EventProperties(BaseModelWithExtra):
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
    coordinates: Union[List[float], List[List[float]], List[List[List[float]]]]


# Define the schema for the feature collection
class GdacsDataValidatorGeometry(BaseModelWithExtra):
    type: str
    bbox: List[float]
    geometry: Geometry
    properties: EventProperties

    @field_validator("bbox")
    @classmethod
    def validate_bbox(cls, value):
        """Ensure bbox has at least four coordinates"""
        if len(value) < 4:
            raise ValueError("Bounding box must have at least four coordinates.")
        return value

    @classmethod
    def validate_event(cls, data) -> bool:
        """Validate the overall data item"""
        try:
            _ = cls(**data)  # This will trigger the validators
        except Exception as e:
            logger.error(
                "Gdacs validation failed",
                exc_info=True,
                # extra=log_extra(e),
            )
            return False
        return True
