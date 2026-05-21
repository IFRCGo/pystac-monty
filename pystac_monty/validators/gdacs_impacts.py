import logging
import typing
from typing import List

from pydantic import BaseModel, ConfigDict

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class BaseModelWithExtra(BaseModel):
    model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)


# Validation for Tropical Cyclone(TC)
class TCImpactItem(BaseModelWithExtra):
    """Item description of Gdacs Tropical Cyclone"""

    id: str
    title: str
    point: str
    name: str
    storm_id: str
    advisory_number: str
    actual: str
    pop: str
    latitude: str
    longitude: str
    advisory_datetime: str
    coordinates: str


class TCChannel(BaseModelWithExtra):
    """Channel description of Gdacs Tropical Cyclone"""

    item: List[TCImpactItem]


class GdacsImpactDataValidatorTC(BaseModelWithExtra):
    """Validator for Gdacs Tropical Cyclone for Impact data"""

    channel: TCChannel
    georss: str
    version: str


# Validation for WildFire(WF)


class WFEventDesc(BaseModelWithExtra):
    namespace: str
    eventtype: typing.Literal["WF"]
    eventname: str


class WFScalarData(BaseModelWithExtra):
    name: str
    value: str


class WFScalars(BaseModelWithExtra):
    scalar: List[WFScalarData]


class WFSourceData(BaseModelWithExtra):
    datasource: str
    io: str
    datum_id: str
    type: str
    scalars: WFScalars


class WFData(BaseModelWithExtra):
    datum: List[WFSourceData]


class GdacsImpactDataValidatorWF(BaseModelWithExtra):
    """Validator for Gdacs Wildfire for Impact data"""

    episodeid: str
    eventid: WFEventDesc
    modelname: str
    modelrun: str
    modelstatus: str
    datums: List[WFData]
