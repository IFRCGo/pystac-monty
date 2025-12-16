import logging
from typing import List

from pydantic import BaseModel, ConfigDict

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class BaseModelWithExtra(BaseModel):
    model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)


class TCImpactItem(BaseModelWithExtra):
    """Item description of Gdacs Tropical Cyclone"""

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


class TCChannel(BaseModelWithExtra):
    """Channel description of Gdacs Tropical Cyclone"""

    item: List[TCImpactItem]


class GdacsImpactDataValidatorTC(BaseModelWithExtra):
    """Validator for Gdacs Tropical Cyclone for Impact data"""

    channel: TCChannel
    georss: str
    version: str
