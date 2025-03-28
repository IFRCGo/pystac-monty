import logging
from typing import List, Tuple, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class BaseModelWithExtra(BaseModel):
    model_config = ConfigDict(extra="ignore")


class PixelType(BaseModelWithExtra):
    type: str
    precision: str
    min: Union[int, None] = None
    max: Union[int, None] = None


class Band(BaseModelWithExtra):
    id: str
    data_type: PixelType
    dimensions: Tuple[int, int]
    crs: str
    crs_transform: List[Union[int, float]]


class Footprint(BaseModelWithExtra):
    type: str
    coordinates: List[List[float]]


class Properties(BaseModelWithExtra):
    dfo_main_cause: str
    dfo_severity: float  # Assuming severity is between 0-5
    system_footprint: Footprint = Field(alias="system:footprint")
    dfo_displaced: int
    id: int
    cc: str
    system_time_end: int = Field(alias="system:time_end")
    system_time_start: int = Field(alias="system:time_start")
    dfo_dead: int
    system_index: str = Field(alias="system:index")

    # dfo_centroid_y: float
    # gfd_country_name: str
    # dfo_centroid_x: float
    # glide_index: str
    # slope_threshold: int
    # threshold_b1b2: float
    # dfo_validation_type: str
    # composite_type: str
    # dfo_country: str
    # countries: str
    # dfo_other_country: str
    # gfd_country_code: str
    # threshold_type: str
    # threshold_b7: float
    # system_asset_size: int = Field(alias="system:asset_size")


class GFDSourceValidator(BaseModelWithExtra):
    type: str
    bands: List[Band]
    version: int
    id: str
    properties: Properties

    @field_validator("type")
    def validate_type(cls, v):
        if v != "Image":
            raise ValueError("Type must be 'Image'")
        return v
