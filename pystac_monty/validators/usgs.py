from attr import dataclass
from pydantic import BaseModel, HttpUrl, Field, field_validator
from typing import List, Optional, Dict, Any

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)

class Metadata(BaseModel):
    generated: int
    url: HttpUrl
    title: str
    status: int
    api: str
    count: int

class Properties(BaseModel):
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

class Geometry(BaseModel):
    type: str
    coordinates: List[float]
    
    @field_validator("coordinates")
    def validate_coordinates(cls, value):
        if len(value) != 3:
            logger.error("Coordinates must have exactly three elements (longitude, latitude, depth)")
        return value

class Feature(BaseModel):
    type: str
    properties: Properties
    geometry: Geometry
    id: str

class EarthquakeData(BaseModel):
    type: str
    metadata: Metadata
    features: List[Feature]
    bbox: List[float]
    
    @field_validator("bbox")
    def validate_bbox(cls, value):
        if len(value) != 6:
            logger.error("Bounding box must contain exactly six elements")
        return value
    
