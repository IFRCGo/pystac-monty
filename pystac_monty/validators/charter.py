"""Charter activation data validators

Pydantic models for validating International Charter on Space and Major Disasters data.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class CpeStatus(BaseModel):
    """CPE (Common Processing Environment) status"""

    stage: str = "notificationNew"


class AreaProperties(BaseModel):
    """Properties for a Charter area"""

    model_config = ConfigDict(populate_by_name=True)

    title: str
    description: Optional[str] = None
    cpe_status: Optional[CpeStatus] = Field(default=None, alias="cpe:status")


class CharterArea(BaseModel):
    """Charter area (geographic region affected)"""

    id: str
    type: str = "Feature"
    geometry: Dict[str, Any]
    bbox: Optional[List[float]] = None
    properties: AreaProperties


class CharterActivationProperties(BaseModel):
    """Properties for a Charter activation"""

    model_config = ConfigDict(populate_by_name=True)

    disaster_activation_id: str = Field(alias="disaster:activation_id")
    disaster_type: List[str] = Field(alias="disaster:type")
    disaster_country: Optional[str] = Field(default=None, alias="disaster:country")
    datetime: str
    title: str
    description: Optional[str] = None


class CharterActivation(BaseModel):
    """Charter activation data validator"""

    model_config = ConfigDict(populate_by_name=True)

    id: str
    type: str = "Feature"
    geometry: Dict[str, Any]
    bbox: Optional[List[float]] = None
    properties: CharterActivationProperties
    areas: List[CharterArea] = Field(default_factory=list)
    links: List[Dict[str, Any]] = Field(default_factory=list)
