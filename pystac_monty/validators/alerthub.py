from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator

# -------------------------
# Shared / small components
# -------------------------


class GeoCode(BaseModel):
    id: str
    valueName: str
    value: str


class PolygonDetail(BaseModel):
    type: str
    coordinates: list


class Polygon(BaseModel):
    id: str
    valuePolygon: PolygonDetail


class Area(BaseModel):
    id: str
    areaDesc: str
    polygons: List[Polygon] = Field(default_factory=list)
    circles: Optional[List[str]] = None
    geocodes: List[GeoCode]


class Country(BaseModel):
    id: str
    name: str
    iso3: str


class Admin1(BaseModel):
    id: str
    name: str
    isUnknown: bool
    alertCount: int


# -------------------------
# Info blocks
# -------------------------


class AlertInfo(BaseModel):
    id: str
    alertId: str
    event: str
    language: str
    category: str
    severity: str
    urgency: str
    headline: str
    description: Optional[str]
    instruction: Optional[str]
    onset: Optional[datetime]
    effective: Optional[datetime]
    expires: Optional[datetime]
    eventCode: Optional[str]
    areas: List[Area]

    @field_validator("expires")
    @classmethod
    def expires_after_effective_date(cls, v, info):
        """Check if expire date is before effective date"""
        effective = info.data.get("effective")
        if v and effective and v < effective:
            raise ValueError("expires must be after effective")
        return v


class AlertInfoSummary(BaseModel):
    id: str
    alertId: str
    event: str
    language: str
    category: str
    severity: str
    urgency: str
    certainty: str
    headline: str
    description: Optional[str]
    instruction: Optional[str]
    onset: Optional[datetime]
    effective: Optional[datetime]
    expires: Optional[datetime]
    eventCode: Optional[str]


# -------------------------
# Main Alert
# -------------------------


class AlertItem(BaseModel):
    id: str
    sent: datetime
    status: str
    msgType: str
    identifier: str
    sender: str
    source: Optional[str]
    scope: str
    restriction: Optional[str]
    addresses: Optional[str]
    code: Optional[str]
    note: Optional[str]
    references: Optional[str]
    incidents: Optional[str]
    url: str

    country: Country
    admin1s: List[Admin1]

    info: Optional[AlertInfo]
    infos: List[AlertInfoSummary]

    @field_validator("status")
    @classmethod
    def validate_status(cls, v):
        allowed = {"ACTUAL", "TEST", "DRAFT"}
        if v not in allowed:
            raise ValueError(f"Invalid status: {v}")
        return v


# -------------------------
# Pagination wrapper
# -------------------------


class AlertList(BaseModel):
    limit: int
    offset: int
    count: int
    items: List[AlertItem]

    @field_validator("limit", "offset", "count")
    @classmethod
    def non_negative(cls, v):
        if v < 0:
            raise ValueError("must be non-negative")
        return v


# -------------------------
# GraphQL envelope
# -------------------------


class PublicData(BaseModel):
    historicalAlerts: AlertList


class DataEnvelope(BaseModel):
    public: PublicData


class AlertHubDataValidator(BaseModel):
    data: DataEnvelope
