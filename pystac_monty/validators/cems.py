"""Pydantic validators for Copernicus EMS Rapid Mapping source payloads.

The CEMS transformer preserves raw upstream dictionaries for pass-through STAC
fields; these models validate the input envelope without normalizing data back
into the transform path.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class BaseModelWithExtra(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class CEMSCountry(BaseModelWithExtra):
    name: str | None = None


class CEMSProductVersion(BaseModelWithExtra):
    uuid: str | None = None
    number: int | None = None
    reason: str | None = None
    delivery_time: str | None = Field(default=None, alias="deliveryTime")
    status_code: str | None = Field(default=None, alias="statusCode")


class CEMSProductImage(BaseModelWithExtra):
    uuid: str | None = None
    sensor_type: str | None = Field(default=None, alias="sensorType")
    sensor_name: str | None = Field(default=None, alias="sensorName")
    resolution_class: str | None = Field(default=None, alias="resolutionClass")
    acquisition_time: str | None = Field(default=None, alias="acquisitionTime")
    file_name: str | None = Field(default=None, alias="fileName")


class CEMSProductLayer(BaseModelWithExtra):
    name: str | None = None
    format: str | None = None
    sld: str | None = None
    layer_json: str | None = Field(default=None, alias="json")


class CEMSProduct(BaseModelWithExtra):
    id: int | str | None = None
    type: str | None = None
    monitoring: bool | None = None
    monitoring_number: int | None = Field(default=None, alias="monitoringNumber")
    feasible: bool | None = None
    images: list[CEMSProductImage] = Field(default_factory=list)
    stats: dict[str, Any] | None = None
    maps_count: int | None = Field(default=None, alias="mapsCount")
    activation_code: str | None = Field(default=None, alias="activationCode")
    aoi_name: str | None = Field(default=None, alias="aoiName")
    aoi_number: int | None = Field(default=None, alias="aoiNumber")
    extent: str | None = None
    expected_delivery: str | None = Field(default=None, alias="expectedDelivery")
    layers: list[CEMSProductLayer] = Field(default_factory=list)
    download_path: str | None = Field(default=None, alias="downloadPath")
    version: CEMSProductVersion | dict[str, Any] = Field(default_factory=CEMSProductVersion)


class CEMSAoi(BaseModelWithExtra):
    name: str | None = None
    extent: str | None = None
    number: int | None = None
    activation_code: str | None = Field(default=None, alias="activationCode")
    products: list[CEMSProduct] = Field(default_factory=list)
    blp_path: str | None = Field(default=None, alias="blpPath")


class CEMSActivation(BaseModelWithExtra):
    code: str | None = None
    name: str | None = None
    reason: str | None = None
    category: str | None = None
    sub_category: str | None = Field(default=None, alias="subCategory")
    sensitive: bool | None = None
    report_link: str | None = Field(default=None, alias="reportLink")
    activator: str | None = None
    event_time: str | None = Field(default=None, alias="eventTime")
    activation_time: str | None = Field(default=None, alias="activationTime")
    closed: bool | None = None
    gdacs_id: str | None = Field(default=None, alias="gdacsId")
    charter_number: int | str | None = Field(default=None, alias="charterNumber")
    charter_url: str | None = Field(default=None, alias="charterUrl")
    continent: str | None = None
    countries: list[CEMSCountry] = Field(default_factory=list)
    aois: list[CEMSAoi] = Field(default_factory=list)
    centroid: str | None = None
    extent: str | None = None
    stats: dict[str, Any] | None = None
    relatedevents: list[str] = Field(default_factory=list)


class CEMSDetailEnvelope(BaseModelWithExtra):
    count: int | None = None
    next: str | None = None
    previous: str | None = None
    results: list[CEMSActivation] = Field(default_factory=list)
