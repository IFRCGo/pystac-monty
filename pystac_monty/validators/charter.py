"""Pydantic validators for International Charter source payloads.

The Charter transformer intentionally preserves raw upstream dictionaries for
pass-through STAC fields, so these models validate the input envelope without
normalizing or dumping data back into the transform path.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class BaseModelWithExtra(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class CharterActivationProperties(BaseModelWithExtra):
    activation_id: int | str | None = Field(default=None, alias="disaster:activation_id")
    call_ids: list[int | str] | None = Field(default=None, alias="disaster:call_ids")
    disaster_type: list[str] | str | None = Field(default=None, alias="disaster:type")
    country: str | None = Field(default=None, alias="disaster:country")
    activation_status: str | None = Field(default=None, alias="cpe:activation_status")
    datetime: Any | None = None
    title: str | None = None
    description: str | None = None


class CharterAreaProperties(BaseModelWithExtra):
    title: str | None = None
    description: str | None = None
    status: dict[str, Any] | None = Field(default=None, alias="cpe:status")


class CharterItemEnvelope(BaseModelWithExtra):
    id: str | int | None = None
    type: str | None = None
    geometry: dict[str, Any] | None = None
    bbox: list[float] | None = None
    links: list[dict[str, Any]] = Field(default_factory=list)
    assets: dict[str, Any] = Field(default_factory=dict)


class CharterActivation(CharterItemEnvelope):
    properties: CharterActivationProperties = Field(default_factory=CharterActivationProperties)


class CharterArea(CharterItemEnvelope):
    properties: CharterAreaProperties | dict[str, Any] = Field(default_factory=CharterAreaProperties)


class CharterVap(CharterItemEnvelope):
    properties: dict[str, Any] = Field(default_factory=dict)


class CharterCalibratedDataset(CharterItemEnvelope):
    properties: dict[str, Any] = Field(default_factory=dict)


class CharterSourceModel(CharterActivation):
    areas: list[CharterArea] = Field(default_factory=list)
    vaps: list[CharterVap] = Field(default_factory=list)
    calibrated_datasets: list[CharterCalibratedDataset] = Field(default_factory=list)
