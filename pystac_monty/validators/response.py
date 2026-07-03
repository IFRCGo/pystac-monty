import re
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from pystac_monty.extension import MontyMethodology, MontyResponseStatus

RESPONSE_TYPE_PATTERN = re.compile(r"^(eo|hum|fin)-[a-z0-9]+(-[a-z0-9]+)*$")
SENDAI_TARGETS = {"A", "B", "C", "D", "E", "F", "G"}


class ResponseDetailValidator(BaseModel):
    """Validates a `monty:response_detail` object against the v1.3.0 schema constraints.

    Unlike the source-data validators in this package (which use `extra="ignore"` to
    tolerate upstream API fields), this validates the outgoing `monty:response_detail`
    object, which the schema declares `additionalProperties: false` for.
    """

    model_config = ConfigDict(extra="forbid")

    type: str
    source_id: Optional[str] = None
    status: Optional[MontyResponseStatus] = None
    monitoring_number: Optional[int] = Field(default=None, ge=1)
    producer: Optional[str] = None
    methodology: Optional[MontyMethodology] = None
    sendai_targets: Optional[list[str]] = None
    sectors: Optional[list[str]] = None

    @field_validator("type")
    @classmethod
    def validate_type_pattern(cls, v: str) -> str:
        if not RESPONSE_TYPE_PATTERN.match(v):
            raise ValueError(f"'type' {v!r} does not match the response taxonomy pattern '{RESPONSE_TYPE_PATTERN.pattern}'")
        return v

    @field_validator("sendai_targets")
    @classmethod
    def validate_sendai_targets(cls, v: Optional[list[str]]) -> Optional[list[str]]:
        if v is None:
            return v
        unknown = sorted(set(v) - SENDAI_TARGETS)
        if unknown:
            raise ValueError(f"sendai_targets contains values outside {sorted(SENDAI_TARGETS)}: {unknown}")
        if len(v) != len(set(v)):
            raise ValueError("sendai_targets must be unique")
        return v
