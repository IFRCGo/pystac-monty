import pydantic
from typing import Literal, Optional, TypedDict

class Source(pydantic.BaseModel):
    type: Literal["file",'memory','url'] = "memory"
