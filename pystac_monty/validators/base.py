from typing import Literal

import pydantic


class Source(pydantic.BaseModel):
    type: Literal["file", "memory", "url"] = "memory"
