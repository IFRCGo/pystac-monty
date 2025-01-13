from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class MontyDataSource:
    source_url: str
    data: Any

    def __init__(self, source_url: str, data: Any):
        self.source_url = source_url

    def get_source_url(self) -> str:
        return self.source_url

    def get_data(self) -> Any:
        return self.data
