import abc
import json
import tempfile
import typing
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Literal, Optional, Tuple, Union

import requests
from pydantic import BaseModel, ConfigDict, Field
from pystac import Collection, Item

from pystac_monty.geocoding import MontyGeoCoder


class TransformSummaryInProgressException(Exception): ...


@dataclass
class TransformSummary:
    total_rows: int = 0
    failed_rows: int = 0
    is_completed: bool = False

    def increment_rows(self, increment=1):
        self.total_rows += increment

    def increment_failed_rows(self, increment=1):
        self.failed_rows += increment

    def mark_as_complete(self):
        self.is_completed = True

    def mark_as_started(self):
        self.is_completed = False
        self.total_rows = 0
        self.failed_rows = 0

    @property
    def success_rows(self) -> int:
        if not self.is_completed:
            raise TransformSummaryInProgressException()
        return self.total_rows - self.failed_rows


class BaseModelWithExtra(BaseModel):
    model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)


class DataType(Enum):
    FILE = "file"
    MEMORY = "memory"


class File(BaseModelWithExtra):
    data_type: Literal[DataType.FILE]
    path: str | tempfile._TemporaryFileWrapper


class Memory(BaseModel):
    data_type: Literal[DataType.MEMORY]
    content: typing.Any


class SourceSchemaValidator(BaseModel):
    source_url: str
    source_data: Union[File, Memory] = Field(discriminator="data_type")


class GenericDataSource(BaseModel):
    source_url: str
    input_data: Union[File, Memory]


class GdacsEpisodes(BaseModel):
    type: str
    data: GenericDataSource
    hazard_type: str | None


class GdacsDataSourceType(BaseModel):
    source_url: str
    event_data: Union[File, Memory]
    episodes: List[Tuple[GdacsEpisodes, GdacsEpisodes, GdacsEpisodes]]


class USGSDataSourceType(BaseModel):
    source_url: str
    event_data: Union[File, Memory]
    loss_data: Union[File, Memory, None] = None


class DesinventarDataSourceType(BaseModel):
    tmp_zip_file: File
    country_code: str
    iso3: str
    source_url: str | None = None


class PDCDataSourceType(BaseModel):
    source_url: str
    uuid: str
    hazard_data: Union[File, Memory]
    exposure_detail_data: Union[File, Memory]
    geojson_path: str


@dataclass
class MontyDataSource:
    source_url: str
    data: typing.Any

    def __init__(self, source_url: str, data: typing.Any):
        self.source_url = source_url
        self.data = data

    def get_source_url(self) -> str:
        return self.source_url

    def get_data(self) -> typing.Any:
        return self.data


@dataclass
class MontyDataSourceV2:
    source_url: str
    data_source: Union[File, Memory]

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, data: dict):
        validated = SourceSchemaValidator(**data)
        if validated:
            self.source_url = validated.source_url
            self.data_source = validated.source_data

    def get_source_url(self) -> str:
        return self.source_url

    def get_data(self) -> typing.Any:
        return self.data


@dataclass
class MontyDataSourceV3:
    root: Union[GenericDataSource, GdacsDataSourceType, USGSDataSourceType, DesinventarDataSourceType, PDCDataSourceType]
    source_url: Optional[str] = field(init=False)

    def __post_init__(self):
        self.source_url = self.root.source_url
        if isinstance(self.root, GenericDataSource):
            self.input_data = self.root.input_data

    def get_source_url(self) -> Optional[str]:
        """Get the Source URL"""
        return self.source_url


DataSource = typing.TypeVar("DataSource")


@dataclass
class MontyDataTransformer(typing.Generic[DataSource]):
    # FIXME: Add a validation in subclass so that source_name is always defined
    source_name: str

    _event_collection_cache: Collection | None = None
    _hazard_collection_cache: Collection | None = None
    _impact_collection_cache: Collection | None = None

    # FIXME: Get this from submodule
    base_collection_url = "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/examples"

    # FIXME: we might have to get ids and urls manually
    def __init__(self, data_source: DataSource, geocoder: MontyGeoCoder):
        self.events_collection_id = f"{self.source_name}-events"
        self.hazards_collection_id = f"{self.source_name}-hazards"
        self.impacts_collection_id = f"{self.source_name}-impacts"

        self.data_source = data_source

        self.events_collection_url = (
            f"{MontyDataTransformer.base_collection_url}/{self.events_collection_id}/{self.events_collection_id}.json"
        )
        self.hazards_collection_url = (
            f"{MontyDataTransformer.base_collection_url}/{self.hazards_collection_id}/{self.hazards_collection_id}.json"
        )
        self.impacts_collection_url = (
            f"{MontyDataTransformer.base_collection_url}/{self.impacts_collection_id}/{self.impacts_collection_id}.json"
        )

        self.geocoder = geocoder

        self.transform_summary = TransformSummary()

    def get_event_collection(self) -> Collection:
        """Get event collection"""
        if self._event_collection_cache is None:
            # Handle local file as well
            if self.events_collection_url.startswith("http"):
                response = requests.get(self.events_collection_url)
                collection_dict = json.loads(response.text)
            else:
                with open(self.events_collection_url) as f:
                    collection_dict = json.load(f)
            collection = Collection.from_dict(collection_dict)
            # update self link with actual link
            collection.set_self_href(self.events_collection_url)
            self._event_collection_cache = collection
        return self._event_collection_cache

    def get_hazard_collection(self) -> Collection:
        """Get hazard collection"""
        if self._hazard_collection_cache is None:
            # Handle local file as well
            if self.hazards_collection_url.startswith("http"):
                response = requests.get(self.hazards_collection_url)
                collection_dict = json.loads(response.text)
            else:
                with open(self.hazards_collection_url) as f:
                    collection_dict = json.load(f)
            collection = Collection.from_dict(collection_dict)
            # update self link with actual link
            collection.set_self_href(self.hazards_collection_url)
            self._hazard_collection_cache = collection
        return self._hazard_collection_cache

    def get_impact_collection(self) -> Collection:
        """Get impact collection"""
        if self._impact_collection_cache is None:
            # Handle local file as well
            if self.impacts_collection_url.startswith("http"):
                response = requests.get(self.impacts_collection_url)
                collection_dict = json.loads(response.text)
            else:
                with open(self.impacts_collection_url) as f:
                    collection_dict = json.load(f)
            collection = Collection.from_dict(collection_dict)
            # update self link with actual link
            collection.set_self_href(self.impacts_collection_url)
            self._impact_collection_cache = collection
        return self._impact_collection_cache

    # FIXME: This method is deprecated
    @abc.abstractmethod
    def make_items(self) -> list[Item]: ...

    @abc.abstractmethod
    def get_stac_items(self) -> typing.Generator[Item, None, None]: ...
