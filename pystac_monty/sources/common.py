import json
from dataclasses import dataclass
from typing import Any

import requests
from pystac import Collection


@dataclass
class MontyDataSource:
    source_url: str
    data: Any

    def __init__(self, source_url: str, data: Any):
        self.source_url = source_url
        self.data = data

    def get_source_url(self) -> str:
        return self.source_url

    def get_data(self) -> Any:
        return self.data


base_collection_url = "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/examples"


@dataclass
class MontyDataTransformer:
    events_collection_id: str
    events_collection_url: str
    hazards_collection_id: str
    hazards_collection_url: str
    impacts_collection_id: str
    impacts_collection_url: str

    # FIXME: we might have to get ids and urls manually
    def __init__(self, source_name: str):
        self.events_collection_id = f"{source_name}-events"
        self.hazards_collection_id = f"{source_name}-hazards"
        self.impacts_collection_id = f"{source_name}-impacts"

        self.events_collection_url = f"{base_collection_url}/{self.events_collection_id}/{self.events_collection_id}.json"
        self.hazards_collection_url = f"{base_collection_url}/{self.hazards_collection_id}/{self.hazards_collection_id}.json"
        self.impacts_collection_url = f"{base_collection_url}/{self.impacts_collection_id}/{self.impacts_collection_id}.json"

    def get_event_collection(self) -> Collection:
        """Get event collection"""
        response = requests.get(self.events_collection_url)
        collection_dict = json.loads(response.text)
        collection = Collection.from_dict(collection_dict)
        # update self link with actual link
        collection.set_self_href(self.events_collection_url)
        return collection

    def get_hazard_collection(self) -> Collection:
        """Get hazard collection"""
        response = requests.get(self.hazards_collection_url)
        collection_dict = json.loads(response.text)
        collection = Collection.from_dict(collection_dict)
        # update self link with actual link
        collection.set_self_href(self.hazards_collection_url)
        return collection

    def get_impact_collection(self) -> Collection:
        """Get impact collection"""
        response = requests.get(self.impacts_collection_url)
        collection_dict = json.loads(response.text)
        collection = Collection.from_dict(collection_dict)
        # update self link with actual link
        collection.set_self_href(self.impacts_collection_url)
        return collection
