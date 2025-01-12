import json
from datetime import datetime
from typing import Any

import requests
from pystac import Collection, Item, Link
from shapely.geometry import Point, mapping

from pystac_monty.extension import MontyExtension
from pystac_monty.hazard_profiles import HazardProfiles
from pystac_monty.sources.common import MontyDataSource


class GlideDataSource(MontyDataSource):
    def __init__(self, source_url: str, data: Any):
        super().__init__(source_url, data)
        self.data = json.loads(data)


class GlideTransformer:
    """
    Transforms Glide event data into STAC Items
    """

    hazard_profiles = HazardProfiles()

    glide_events_collection_url = (
        "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/examples/glide-events/glide-events.json"
    )

    def __init__(self, data: GlideDataSource) -> None:
        self.data = data

    def make_items(self) -> list[Item]:
        items = []

        """ Create glide event items """
        glide_events = self.make_event_items()
        items.extend(glide_events)

        return items

    def make_event_items(self) -> list[Item]:
        event_items = []
        # validate data for glide transformation
        glide_events = self.check_and_get_glide_events()

        if not glide_events == []:
            for data in glide_events:
                glide_id = data.get("event") + "-" + data.get("number") + "-" + data.get("geocode")
                latitude = float(data.get("latitude"))
                longitude = float(data.get("longitude"))
                event_date = {"year": data.get("year"), "month": data.get("month"), "day": data.get("day")}

                point = Point(longitude, latitude)
                geometry = mapping(point)
                bbox = [longitude, latitude, longitude, latitude]

                item = Item(
                    id=glide_id,
                    geometry=geometry,
                    bbox=bbox,
                    datetime=self.make_date(event_date),
                    properties={
                        "title": data.get("title", ""),
                        "description": data.get("comments", ""),
                        "magnitude": data.get("magnitude", ""),
                        "source": data.get("source", ""),
                        "docid": data.get("docid", ""),
                        "status": data.get("status", ""),
                    },
                )

                item.set_collection(self.get_event_collection())
                item.properties["roles"] = ["source", "event"]

                MontyExtension.add_to(item)
                monty = MontyExtension.ext(item)
                # Since there is no episode_number in glide data,
                # we set it to 1 as it is required to create the correlation id
                # in the method monty.compute_and_set_correlation_id(..)
                monty.episode_number = 1
                monty.hazard_codes = [data.get("event")]
                monty.country_codes = [data.get("geocode")]

                monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

                item.add_link(Link("via", self.data.get_source_url(), "application/json", "Glide Event Data"))

                event_items.append(item)

            return event_items

    def make_date(self, data: dict) -> datetime:
        year = data["year"]
        month = data["month"]
        day = data["day"]

        dt = datetime(year, month, day)
        formatted_date = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        date = datetime.fromisoformat(formatted_date.replace("Z", "+00:00"))
        return date

    def get_event_collection(self) -> Collection:
        response = requests.get(self.glide_events_collection_url)
        collection_dict = json.loads(response.text)
        return Collection.from_dict(collection_dict)

    def check_and_get_glide_events(self) -> list[Any]:
        glideset: list[Any] = self.data.get_data()["glideset"]
        if glideset == []:
            raise ValueError(f"No Glide data found in {self.data.get_source_url()}")
        for obj in glideset:
            required_fields = ["latitude", "longitude", "event", "number", "geocode"]
            missing_fields = [field for field in required_fields if field not in obj]

            if missing_fields:
                raise ValueError(f"Missing required fields {missing_fields} in glide number {obj.get('number')}")
        return glideset
