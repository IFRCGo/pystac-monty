import json
import mimetypes
from datetime import datetime
from typing import Any, List

import requests
from pystac import Asset, Collection, Item, Link
from shapely.geometry import Point, mapping

from pystac_monty.extension import HazardDetail, MontyEstimateType, MontyExtension
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import MontyDataSource

STAC_EVENT_ID_PREFIX = "glide-event-"
STAC_HAZARD_ID_PREFIX = "glide-hazard-"


class GlideDataSource(MontyDataSource):
    def __init__(self, source_url: str, data: Any):
        super().__init__(source_url, data)
        self.data = json.loads(data)


class GlideTransformer:
    """
    Transforms Glide event data into STAC Items
    """

    hazard_profiles = MontyHazardProfiles()

    glide_events_collection_url = (
        "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/examples/glide-events/glide-events.json"
    )

    glide_hazard_collection_url = (
        "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/examples/glide-hazards/glide-hazards.json"
    )

    def __init__(self, data: GlideDataSource) -> None:
        self.data = data

    def make_items(self) -> list[Item]:
        """Create Glide Items"""
        items = []

        glide_events = self.make_source_event_items()
        items.extend(glide_events)

        glide_hazards = self.make_hazard_event_items()
        items.extend(glide_hazards)

        return items

    def get_hazard_codes(self, hazard: str) -> List[str]:
        hazard_mapping = {
            "EQ": ["GH0001", "GH0002", "GH0003", "GH0004", "GH0005"],
            "TC": ["MH0030", "MH0031", "MH0032"],
            "FL": ["nat-hyd-flo-flo"],
            "DR": ["MH0035"],
            "WF": ["EN0013"],
            "VO": ["GH009", "GH0013", "GH0014", "GH0015", "GH0016"],
            "TS": ["MH0029", "GH0006"],
            "CW": ["MH0040"],
            "EP": ["nat-bio-epi-dis"],
            "EC": ["MH0031"],
            "ET": ["nat-met-ext-col", "nat-met-ext-hea", "nat-met-ext-sev"],
            "FR": ["tec-ind-fir-fir"],
            "FF": ["MH0006"],
            "HT": ["MH0047"],
            "IN": ["BI0002", "BI0003"],
            "LS": ["nat-hyd-mmw-lan"],
            "MS": ["MH0051"],
            "ST": ["MH0003"],
            "SL": ["nat-hyd-mmw-lan"],
            "AV": ["nat-geo-mmd-ava"],
            "SS": ["MH0027"],
            "AC": ["AC"],
            "TO": ["MH0059"],
            "VW": ["MH0060"],
            "WV": ["MH0029", "GH0006"],
        }
        if hazard not in hazard_mapping:
            raise KeyError(f"Hazard {hazard} not found.")
        return hazard_mapping.get(hazard)

    def make_source_event_items(self) -> List[Item]:
        """Create source event items"""
        event_items = []
        # validate data for glide transformation
        glide_events = self.check_and_get_glide_events()

        if not glide_events == []:
            for data in glide_events:
                glide_id = STAC_EVENT_ID_PREFIX + data.get("event") + "-" + data.get("number") + "-" + data.get("geocode")
                latitude = float(data.get("latitude"))
                longitude = float(data.get("longitude"))
                event_date = {
                    "year": abs(int(data.get("year"))),
                    "month": abs(int(data.get("month"))),
                    "day": abs(int(data.get("day"))),
                }  # abs is used to ignore negative sign

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
                monty.hazard_codes = self.get_hazard_codes(data.get("event"))
                monty.country_codes = [data.get("geocode")]

                monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

                item.add_link(Link("via", self.data.get_source_url(), "application/json", "Glide Event Data"))
                item.add_asset(
                    "report",
                    Asset(
                        href=f"https://www.glidenumber.net/glide/public/search/details.jsp?glide={data.get('docid')}",
                        media_type=mimetypes.types_map[".json"],
                        title="Report",
                    ),
                )

                event_items.append(item)
        return event_items

    def make_hazard_event_items(self) -> List[Item]:
        """Create hazard event items"""
        hazard_items = []
        items = self.make_source_event_items()

        for item in items:
            item.id = item.id.replace(STAC_EVENT_ID_PREFIX, STAC_HAZARD_ID_PREFIX)
            item.set_collection(self.get_hazard_collection())
            item.properties["roles"] = ["source", "hazard"]

            monty = MontyExtension.ext(item)
            monty.hazard_detail = self.get_hazard_detail(item)
            hazard_items.append(item)

        return hazard_items

    def get_hazard_detail(self, item: Item) -> HazardDetail:
        """Get hazard detail"""
        magnitude = item.properties.get("magnitude", "").strip()
        return HazardDetail(
            cluster=self.hazard_profiles.get_cluster_code(item),
            severity_value=int(float(magnitude)) if magnitude else 0,
            severity_unit="glide",
            estimate_type=MontyEstimateType.PRIMARY,
        )

    def make_date(self, event_date: dict) -> datetime:
        """Generate a datetime object"""
        dt = datetime(**event_date)
        formatted_date = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        date = datetime.fromisoformat(formatted_date.replace("Z", "+00:00"))
        return date

    def get_event_collection(self, timeout: int = 30) -> Collection:
        """Get event collection"""
        response = requests.get(self.glide_events_collection_url, timeout=timeout)
        collection_dict = json.loads(response.text)
        return Collection.from_dict(collection_dict)

    def get_hazard_collection(self, timeout: int = 30) -> Collection:
        """Get hazard collection"""
        response = requests.get(self.glide_hazard_collection_url, timeout=timeout)
        collection_dict = json.loads(response.text)
        return Collection.from_dict(collection_dict)

    def check_and_get_glide_events(self) -> list[Any]:
        """Validate the source fields"""
        glideset: list[Any] = self.data.get_data()["glideset"]
        if glideset == []:
            print(f"No Glide data found in {self.data.get_source_url()}")
        for obj in glideset:
            required_fields = ["latitude", "longitude", "event", "number", "geocode"]
            missing_fields = [field for field in required_fields if field not in obj]

            if missing_fields:
                raise ValueError(f"Missing required fields {missing_fields} in glide number {obj.get('number')}")
        return glideset
