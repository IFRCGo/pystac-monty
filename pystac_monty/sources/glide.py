import json
import logging
import mimetypes
import typing
from datetime import datetime
from typing import Any, List

from pystac import Asset, Item, Link
from shapely.geometry import Point, mapping

from pystac_monty.extension import HazardDetail, MontyEstimateType, MontyExtension
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import MontyDataSource, MontyDataTransformer
from pystac_monty.validators.glide import GlideSetValidator

logger = logging.getLogger(__name__)

STAC_EVENT_ID_PREFIX = "glide-event-"
STAC_HAZARD_ID_PREFIX = "glide-hazard-"


class GlideDataSource(MontyDataSource):
    def __init__(self, source_url: str, data: Any):
        super().__init__(source_url, data)
        self.data = json.loads(data)


class GlideTransformer(MontyDataTransformer[GlideDataSource]):
    """
    Transforms Glide event data into STAC Items
    """

    hazard_profiles = MontyHazardProfiles()
    source_name = "glide"

    # FIXME: This is deprecated
    def make_items(self) -> list[Item]:
        return list(self.get_stac_items())

    def get_stac_items(self) -> typing.Generator[Item, None, None]:
        glideset: list[dict] = self.data_source.get_data()["glideset"]

        self.transform_summary.mark_as_started()
        for row in glideset:
            self.transform_summary.increment_rows()
            try:
                def parse_row_data(row: dict):
                    obj = GlideSetValidator(**row)
                    return obj

                data = parse_row_data(row)
                if event_item := self.make_source_event_items(data):
                    yield event_item
                    yield self.make_hazard_event_items(event_item)
                else:
                    self.transform_summary.increment_failed_rows()
            except Exception:
                self.transform_summary.increment_failed_rows()
                logger.error("Failed to process glide", exc_info=True)
        self.transform_summary.mark_as_complete()

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
        return hazard_mapping[hazard]

    def make_source_event_items(self, data: GlideSetValidator) -> Item | None:
        """Create source event items"""
        glide_id = STAC_EVENT_ID_PREFIX + data.event + "-" + data.number + "-" + data.geocode
        latitude = data.latitude
        longitude = data.longitude
        event_date = {
            "year": data.year,
            "month": data.month,
            "day": data.day,
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
                # "title": humaran readable from event, location, year, month, day,
                "title": f"{data.event} in {data.location} on {data.year}, {data.month}, {data.day}",
                "description": data.comments,
                "magnitude": data.magnitude,
                "source": data.source,
                "docid": data.docid,
                "status": data.status,
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
        monty.hazard_codes = self.get_hazard_codes(data.event)
        monty.country_codes = [data.geocode]

        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        item.add_link(Link("via", self.data_source.get_source_url(), "application/json", "Glide Event Data"))
        item.add_asset(
            "report",
            Asset(
                href=f"https://www.glidenumber.net/glide/public/search/details.jsp?glide={data.docid}",
                media_type=mimetypes.types_map[".json"],
                title="Report",
            ),
        )

        return item

    def make_hazard_event_items(self, event_item: Item) -> Item:
        """Create hazard event items"""
        item = event_item.clone()
        item.id = item.id.replace(STAC_EVENT_ID_PREFIX, STAC_HAZARD_ID_PREFIX)
        item.set_collection(self.get_hazard_collection())
        item.properties["roles"] = ["source", "hazard"]

        monty = MontyExtension.ext(item)
        monty.hazard_detail = self.get_hazard_detail(item)

        return item

    def get_hazard_detail(self, item: Item) -> HazardDetail:
        """Get hazard detail"""
        # FIXME: This is not type safe
        magnitude = item.properties.get("magnitude")

        return HazardDetail(
            cluster=self.hazard_profiles.get_cluster_code(item),
            severity_value=magnitude or 0,
            severity_unit="glide",
            estimate_type=MontyEstimateType.PRIMARY,
        )

    def make_date(self, event_date: dict) -> datetime:
        """Generate a datetime object"""
        dt = datetime(**event_date)
        formatted_date = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        date = datetime.fromisoformat(formatted_date.replace("Z", "+00:00"))
        return date

    def check_and_get_glide_events(self) -> list[Any]:
        """Validate the source fields"""
        glideset: list[Any] = self.data_source.get_data()["glideset"]
        if glideset == []:
            print(f"No Glide data found in {self.data_source.get_source_url()}")
        for obj in glideset:
            required_fields = ["latitude", "longitude", "event", "number", "geocode"]
            missing_fields = [field for field in required_fields if field not in obj]

            if missing_fields:
                raise ValueError(f"Missing required fields {missing_fields} in glide number {obj.get('number')}")
        return glideset
