import json
import logging
import typing
from datetime import datetime
from typing import Any, List

import pytz
from pystac import Item

from pystac_monty.extension import (
    HazardDetail,
    ImpactDetail,
    MontyEstimateType,
    MontyExtension,
    MontyImpactExposureCategory,
    MontyImpactType,
)
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import MontyDataSource, MontyDataTransformer
from pystac_monty.validators.gfd import GFDSourceValidator

logger = logging.getLogger(__name__)

# Constants

STAC_EVENT_ID_PREFIX = "gfd-event-"
STAC_HAZARD_ID_PREFIX = "gfd-hazard-"
STAC_IMPACT_ID_PREFIX = "gfd-impact-"


class GFDDataSource(MontyDataSource):
    """GFD Data from the source"""

    def __init__(self, source_url: str, data: Any):
        super().__init__(source_url, data)
        self.data = json.loads(data)


class GFDTransformer(MontyDataTransformer[GFDDataSource]):
    """Transform the source data into the STAC items"""

    hazard_profiles = MontyHazardProfiles()
    source_name = "gfd"

    def make_items(self) -> List[Item]:
        return list(self.get_stac_items())

    def get_stac_items(self) -> typing.Generator[Item, None, None]:
        data = self.data_source.get_data()

        failed_items_count = 0
        total_items_count = 0

        for row in data:
            total_items_count += 1
            try:
                def parse_row_data(obj: dict):
                    obj = GFDSourceValidator(**obj)
                    return obj

                data = parse_row_data(row)
                if event_item := self.make_source_event_item(data):
                    yield event_item
                    yield self.make_hazard_event_item(event_item, data)
                    yield from self.make_impact_items(event_item, data)
                else:
                    failed_items_count += 1
            except Exception:
                failed_items_count += 1
                logger.error("Failed to process gfd", exc_info=True)

        print(failed_items_count)

    def _get_bounding_box(self, polygon: list):
        """Get the bounding box from the polygon"""
        lons, lats = zip(*polygon)  # Separate longitudes and latitudes
        return [min(lons), min(lats), max(lons), max(lats)]

    def make_source_event_item(self, data: GFDSourceValidator) -> Item:
        """Create the source event item"""

        data = data.model_dump()

        properties = data["properties"]
        footprint = data["properties"]["system_footprint"]
        # Note: Convert LinearRing to Polygon as LinearRing is not supported in STAC spec.
        geometry = {"type": "Polygon", "coordinates": [footprint["coordinates"]]}

        description = properties["dfo_main_cause"]

        bbox = self._get_bounding_box(footprint["coordinates"])
        # Episode number not in the source, so, set it to 1
        episode_number = 1

        startdate = pytz.utc.localize(datetime.fromtimestamp(properties["system_time_start"] / 1000))
        enddate = pytz.utc.localize(datetime.fromtimestamp(properties["system_time_end"] / 1000))

        item = Item(
            id=f"{STAC_EVENT_ID_PREFIX}{properties['id']}",
            geometry=geometry,
            bbox=bbox,
            datetime=startdate,
            properties={
                "title": description,
                "description": description,
                "start_datetime": startdate.isoformat(),
                "end_datetime": enddate.isoformat(),
            },
        )

        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = episode_number
        monty.country_codes = properties["cc"].split(",")
        monty.hazard_codes = ["FL"]  # GFD is a Flood related source
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        return item

    def make_hazard_event_item(self, event_item: Item, row: GFDSourceValidator) -> Item:
        """Create hazard items"""

        row = row.model_dump()
        hazard_item = event_item.clone()
        hazard_item.id = event_item.id.replace(STAC_EVENT_ID_PREFIX, STAC_HAZARD_ID_PREFIX)
        hazard_item.properties["roles"] = ["source", "hazard"]
        hazard_item.set_collection(self.get_hazard_collection())

        monty = MontyExtension.ext(hazard_item)
        # Hazard Detail
        monty.hazard_detail = HazardDetail(
            cluster=self.hazard_profiles.get_cluster_code(hazard_item),
            severity_value=row["properties"]["dfo_severity"],
            severity_unit="GFD Flood Severity Score",
            severity_label=None,
            estimate_type=MontyEstimateType.PRIMARY,
        )
        return hazard_item

    def make_impact_items(self, event_item: Item, src_data: GFDSourceValidator) -> List[Item]:
        """Returns the impact details related to flood"""
        impact_fields = {
            "dfo_dead": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.DEATH),
            "dfo_displaced": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.TOTAL_DISPLACED_PERSONS),
        }

        impact_items = []
        src_data = src_data.model_dump()

        for key_field, (category, impact_type) in impact_fields.items():
            impact_item = event_item.clone()
            impact_item.id = f"{STAC_IMPACT_ID_PREFIX}{src_data["properties"]['id']}-{key_field}"
            impact_item.properties["title"] = f"{event_item.properties['title']}-{key_field}"
            impact_item.properties["roles"] = ["source", "impact"]
            impact_item.set_collection(self.get_impact_collection())

            monty = MontyExtension.ext(impact_item)
            monty.impact_detail = ImpactDetail(
                category=category,
                type=impact_type,
                value=src_data["properties"][key_field],
                unit="count",
                estimate_type=MontyEstimateType.PRIMARY,
            )
            impact_items.append(impact_item)
        return impact_items

    def check_and_get_gfd_data(self):
        """Get the GFD data"""
        return [item["properties"] for item in self.data_source.get_data()]
