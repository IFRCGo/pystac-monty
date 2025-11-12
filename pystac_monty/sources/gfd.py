import json
import logging
import os
import typing
from datetime import datetime
from typing import List, Union

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
from pystac_monty.sources.common import (
    DataType,
    File,
    GenericDataSource,
    Memory,
    MontyDataSourceV3,
    MontyDataTransformer,
)
from pystac_monty.validators.gfd import GFDSourceValidator

logger = logging.getLogger(__name__)

# Constants

STAC_EVENT_ID_PREFIX = "gfd-event-"
STAC_HAZARD_ID_PREFIX = "gfd-hazard-"
STAC_IMPACT_ID_PREFIX = "gfd-impact-"


class GFDDataSource(MontyDataSourceV3):
    """GFD Data from the source"""

    file_path: str
    source_url: str
    data: Union[str, dict]
    data_source: Union[File, Memory]

    def __init__(self, data: GenericDataSource):
        super().__init__(root=data)

        def handle_file_data():
            if os.path.isfile(self.input_data.path):
                self.file_path = self.input_data.path
            else:
                raise ValueError("File path does not exist")

        def handle_memory_data():
            if isinstance(self.input_data.content, list):
                self.data = self.input_data.content
            else:
                raise ValueError("Data must be in JSON")

        input_data_type = self.input_data.data_type
        match input_data_type:
            case DataType.FILE:
                handle_file_data()
            case DataType.MEMORY:
                handle_memory_data()
            case _:
                typing.assert_never(input_data_type)

    def get_data(self) -> Union[dict, str]:
        """Get the data"""
        if self.input_data.data_type == DataType.FILE:
            with open(self.file_path, "r", encoding="utf-8") as f:
                return json.loads(f.read())
        return self.data

    def get_input_data_type(self) -> DataType:
        """Get the input data type"""
        return self.input_data.data_type


class GFDTransformer(MontyDataTransformer[GFDDataSource]):
    """Transform the source data into the STAC items"""

    hazard_profiles = MontyHazardProfiles()
    source_name = "gfd"

    # FIXME: This is deprecated
    def make_items(self) -> List[Item]:
        return list(self.get_stac_items())

    def get_stac_items(self) -> typing.Generator[Item, None, None]:
        data = self.data_source.get_data()

        self.transform_summary.mark_as_started()
        for row in data:
            self.transform_summary.increment_rows()
            try:
                data = GFDSourceValidator(**row)
                if event_item := self.make_source_event_item(data):
                    yield event_item
                    yield self.make_hazard_event_item(event_item, data)
                    yield from self.make_impact_items(event_item, data)
                else:
                    self.transform_summary.increment_failed_rows()
            except Exception:
                self.transform_summary.increment_failed_rows()
                logger.warning("Failed to process GFD data", exc_info=True)
        self.transform_summary.mark_as_complete()

    def _get_bounding_box(self, polygon: list):
        """Get the bounding box from the polygon"""
        lons, lats = zip(*polygon)  # Separate longitudes and latitudes
        return [min(lons), min(lats), max(lons), max(lats)]

    def make_source_event_item(self, data: GFDSourceValidator) -> Item:
        """Create the source event item"""

        properties = data.properties
        footprint = data.properties.system_footprint
        # Note: Convert LinearRing to Polygon as LinearRing is not supported in STAC spec.
        # FIXME: This might be incorrect
        geometry = {"type": "Polygon", "coordinates": [footprint.coordinates]}

        description = properties.dfo_main_cause

        bbox = self._get_bounding_box(footprint.coordinates)
        # Episode number not in the source, so, set it to 1
        episode_number = 1

        startdate = pytz.utc.localize(datetime.fromtimestamp(properties.system_time_start / 1000))
        enddate = pytz.utc.localize(datetime.fromtimestamp(properties.system_time_end / 1000))

        item = Item(
            id=f"{STAC_EVENT_ID_PREFIX}{properties.id}",
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
        monty.country_codes = properties.cc.split(",")
        monty.hazard_codes = ["MH0600", "nat-hyd-flo-flo", "FL"]  # GFD is a Flood related source
        monty.hazard_codes = self.hazard_profiles.get_canonical_hazard_codes(item=item)

        hazard_keywords = self.hazard_profiles.get_keywords(monty.hazard_codes)
        item.properties["keywords"] = list(set(hazard_keywords + monty.country_codes))

        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        return item

    def make_hazard_event_item(self, event_item: Item, row: GFDSourceValidator) -> Item:
        """Create hazard items"""

        hazard_item = event_item.clone()
        hazard_item.id = event_item.id.replace(STAC_EVENT_ID_PREFIX, STAC_HAZARD_ID_PREFIX)
        hazard_item.properties["roles"] = ["source", "hazard"]
        hazard_item.set_collection(self.get_hazard_collection())

        monty = MontyExtension.ext(hazard_item)
        monty.hazard_codes = [self.hazard_profiles.get_undrr_2025_code(hazard_codes=monty.hazard_codes)]
        # Hazard Detail
        monty.hazard_detail = HazardDetail(
            severity_value=row.properties.dfo_severity,
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
        for key_field, (category, impact_type) in impact_fields.items():
            impact_item = event_item.clone()
            impact_item.id = f"{STAC_IMPACT_ID_PREFIX}{src_data.properties.id}-{key_field}"
            impact_item.properties["title"] = f"{event_item.properties['title']}-{key_field}"
            impact_item.properties["roles"] = ["source", "impact"]
            impact_item.set_collection(self.get_impact_collection())

            monty = MontyExtension.ext(impact_item)
            monty.impact_detail = ImpactDetail(
                category=category,
                type=impact_type,
                value=getattr(src_data.properties, key_field),
                unit="count",
                estimate_type=MontyEstimateType.PRIMARY,
            )
            impact_items.append(impact_item)
        return impact_items

    def check_and_get_gfd_data(self):
        """Get the GFD data"""
        return [item["properties"] for item in self.data_source.get_data()]
