import datetime
import itertools
import json
import logging
import os
import re
import typing
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

import ijson
import pytz
from markdownify import markdownify as md
from pystac import Asset, Item, Link
from shapely.geometry import Point, mapping

from pystac_monty.extension import (
    ImpactDetail,
    MontyEstimateType,
    MontyExtension,
    MontyImpactExposureCategory,
    MontyImpactType,
)
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import DataType, MontyDataSource, MontyDataSourceV2, MontyDataTransformer
from pystac_monty.sources.utils import IDMCUtils, order_data_file
from pystac_monty.validators.idu import IDUSourceValidator

logger = logging.getLogger(__name__)

# Constants

STAC_EVENT_ID_PREFIX = "idmc-idu-event-"
STAC_IMPACT_ID_PREFIX = "idmc-idu-impact-"


@dataclass
class IDUDataSourceV2(MontyDataSourceV2):
    """IDU Data Source Version 2"""

    parsed_content: List[dict]
    ordered_temp_file: Optional[str] = None

    def __init__(self, data: dict):
        super().__init__(data=data)

        def handle_file_data():
            if os.path.isfile(self.data_source.path):
                jq_filter = "sort_by(.event_id)"
                self.ordered_temp_file = order_data_file(filepath=self.data_source.path, jq_filter=jq_filter)

        def handle_memory_data():
            self.parsed_content = json.loads(self.data_source.content)

        input_data_type = self.data_source.data_type
        match input_data_type:
            case DataType.FILE:
                handle_file_data()
            case DataType.MEMORY:
                handle_memory_data()
            case _:
                typing.assert_never(input_data_type)

    def get_ordered_tmp_file(self):
        """Get the temp file object which has ordered data"""
        return self.ordered_temp_file

    def get_input_data_type(self) -> DataType:
        """Returns the input data type"""
        return self.data_source.data_type

    def get_memory_data(self):
        """Get the parsed memory data"""
        return self.parsed_content


@dataclass
class IDUDataSource(MontyDataSource):
    """IDU Data directly from the source"""

    def __init__(self, source_url: str, data: Any):
        super().__init__(source_url, data)
        self.data = json.loads(data)


class IDUTransformer(MontyDataTransformer[IDUDataSourceV2]):
    """Transform the source data into the STAC items"""

    hazard_profiles = MontyHazardProfiles()
    source_name = "idmc-idu"

    def get_stac_items(self) -> typing.Generator[Item, None, None]:
        """Creates the STAC Items"""
        self.transform_summary.mark_as_started()

        for idu_data in self.check_and_get_idu_data():
            self.transform_summary.increment_rows(len(idu_data))
            try:

                def get_validated_data(items: list[dict]) -> List[IDUSourceValidator]:
                    validated_data: list[IDUSourceValidator] = []
                    for item in items:
                        obj = IDUSourceValidator(**item)
                        validated_data.append(obj)
                    return validated_data

                validated_data = get_validated_data(idu_data)

                if event_item := self.make_source_event_item(validated_data):
                    yield event_item
                    yield from self.make_impact_item(event_item, validated_data)
                else:
                    self.transform_summary.increment_failed_rows(len(idu_data))
            except Exception:
                self.transform_summary.increment_failed_rows(len(idu_data))
                logger.error("Failed to process the IDU data", exc_info=True)
        self.transform_summary.mark_as_complete()

    # FIXME: This is deprecated
    def make_items(self) -> List[Item]:
        return list(self.get_stac_items())

    def make_source_event_item(self, data_items: List[IDUSourceValidator]) -> Item | None:
        """Create an Event Item"""
        # For now, get the first item only to create a single event item
        data_item = data_items[0]
        latitude = float(data_item.latitude or 0.0)
        longitude = float(data_item.longitude or 0.0)

        # Create the geojson point
        # FIXME: We might need to get this from min and max of all figures
        point = Point(longitude, latitude)
        geometry = mapping(point)
        bbox = [longitude, latitude, longitude, latitude]

        description = md(data_item.standard_popup_text)

        # Episode number not in the source, so, set it to 1
        episode_number = 1

        startdate_str = data_item.event_start_date.strftime("%Y-%m-%d")
        enddate_str = data_item.event_end_date.strftime("%Y-%m-%d")

        # FIXME: We might need to get this from min and max of all figures
        startdate = pytz.utc.localize(datetime.datetime.fromisoformat(startdate_str))
        enddate = pytz.utc.localize(datetime.datetime.fromisoformat(enddate_str))

        item = Item(
            id=f"{STAC_EVENT_ID_PREFIX}{data_item.event_id}",
            geometry=geometry,
            bbox=bbox,
            datetime=startdate,
            properties={
                "title": data_item.event_name,
                "description": description,
                "start_datetime": startdate.isoformat(),
                "end_datetime": enddate.isoformat(),
                "location": data_item.locations_name,
                "sources": data_item.sources,
            },
        )

        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        item.add_asset("report", Asset(href=data_item.source_url, media_type="application/pdf", title="Report"))
        item.add_link(Link("via", self.data_source.get_source_url(), "application/json", "IDU Event Data"))

        hazard_tuple = (data_item.category, data_item.subcategory, data_item.type, data_item.subtype)

        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = episode_number
        monty.country_codes = [data_item.iso3]
        monty.hazard_codes = IDMCUtils.hazard_codes_mapping(hazard=hazard_tuple)
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        return item

    def _get_impact_type_from_desc(self, description: str):
        """Get impact type from description using regex"""
        keywords = list(IDMCUtils.mappings.keys())
        # Get the first match
        match = re.findall(r"\((.*?)\)", description)
        # Use the first item only
        if match and match[0] in keywords:
            return match[0]
        logger.warning(f"Match {match} not found. Using the default value.")
        return "displaced"

    def make_impact_item(self, event_item: Item, data_items: List[IDUSourceValidator]) -> List[Item]:
        """Create impact items"""
        items = []

        for data_item in data_items:
            impact_item = event_item.clone()

            startdate_str = data_item.displacement_start_date.strftime("%Y-%m-%d")
            enddate_str = data_item.displacement_end_date.strftime("%Y-%m-%d")
            startdate = pytz.utc.localize(datetime.datetime.fromisoformat(startdate_str))
            enddate = pytz.utc.localize(datetime.datetime.fromisoformat(enddate_str))

            description = data_item.standard_popup_text
            impact_type = self._get_impact_type_from_desc(description=description)

            impact_item.id = f"{impact_item.id.replace(STAC_EVENT_ID_PREFIX, STAC_IMPACT_ID_PREFIX)}{data_item.id}-{impact_type}"
            impact_item.datetime = startdate
            impact_item.properties["start_datetime"] = startdate.isoformat()
            impact_item.properties["end_datetime"] = enddate.isoformat()
            impact_item.properties["roles"] = ["source", "impact"]
            impact_item.set_collection(self.get_impact_collection())

            monty = MontyExtension.ext(impact_item)
            monty.impact_detail = self.get_impact_details(idu_src_item=data_item, impact_type=impact_type)

            items.append(impact_item)
        return items

    def get_impact_details(self, idu_src_item: IDUSourceValidator, impact_type: str):
        """Returns the impact details related to displacement"""
        category, category_type = IDMCUtils.mappings.get(
            impact_type, (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.INTERNALLY_DISPLACED_PERSONS)
        )
        return ImpactDetail(
            category=category,
            type=category_type,
            value=idu_src_item.figure,
            unit="count",
            estimate_type=MontyEstimateType.PRIMARY,
        )

    def check_and_get_idu_data(self) -> Iterator[List[Dict]]:
        """Validate the source fields"""

        required_fields = ["latitude", "longitude", "event_id"]

        tmp_file = self.data_source.get_ordered_tmp_file()
        input_data_type = self.data_source.get_input_data_type()
        match input_data_type:
            case DataType.FILE:
                with open(tmp_file.name, "rb") as f:
                    items = ijson.items(f, "item")  # assumes top-level is a JSON array
                    current_id = None
                    group = []

                    for item in items:
                        if item["displacement_type"] not in IDMCUtils.DisplacementType._value2member_map_:
                            logging.error(f"Unknown displacement type: {item['displacement_type']} found. Ignore the datapoint.")
                            continue
                        # Get the Disaster type data only
                        if IDMCUtils.DisplacementType(item["displacement_type"]) == IDMCUtils.DisplacementType.DISASTER_TYPE:
                            missing_fields = [field for field in required_fields if field not in item]
                            if missing_fields:
                                raise ValueError(f"Missing required fields {missing_fields}.")

                            item_id = item["event_id"]
                            if item_id != current_id:
                                if group:
                                    yield group
                                group = [item]
                                current_id = item_id
                            else:
                                group.append(item)

                    if group:
                        yield group
            case DataType.MEMORY:
                data_contents = self.data_source.get_memory_data()
                data_contents.sort(key=lambda x: x.get("event_id", " "))
                yield from [list(group) for _, group in itertools.groupby(data_contents, key=lambda x: x.get("event_id", " "))]
            case _:
                typing.assert_never(input_data_type)
