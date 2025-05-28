import json
import logging
import os
import typing
from typing import List, Union

from pystac import Item

from pystac_monty.extension import ImpactDetail, MontyEstimateType, MontyExtension, MontyImpactExposureCategory, MontyImpactType
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import DataType, File, GenericDataSource, Memory, MontyDataSourceV3, MontyDataTransformer
from pystac_monty.validators.ifrc import IFRCsourceValidator

logger = logging.getLogger(__name__)

STAC_EVENT_ID_PREFIX = "ifrcevent-event-"
STAC_IMPACT_ID_PREFIX = "ifrcevent-impact-"


class IFRCEventDataSource(MontyDataSourceV3):
    file_path: str
    source_url: str
    data: Union[str, dict]
    data_source: Union[File, Memory]

    def __init__(self, data: GenericDataSource):
        # FIXME: Why do we load using json
        super().__init__(data)
        self.source_url = data.source_url
        self.data_source = data.data_source

        def handle_file_data():
            if os.path.isfile(self.data_source.path):
                self.file_path = self.data_source.path
            else:
                raise ValueError("File path does not exists", exc_info=True)

        def handle_memory_data():
            if isinstance(self.data_source.content, dict):
                self.data = self.data_source.content
            else:
                raise ValueError("Data must be in Json", exc_info=True)

        input_data_type = self.data_source.data_type
        match input_data_type:
            case DataType.FILE:
                handle_file_data()
            case DataType.MEMORY:
                handle_memory_data()
            case _:
                typing.assert_never(input_data_type)

    def get_data(self) -> Union[dict, str]:
        if self.data_source.data_type == DataType.FILE:
            return self.file_path
        return self.data

    def get_input_data_type(self) -> DataType:
        return self.data_source.data_type


class IFRCEventTransformer(MontyDataTransformer[IFRCEventDataSource]):
    hazard_profiles = MontyHazardProfiles()
    source_name = "ifrcevent"

    # FIXME: This is not used anymore
    def make_items(self):
        return list(self.get_stac_items())

    def get_stac_items_from_file(self) -> typing.Generator[Item, None, None]:
        data_path = self.data_source.get_data()
        with open(data_path, "r", encoding="utf-8") as f:
            filtered_ifrcevent_data = []
            for item in json.load(f):
                appeals: list[typing.Dict] | None = item.get("appeals")
                if not appeals:
                    continue

                first_appeal: typing.Dict = appeals[0]
                if first_appeal.get("atype", None) not in {0, 1}:
                    continue

                dtype: typing.Dict | None = item.get("dtype")
                if not dtype:
                    continue

                dtype_name: str | None = dtype.get("name")
                if not self.check_accepted_disaster_types(dtype_name):
                    continue
                filtered_ifrcevent_data.append(item)

            self.transform_summary.mark_as_started()
            for data in filtered_ifrcevent_data:
                self.transform_summary.increment_rows()
                try:
                    ifrcdata = IFRCsourceValidator(**data)
                    if event_item := self.make_source_event_item(ifrcdata):
                        yield event_item
                        yield from self.make_impact_items(event_item, ifrcdata)
                    else:
                        self.transform_summary.increment_failed_rows()
                except Exception:
                    self.transform_summary.increment_failed_rows()
                    logger.error("Failed to process ifrc events", exc_info=True)
            self.transform_summary.mark_as_complete()

    def get_stac_items_from_memory(self) -> typing.Generator[Item, None, None]:
        data = self.data_source.get_data()
        filtered_ifrcevent_data = []
        for item in data:
            appeals: list[typing.Dict] | None = item.get("appeals")
            if not appeals:
                continue

            first_appeal: typing.Dict = appeals[0]
            if first_appeal.get("atype", None) not in {0, 1}:
                continue

            dtype: typing.Dict | None = item.get("dtype")
            if not dtype:
                continue

            dtype_name: str | None = dtype.get("name")
            if not self.check_accepted_disaster_types(dtype_name):
                continue
            filtered_ifrcevent_data.append(item)

        self.transform_summary.mark_as_started()
        for data in filtered_ifrcevent_data:
            self.transform_summary.increment_rows()
            try:
                ifrcdata = IFRCsourceValidator(**data)
                if event_item := self.make_source_event_item(ifrcdata):
                    yield event_item
                    yield from self.make_impact_items(event_item, ifrcdata)
                else:
                    self.transform_summary.increment_failed_rows()
            except Exception:
                self.transform_summary.increment_failed_rows()
                logger.error("Failed to process ifrc events", exc_info=True)
        self.transform_summary.mark_as_complete()

    def get_stac_items(self) -> typing.Generator[Item, None, None]:
        data_type = self.data_source.get_input_data_type()
        match data_type:
            case DataType.FILE:
                yield from self.get_stac_items_from_file()
            case DataType.MEMORY:
                yield from self.get_stac_items_from_memory()
            case _:
                typing.assert_never(data_type)

    def make_source_event_item(self, data: IFRCsourceValidator) -> Item:
        """Create an event item"""
        geometry = None
        bbox = None

        if data.countries:
            geom_data = self.geocoder.get_geometry_from_iso3(data.countries[0].iso3)
            if not geom_data:
                geom_data = self.geocoder.get_geometry_by_country_name(data.countries[0].name)
            if geom_data:
                geometry = geom_data["geometry"]
                bbox = geom_data["bbox"]
            else:
                raise ValueError("No geometry data")
        else:
            raise ValueError("Empty Countries; cannot generate geometry and bbox")

        # start_date = datetime.fromisoformat(data["disaster_start_date"])
        start_date = data.disaster_start_date
        # Create item
        item = Item(
            id=f"{STAC_EVENT_ID_PREFIX}{data.id}",
            geometry=geometry,
            bbox=bbox,
            datetime=start_date,
            properties={
                "title": data.name,
                "description": data.summary.strip() if data.summary.strip() != "" else "NA",
                "start_datetime": start_date.isoformat(),
                # NOTE: source doesnot provide disaster end date so we assume startdate as end date
                "end_datetime": start_date.isoformat(),
            },
        )

        # Add Monty extension
        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = 1  # IFRC DREF doesn't have episodes
        monty.hazard_codes = self.map_ifrc_to_hazard_codes(data.dtype.name)
        monty.country_codes = [country.iso3 for country in data.countries]
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)
        # Set collection and roles
        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]
        return item

    def map_ifrc_to_hazard_codes(self, classification_key: str) -> List[str]:
        """
        Map IFRC DREF & EA classification key to UNDRR-ISC 2020 Hazard codes

        Args:
            classification_key: dtype name (e.g., 'Flood')

        Returns:
            List of UNDRR-ISC hazard codes
        """

        # IFRC DREF hazards classification mapping to UNDRR-ISC codes
        mapping = {
            "Earthquake": ["GH0001", "GH0002", "GH0003", "GH0004", "GH0005"],
            "Cyclone": ["MH0030", "MH0031", "MH0032"],
            "Volcanic Eruption": ["GH009", "GH0013", "GH0014", "GH0015", "GH0016"],
            "Tsunami": ["MH0029", "GH0006"],
            "Flood": ["FL"],  # General flood
            "Cold Wave": ["MH0040"],
            "Fire": ["FR", "tec-ind-fir-fir"],
            "Heat Wave": ["MH0047"],
            "Drought": ["MH0035"],
            "Storm Surge": ["MH0027"],
            "Landslide": ["GH0007"],
            "Flash Flood": ["MH0006"],
            "Epidemic": ["nat-bio-epi-dis"],  # General epidemic
        }

        if classification_key in mapping:
            return mapping[classification_key]

        return []

    def make_impact_items(self, event_item: Item, ifrcevent_data: IFRCsourceValidator) -> List[Item]:
        """Create impact items"""
        if not ifrcevent_data.field_reports:
            return []

        items = []
        # Note that the monty impact types should be unique in the mapping
        # to ensure that the generated impact ids and items are unique
        impact_field_category_map = {
            ("num_dead", "gov_num_dead", "other_num_dead"): (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.DEATH),
            ("num_displaced", "gov_num_displaced", "other_num_displaced"): (
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.TOTAL_DISPLACED_PERSONS,
            ),
            ("num_injured", "gov_num_injured", "other_num_injured"): (
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.INJURED,
            ),
            ("num_missing", "gov_num_missing", "other_num_missing"): (
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.MISSING,
            ),
            ("num_affected", "gov_num_affected", "other_num_affected"): (
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.TOTAL_AFFECTED,
            ),
            ("num_assisted", "gov_num_assisted", "other_num_assisted"): (
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.ASSISTED,
            ),
            ("num_potentially_affected", "gov_num_potentially_affected", "other_num_potentially_affected"): (
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.POTENTIALLY_AFFECTED,
            ),
            ("num_highest_risk", "gov_num_highest_risk", "other_num_highest_risk"): (
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.HIGHEST_RISK,
            ),
        }

        for impact_field, (category, impact_type) in impact_field_category_map.items():
            impact_item = event_item.clone()
            impact_item.id = f"{STAC_IMPACT_ID_PREFIX}-{ifrcevent_data.id}-{impact_type}"
            impact_item.properties["roles"] = ["source", "impact"]
            impact_item.set_collection(self.get_impact_collection())

            monty = MontyExtension.ext(impact_item)

            # only save impact value if not null
            value = None
            for field in impact_field:
                value = getattr(ifrcevent_data.field_reports[0], field)
                if value:
                    break

            if not value:
                continue

            monty.impact_detail = self.get_impact_details(category, impact_type, value)
            items.append(impact_item)

        return items

    def get_impact_details(self, category, impact_type, value, unit=None):
        """Returns the impact details"""
        return ImpactDetail(
            category=category,
            type=impact_type,
            value=value,
            unit=unit,
            estimate_type=MontyEstimateType.PRIMARY,
        )

    # FIXME: not used here
    def check_accepted_disaster_types(self, disaster: str | None):
        if not disaster:
            return []

        # Filter out relevant disaster types
        monty_accepted_disaster_types = [
            "Earthquake",
            "Cyclone",
            "Volcanic Eruption",
            "Tsunami",
            "Flood",
            "Cold Wave",
            "Fire",
            "Heat Wave",
            "Drought",
            "Storm Surge",
            "Landslide",
            "Flash Flood",
            "Epidemic",
        ]
        return disaster in monty_accepted_disaster_types
