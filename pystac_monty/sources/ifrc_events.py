import json
import logging
import os
import re
import typing
from dataclasses import dataclass, field
from typing import Dict, List

from pystac import Item
from shapely.geometry import mapping, shape
from shapely.ops import unary_union

from pystac_monty.extension import ImpactDetail, MontyEstimateType, MontyExtension, MontyImpactExposureCategory, MontyImpactType
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import (
    DataType,
    File,
    GenericDataSource,
    Memory,
    MontyDataSourceV3,
    MontyDataTransformer,
    with_processing_extension,
)
from pystac_monty.validators.ifrc import IFRCsourceValidator

logger = logging.getLogger(__name__)

STAC_EVENT_ID_PREFIX = "ifrcevent-event-"
STAC_IMPACT_ID_PREFIX = "ifrcevent-impact-"

# IFRC DREF disaster type (dtype.name) -> [UNDRR-ISC 2025, EM-DAT, GLIDE] hazard codes.
# Source of truth: monty-stac-extension docs/model/sources/IFRC-DREF/README.md
# ("Hazard Type Mapping"). "Fire" and "Cyclone" are defaults refined by name-based
# rules below ("the fire rule" / "the cyclone rule" in that doc).
IFRC_HAZARD_CODES: Dict[str, List[str]] = {
    "Earthquake": ["GH0101", "nat-geo-ear-gro", "EQ"],
    "Cyclone": ["MH0306", "nat-met-sto-tro", "TC"],  # Depression or Cyclone (default)
    # General eruption: HIP 2025 has no volcanic chapeau (GH0201-GH0205 only), so
    # GH0201 carries the general case -- not because the event is a lava flow.
    "Volcanic Eruption": ["GH0201", "nat-geo-vol-vol", "VO"],
    "Tsunami": ["MH0705", "nat-geo-ear-tsu", "TS"],
    "Flood": ["MH0600", "nat-hyd-flo-flo", "FL"],  # Flooding (chapeau)
    "Cold Wave": ["MH0502", "nat-met-ext-col", "CW"],
    "Fire": ["EN0205", "nat-cli-wil-wil", "WF"],  # Wildfires (default)
    "Heat Wave": ["MH0501", "nat-met-ext-hea", "HT"],
    "Drought": ["MH0401", "nat-cli-dro-dro", "DR"],
    "Storm Surge": ["MH0703", "nat-met-sto-sur", "SS"],
    "Landslide": ["GH0300", "nat-geo-mmd-lan", "LS"],  # Gravitational Mass Movement (chapeau)
    "Pluvial/Flash Flood": ["MH0603", "nat-hyd-flo-fla", "FF"],
    "Epidemic": ["BI0101", "nat-bio-epi-dis", "EP"],
    # Societal hazard type has no GLIDE/EM-DAT row in the cross-classification mapping.
    "Civil Unrest": ["SO0103"],
    "Insect Infestation": ["BI0401", "nat-bio-inf-inf", "IN"],
}

# "The fire rule": GO's single "Fire" type is dominated by wildfires, so EN0205/WF is
# the default and these patterns refine to the industrial/structural EM-DAT split.
# Checked against the event `name` only -- `summary` is narrative impact prose that
# describes what a wildfire destroyed, not what kind of fire it was.
FIRE_INDUSTRIAL_PATTERN = re.compile(r"\b(factory|industrial|plant|refinery|warehouse)\b", re.IGNORECASE)
FIRE_MISC_PATTERN = re.compile(r"\b(landfill|market|building|structural|residential|apartment|camp|slum)\b", re.IGNORECASE)

# "The cyclone rule": GO's "Cyclone" type is not exclusively tropical, so MH0306
# (Depression or Cyclone) is the default and these patterns refine using the name.
CYCLONE_EXTRATROPICAL_PATTERN = re.compile(r"extratropical|extra-tropical", re.IGNORECASE)
CYCLONE_TROPICAL_PATTERN = re.compile(r"hurricane|typhoon|tropical|\bTCs?\b", re.IGNORECASE)


@dataclass
class IFRCEventDataSource(MontyDataSourceV3):
    file_path: str = field(init=False)
    data: str | dict = field(init=False)
    input_data: File | Memory = field(init=False)

    def __init__(self, data: GenericDataSource, eoapi_url: str | None = None):
        super().__init__(root=data, eoapi_url=eoapi_url)

        def handle_file_data():
            if os.path.isfile(self.input_data.path):
                self.file_path = self.input_data.path
            else:
                raise ValueError("File path does not exist")

        def handle_memory_data():
            if isinstance(self.input_data.content, list):
                self.data = self.input_data.content
            else:
                raise ValueError("Data must be list of dictionary")

        input_data_type = self.input_data.data_type
        match input_data_type:
            case DataType.FILE:
                handle_file_data()
            case DataType.MEMORY:
                handle_memory_data()
            case _:
                typing.assert_never(input_data_type)

    def get_data(self) -> dict | str:
        if self.input_data.data_type == DataType.FILE:
            return self.file_path
        return self.data

    def get_input_data_type(self) -> DataType:
        return self.input_data.data_type


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
                appeals: list[dict] | None = item.get("appeals")
                if not appeals:
                    continue

                appeal_set = {appeal["atype"] for appeal in appeals if appeal.get("atype") not in [None, ""]}
                # Only allow types DREF(0) and Emergency Appeal(1)
                # Note: Might need to sync with the request query in montandon-etl repo
                if not appeal_set.issubset({0, 1}):
                    continue

                dtype: dict | None = item.get("dtype")
                if not dtype:
                    continue

                dtype_name: str | None = dtype.get("name")
                if not self.check_accepted_disaster_types(dtype_name):
                    logger.warning(f"The disaster type {dtype_name} is not processed. Ignoring")
                    continue
                filtered_ifrcevent_data.append(item)
            logger.info(f"Total items to process: {len(filtered_ifrcevent_data)}")

            self.transform_summary.mark_as_started()
            for data in filtered_ifrcevent_data:
                self.transform_summary.increment_rows()
                try:
                    ifrcdata = IFRCsourceValidator(**data)
                    if event_item := self.make_source_event_item(ifrcdata):
                        impact_items = self.make_impact_items(event_item, ifrcdata)

                        all_items = [event_item] + impact_items
                        self.set_item_hrefs(items=all_items, eoapi_url=self.data_source.eoapi_url)
                        self.add_related_links(event_item=event_item, impact_items=impact_items)

                        yield event_item
                        yield from impact_items
                    else:
                        self.transform_summary.increment_failed_rows()
                except Exception:
                    self.transform_summary.increment_failed_rows()
                    e_id = data.get("id", "N/A")
                    logger.warning(f"Failed to process IFRC events data with id {e_id}", exc_info=True)
            self.transform_summary.mark_as_complete()

    def get_stac_items_from_memory(self) -> typing.Generator[Item, None, None]:
        data = self.data_source.get_data()
        filtered_ifrcevent_data = []
        for item in data:
            appeals: list[dict] | None = item.get("appeals")
            if not appeals:
                continue

            appeal_set = {appeal["atype"] for appeal in appeals if appeal.get("atype") not in [None, ""]}

            # Only allow types DREF(0) and Emergency Appeal(1)
            # Note: Might need to sync with the request query in montandon-etl repo
            if not appeal_set.issubset({0, 1}):
                continue

            dtype: dict | None = item.get("dtype")
            if not dtype:
                continue

            dtype_name: str | None = dtype.get("name")
            if not self.check_accepted_disaster_types(dtype_name):
                logger.warning(f"The disaster type {dtype_name} is not processed. Ignoring")
                continue
            filtered_ifrcevent_data.append(item)
            logger.info(f"Total items to process: {len(filtered_ifrcevent_data)}")

        self.transform_summary.mark_as_started()
        for data in filtered_ifrcevent_data:
            self.transform_summary.increment_rows()
            try:
                ifrcdata = IFRCsourceValidator(**data)
                if event_item := self.make_source_event_item(ifrcdata):
                    impact_items = self.make_impact_items(event_item, ifrcdata)

                    all_items = [event_item] + impact_items
                    self.set_item_hrefs(items=all_items, eoapi_url=self.data_source.eoapi_url)
                    self.add_related_links(event_item=event_item, impact_items=impact_items)

                    yield event_item
                    yield from impact_items
                else:
                    self.transform_summary.increment_failed_rows()
            except Exception:
                self.transform_summary.increment_failed_rows()
                e_id = data.get("id", "N/A")
                logger.warning(f"Failed to process IFRC events data with id {e_id}", exc_info=True)
        self.transform_summary.mark_as_complete()

    @with_processing_extension
    def get_stac_items(self) -> typing.Generator[Item, None, None]:
        data_type = self.data_source.get_input_data_type()
        match data_type:
            case DataType.FILE:
                yield from self.get_stac_items_from_file()
            case DataType.MEMORY:
                yield from self.get_stac_items_from_memory()
            case _:
                typing.assert_never(data_type)

    def _get_geometry(self, affected_iso3_or_countries: list[str]) -> tuple[dict[str, str | list] | None, list | None]:
        """Generate the geometrical polygon or multipolygon of a country or countries involved in the event."""
        polygon_geometries = []
        for iso3_or_country in affected_iso3_or_countries:
            geom_data = self.geocoder.get_geometry_from_iso3(iso3_or_country, simplified=True)
            if not geom_data:
                geom_data = self.geocoder.get_geometry_by_country_name(iso3_or_country, simplified=True)
            if geom_data:
                polygon_geometries.append(geom_data["geometry"])
        if polygon_geometries:
            combined_geometry = unary_union([shape(g) for g in polygon_geometries])
            geometry = mapping(combined_geometry)
            bbox = list(combined_geometry.bounds)
        else:
            logger.warning("No geometry polygons found. Skipping items formation.")
            return None, None
        return geometry, bbox

    def make_source_event_item(self, data: IFRCsourceValidator) -> Item | None:
        """Create an event item"""
        geometry = None
        bbox = None
        if data.countries:
            affected_iso3_or_countries = [item.iso3 if item.iso3 else item.name for item in data.countries]
            geometry, bbox = self._get_geometry(affected_iso3_or_countries=affected_iso3_or_countries)
        else:
            raise ValueError("Empty Countries; cannot generate geometry and bbox")
        if not geometry or not bbox:
            return None
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
        monty.src_event_id = str(data.id)
        monty.episode_number = 1  # IFRC DREF doesn't have episodes
        monty.hazard_codes = self.map_ifrc_to_hazard_codes(data.dtype.name, data.name)
        monty.hazard_codes = self.hazard_profiles.get_canonical_hazard_codes(item=item)

        monty.country_codes = [country.iso3 for country in data.countries]

        hazard_keywords = self.hazard_profiles.get_keywords(monty.hazard_codes)
        country_keywords = [country.name for country in data.countries] if data.countries else []
        item.properties["keywords"] = list(set(hazard_keywords + country_keywords))

        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)
        # Set collection and roles
        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]
        return item

    def map_ifrc_to_hazard_codes(self, classification_key: str, name: str = "") -> List[str]:
        """
        Map IFRC DREF disaster type names to standard hazard codes.
        Returns codes in order: [UNDRR-ISC 2025, EM-DAT, GLIDE]

        The UNDRR-ISC 2025 code is the reference classification for the Monty extension.
        All three codes are included for maximum interoperability, except "Civil Unrest"
        (Societal hazards have no GLIDE/EM-DAT row in the cross-classification mapping).

        "Fire" and "Cyclone" default to a general code and are refined using the event
        `name` -- see "the fire rule" / "the cyclone rule" in monty-stac-extension's
        docs/model/sources/IFRC-DREF/README.md, which is the source of truth for this
        mapping.

        Args:
            classification_key: IFRC disaster type name (e.g., 'Flood', 'Earthquake')
            name: Event name, used to refine the "Fire" and "Cyclone" defaults

        Returns:
            List of classification codes [2025, EM-DAT, GLIDE]
        """
        if classification_key == "Fire":
            if FIRE_INDUSTRIAL_PATTERN.search(name):
                return ["TL0305", "tec-ind-fir-fir", "FR"]
            if FIRE_MISC_PATTERN.search(name):
                return ["TL0305", "tec-mis-fir-fir", "FR"]
        elif classification_key == "Cyclone":
            if CYCLONE_EXTRATROPICAL_PATTERN.search(name):
                return ["MH0307", "nat-met-sto-ext", "EC"]
            if CYCLONE_TROPICAL_PATTERN.search(name):
                return ["MH0309", "nat-met-sto-tro", "TC"]

        if classification_key not in IFRC_HAZARD_CODES:
            logger.warning(f"IFRC disaster type '{classification_key}' not found in UNDRR-ISC 2025 mapping.")

        return IFRC_HAZARD_CODES.get(classification_key, [])

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

        for field_report in ifrcevent_data.field_reports:
            iso3_list = [country.iso3 for country in field_report.countries]
            geometry = None
            bbox = None
            geometry_computed = False

            for impact_field, (category, impact_type) in impact_field_category_map.items():
                # only build the item if atleast one impact value is not null
                value = None
                for field_name in impact_field:
                    value = getattr(field_report, field_name)
                    if value:
                        break

                if not value:
                    continue

                if not geometry_computed:
                    # NOTE It is likely that the impacted countries might be different or just a part of where the event occurred
                    # The field reports may contain different country/regions.
                    affected_iso3_or_countries = [item.iso3 if item.iso3 else item.name for item in field_report.countries]
                    geometry, bbox = self._get_geometry(affected_iso3_or_countries=affected_iso3_or_countries)
                    geometry_computed = True

                impact_item = event_item.clone()
                impact_item.id = f"{STAC_IMPACT_ID_PREFIX}{ifrcevent_data.id}-{impact_type}-{field_report.id}"
                impact_item.properties["roles"] = ["source", "impact"]
                impact_item.set_collection(self.get_impact_collection())

                # NOTE if the geometry and bbox can be generated from field report, use it
                # Otherwise fallback to using them from the event item
                if geometry and bbox:
                    impact_item.geometry = geometry
                    impact_item.bbox = bbox

                monty = MontyExtension.ext(impact_item)
                # If iso3_list is non-empty, use it, if not,  fallback to using the countries list from event item
                if iso3_list:
                    monty.country_codes = iso3_list

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

    def check_accepted_disaster_types(self, disaster: str | None) -> bool:
        if not disaster:
            return False

        # Derived from IFRC_HAZARD_CODES so the accepted-types list and the hazard
        # code mapping can never drift apart (see monty-stac-extension#96).
        return disaster in IFRC_HAZARD_CODES
