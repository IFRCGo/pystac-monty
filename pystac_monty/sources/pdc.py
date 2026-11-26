import json
import logging
import os
import re
import typing
import unicodedata
from datetime import datetime
from enum import Enum
from typing import Any, Generator, List, Optional, Union

import pytz
from markdownify import markdownify as md
from pydantic import BaseModel, ValidationError
from pystac import Asset, Item
from shapely.geometry import Point, mapping

from pystac_monty.extension import (
    HazardDetail,
    ImpactDetail,
    MontyEstimateType,
    MontyExtension,
    MontyImpactExposureCategory,
    MontyImpactExposureCatgoryLabel,
    MontyImpactType,
    MontyImpactTypeLabel,
)
from pystac_monty.geocoding import MontyGeoCoder
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import (
    DataType,
    GenericDataSource,
    MontyDataSourceV3,
    MontyDataTransformer,
    PDCDataSourceType,
)
from pystac_monty.sources.utils import unique_in_order
from pystac_monty.validators.pdc import AdminData, ExposureDetailValidator, HazardEventValidator

logger = logging.getLogger(__name__)
# Constants

STAC_EVENT_ID_PREFIX = "pdc-event-"
STAC_HAZARD_ID_PREFIX = "pdc-hazard-"
STAC_IMPACT_ID_PREFIX = "pdc-impact-"

# The text of each impact field in the title of its impact item.
PDC_IMPACT_FIELD_LABELS = {
    ("population", "total0_4"): "Population aged 0 to 4",
    ("population", "total5_9"): "Population aged 5 to 9",
    ("population", "total10_14"): "Population aged 10 to 14",
    ("population", "total15_19"): "Population aged 15 to 19",
    ("population", "total20_24"): "Population aged 20 to 24",
    ("population", "total25_29"): "Population aged 25 to 29",
    ("population", "total30_34"): "Population aged 30 to 34",
    ("population", "total35_39"): "Population aged 35 to 39",
    ("population", "total40_44"): "Population aged 40 to 44",
    ("population", "total45_49"): "Population aged 45 to 49",
    ("population", "total50_54"): "Population aged 50 to 54",
    ("population", "total55_59"): "Population aged 55 to 59",
    ("population", "total60_64"): "Population aged 60 to 64",
    ("population", "total65_Plus"): "Population aged 65 and over",
    ("population", "total"): "Total population",
    ("population", "households"): "Households",
    ("capital", "total"): "Total capital",
    ("capital", "school"): "School capital",
    ("capital", "hospital"): "Hospital capital",
}

IMPACT_TYPE_LABELS = MontyImpactTypeLabel()
IMPACT_CATEGORY_LABELS = MontyImpactExposureCatgoryLabel()

# PDC hazard types for which HIPs 2025 has no code at the granularity that PDC states.
# A hazard item must have one UNDRR-ISC 2025 code, so no item is made for these types.
# A specific code would add precision that PDC does not give.
PDC_TYPES_WITHOUT_UNDRR_2025_CODE = {
    "ACCIDENT": "general accident; the Technological family has no general code",
    "COMBAT": "the type does not say if the conflict is international (SO0101) or not (SO0102)",
    "CYBER": "the Cyberhazards cluster has no general code",
    "HIGHSURF": "HIPs 2025 has no code for wave action",
    "MANMADE": "general man-made hazard; the Technological family has no general code",
    "MARINE": "the Marine-related cluster has no general code",
    "OCCURRENCE": "the type has no hazard meaning",
    "WEAPONS": "the type does not say which weapon or which use",
}


class PDCHazard(BaseModel):
    type: str
    data: GenericDataSource


class PDCExposureDetail(BaseModel):
    type: str
    data: GenericDataSource


class PDCGeojson(BaseModel):
    type: str
    data: GenericDataSource


class PDCSourceType(Enum):
    HAZARD = "hazard"
    EXPOSURE_DETAIL = "exposure_detail"
    GEOJSON = "geojson"


class PDCDataSource(MontyDataSourceV3):
    type: PDCSourceType
    source_url: str
    uuid: str
    hazard_file_path: str
    exposure_detail_file_paths: List[str]
    geojson_path: str
    hazard_data: Union[str, dict]
    exposure_detail_data: List[dict]

    def __init__(self, data: PDCDataSourceType, eoapi_url: str | None = None):
        super().__init__(root=data, eoapi_url=eoapi_url)
        self.source_url = data.source_url
        self.uuid = data.uuid
        self.geojson_path = data.geojson_path
        self.exposure_detail_file_paths = []
        self.exposure_detail_data = []

        def handle_file_data():
            if os.path.isfile(data.hazard_data.path):
                self.hazard_file_path = data.hazard_data.path
            else:
                raise ValueError("File path does not exist")

            for exposure_detail_data in data.exposure_detail_data:
                if exposure_detail_data.data_type != DataType.FILE:
                    continue
                if os.path.isfile(exposure_detail_data.path):
                    self.exposure_detail_file_paths.append(exposure_detail_data.path)
                else:
                    raise ValueError("File path does not exist")

        def handle_memory_data(): ...

        input_data_type = data.hazard_data.data_type
        match input_data_type:
            case DataType.FILE:
                handle_file_data()
            case DataType.MEMORY:
                handle_memory_data()
            case _:
                typing.assert_never(input_data_type)

    def get_hazard_data(self) -> typing.Union[dict, str]:
        if self.root.hazard_data.data_type == DataType.FILE:
            with open(self.hazard_file_path, "r", encoding="utf-8") as f:
                self.hazard_data = json.loads(f.read())
        return self.hazard_data

    def get_exposure_detail_data(self) -> List[dict]:
        """Get all the exposure details (snapshots) of the hazard"""
        # NOTE: Each exposure detail has its own data type (independent of the hazard data)
        exposure_detail_data = []
        for exposure_detail in self.root.exposure_detail_data:
            if exposure_detail.data_type == DataType.FILE:
                with open(exposure_detail.path, "r", encoding="utf-8") as f:
                    exposure_detail_data.append(json.loads(f.read()))
            else:
                content = exposure_detail.content
                exposure_detail_data.append(json.loads(content) if isinstance(content, (str, bytes)) else content)
        self.exposure_detail_data = exposure_detail_data
        return self.exposure_detail_data

    def get_input_data_type(self) -> DataType:
        return self.root.hazard_data.data_type

    def get_source_url(self) -> str:
        return self.source_url

    def get_uuid(self) -> str:
        return self.uuid


class PDCTransformer(MontyDataTransformer):
    """Transform the source data into the STAC items"""

    hazard_profiles = MontyHazardProfiles()
    source_name = "pdc"
    source_doc = "docs/model/sources/PDC/README.md"

    def __init__(self, pdc_data_src: PDCDataSource, geocoder: MontyGeoCoder):
        super().__init__(pdc_data_src, geocoder)
        self.uuid = pdc_data_src.uuid
        self.hazards_data = pdc_data_src.get_hazard_data()
        self.exposure_details = pdc_data_src.get_exposure_detail_data()
        self.geojson_path = pdc_data_src.geojson_path

        # NOTE Setting 1 to the episode_number as it is missing in the source data.

        self.episode_number = 1
        self.hazard_data = self._get_hazard_data()

    def _get_hazard_data(self):
        """Get a single hazard data"""
        for item in self.hazards_data:
            if item["uuid"] == self.uuid:
                return item
        return {}

    def make_items(self):
        return list(self.get_stac_items())

    def get_stac_items(self) -> Generator[Item, None, None]:
        """Creates the STAC Items"""

        self.transform_summary.mark_as_started()

        hazard_item = self._get_hazard_data()
        if hazard_item:
            self.transform_summary.increment_rows()
            try:
                pdc_hazard_data = HazardEventValidator(**hazard_item)
                exposure_details = self._get_valid_exposure_details()

                # NOTE: Event and hazard items are created once for all the exposure details (snapshots)
                if event_item := self.make_source_event_item(pdc_hazard_data, exposure_details):
                    hazard_item = self.make_hazard_item(event_item, pdc_hazard_data)
                    impact_items = [
                        impact_item
                        for exposure_detail in exposure_details
                        for impact_item in self.make_impact_items(event_item, exposure_detail)
                    ]

                    all_items = [event_item, hazard_item] + impact_items
                    self.set_item_hrefs(items=all_items, eoapi_url=self.data_source.eoapi_url)
                    self.add_related_links(event_item, hazard_items=[hazard_item], impact_items=impact_items)

                    yield event_item
                    yield hazard_item
                    yield from impact_items
                else:
                    self.transform_summary.increment_failed_rows()
            except Exception:
                self.transform_summary.increment_failed_rows()
                logger.warning("Failed to process PDC data", exc_info=True)
            self.transform_summary.mark_as_complete()
        else:
            logger.warning("No Hazard items found")

    def _get_valid_exposure_details(self) -> List[ExposureDetailValidator]:
        """
        Validate the exposure details, earliest snapshot first.
        Invalid exposure details are skipped; the event and hazard items are still created.

        NOTE: Earliest first keeps monty:country_codes[0] (and so monty:corr_id) stable:
        a new snapshot can only append countries at the end.
        """
        exposure_details = []
        for exposure_detail_data in self.exposure_details:
            try:
                exposure_details.append(ExposureDetailValidator(**exposure_detail_data))
            except ValidationError:
                logger.warning("Invalid PDC exposure detail data, skipping impact items", exc_info=True)
        # NOTE: Sort by datetime (not the raw value) as the timestamp can be in milliseconds or seconds.
        # Sorting is stable, so snapshots with the same timestamp keep the input order.
        exposure_details = sorted(exposure_details, key=lambda e: self.get_exposure_datetime(e.timestamp))

        # NOTE: Snapshots with the same timestamp would create the same impact ids; keep the last one
        unique_exposure_details = {}
        for exposure_detail in exposure_details:
            timestamp_ms = self.get_exposure_timestamp_ms(exposure_detail.timestamp)
            if timestamp_ms in unique_exposure_details:
                logger.warning("Duplicate PDC exposure detail timestamp %s, keeping the last one", timestamp_ms)
            unique_exposure_details[timestamp_ms] = exposure_detail
        return list(unique_exposure_details.values())

    def make_source_event_item(
        self,
        pdc_hazard_data: HazardEventValidator,
        pdc_exposure_details: List[ExposureDetailValidator] | None = None,
    ):
        """
        Create an Event Item

        NOTE: Countries are collected from all the exposure details (earliest snapshot first)
        """
        pdc_exposure_details = pdc_exposure_details or []

        latitude = float(pdc_hazard_data.latitude)
        longitude = float(pdc_hazard_data.longitude)
        # Create the geojson point
        point = Point(longitude, latitude)
        geometry = mapping(point)
        bbox = [longitude, latitude, longitude, latitude]

        description = md(pdc_hazard_data.description).strip() or "NA"

        startdate = int(pdc_hazard_data.start_Date)
        enddate = int(pdc_hazard_data.end_Date)

        if startdate:
            startdate = pytz.utc.localize(datetime.fromtimestamp(startdate / 1_000))
        if enddate:
            enddate = pytz.utc.localize(datetime.fromtimestamp(enddate / 1_000))

        # NOTE: Exposure timestamp is intentionally not part of the event (and hazard) id,
        # so the same PDC hazard maps to the same item regardless of exposure availability.
        item_id = f"{STAC_EVENT_ID_PREFIX}{self.hazard_data['uuid']}-{self.hazard_data['hazard_ID']}"

        item = Item(
            id=item_id,
            geometry=geometry,
            bbox=bbox,
            datetime=startdate,
            properties={
                "title": pdc_hazard_data.hazard_Name,
                "description": description,
                "start_datetime": startdate.isoformat(),
                "end_datetime": enddate.isoformat(),
                "category_id": pdc_hazard_data.category_ID,
            },
        )

        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        all_iso3 = []
        for pdc_exposure_data in pdc_exposure_details:
            all_iso3.extend([admin.country for admin in pdc_exposure_data.totalByCountry if admin.country])
        if not all_iso3:
            # NOTE: Without exposure data (or countries in it), get the country from the hazard location
            all_iso3.append(self.geocoder.get_iso3_from_point(point) or "UNK")

        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.src_event_id = str(pdc_hazard_data.hazard_ID)
        monty.episode_number = self.episode_number
        # Source order, without duplicates: monty:corr_id uses country_codes[0].
        monty.country_codes = unique_in_order(all_iso3)

        hazard_codes = self._map_pdc_to_hazard_codes(hazard=pdc_hazard_data.type_ID)
        if not hazard_codes:
            # NOTE: The caller counts the hazard as a failed row.
            return None
        monty.hazard_codes = hazard_codes
        monty.hazard_codes = self.hazard_profiles.get_canonical_hazard_codes(item=item)

        # Generate keywords for discoverability
        hazard_keywords = self.hazard_profiles.get_keywords(monty.hazard_codes)
        country_keywords = [obj.admin0 for pdc_exposure_data in pdc_exposure_details for obj in pdc_exposure_data.totalByCountry]
        item.properties["keywords"] = unique_in_order(hazard_keywords + country_keywords)

        # TODO: Deal with correlation id if country_codes is a empty list
        if monty.country_codes:
            monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        if self.geojson_path:
            item.add_asset("Maps", Asset(href=self.geojson_path, media_type="geojson", title="Polygon"))

        if pdc_hazard_data.snc_url:
            item.add_asset("report", Asset(href=pdc_hazard_data.snc_url, media_type="html", title="Report"))
        return item

    def _map_pdc_to_hazard_codes(self, hazard: str) -> List[str] | None:
        """
        Map PDC hazard types to standard classification codes.
        Returns codes in order: [UNDRR-ISC 2025, EM-DAT, GLIDE]

        A type in PDC_TYPES_WITHOUT_UNDRR_2025_CODE gives None, and no item is made for it.

        The UNDRR-ISC 2025 code is the reference classification for the Monty extension.
        All three codes are included for maximum interoperability.

        Args:
            hazard: PDC hazard type (e.g., 'EARTHQUAKE', 'FLOOD')

        Returns:
            List of classification codes or None if not found
        """
        # Natural Hazards
        natural_hazards = {
            "AVALANCHE": ["MH0801", "nat-hyd-mmw-ava", "AV"],
            "BIOMEDICAL": ["BI0101", "nat-bio-epi-dis", "EP"],
            "DROUGHT": ["MH0401", "nat-cli-dro-dro", "DR"],
            "EARTHQUAKE": ["GH0101", "nat-geo-ear-gro", "EQ"],
            "EXTREMETEMPERATURE": ["MH0501", "nat-met-ext-hea", "HT"],  # Default to heat, may need logic for cold
            "FLOOD": ["MH0600", "nat-hyd-flo-flo", "FL"],
            "LANDSLIDE": ["GH0300", "nat-geo-mmd-lan", "LS"],
            "SEVEREWEATHER": ["MH0103", "nat-met-sto-sto", "ST"],
            "STORM": ["MH0103", "nat-met-sto-sto", "ST"],
            "TORNADO": ["MH0305", "nat-met-sto-tor", "TO"],
            "CYCLONE": ["MH0306", "nat-met-sto-tro", "TC"],
            "TSUNAMI": ["MH0705", "nat-geo-ear-tsu", "TS"],
            "VOLCANO": ["GH0201", "nat-geo-vol-vol", "VO"],
            "WILDFIRE": ["EN0205", "nat-cli-wil-wil", "WF"],
            "WINTERSTORM": ["MH0403", "nat-met-sto-bli", "OT"],
            "HIGHWIND": ["MH0301", "nat-met-sto-sto", "VW"],
        }

        # Geopolitical & Technological Hazards
        # The triplet is one row of the crosswalk. These rows have no EM-DAT key.
        tech_social_hazards = {
            "ACTIVESHOOTER": ["SO0301", "OT"],  # Violence
            "CIVILUNREST": ["SO0103", "CE"],  # Civil Unrest
            "POLITICALCONFLICT": ["SO0103", "CE"],  # Civil Unrest (nearest code)
            "TERRORISM": ["SO0301", "OT"],  # Violence
        }

        # Combine all mappings
        hazard_mapping = {**natural_hazards, **tech_social_hazards}

        if hazard in PDC_TYPES_WITHOUT_UNDRR_2025_CODE:
            logger.warning(f"PDC hazard type '{hazard}' has no UNDRR-ISC 2025 code. No item is made.")
            return None

        if hazard not in hazard_mapping:
            logger.warning(f"PDC hazard type '{hazard}' not found in UNDRR-ISC 2025 mapping.")
            return None

        return hazard_mapping[hazard]

    def make_hazard_item(self, event_item: Item, hazard_data: HazardEventValidator) -> Item:
        """Create Hazard Item"""
        if not event_item:
            return None

        hazard_item = event_item.clone()
        hazard_item.id = event_item.id.replace(STAC_EVENT_ID_PREFIX, STAC_HAZARD_ID_PREFIX)
        hazard_item.properties["roles"] = ["source", "hazard"]
        hazard_item.set_collection(self.get_hazard_collection())

        monty = MontyExtension.ext(hazard_item)
        monty.hazard_codes = [self.hazard_profiles.get_undrr_2025_code(hazard_codes=monty.hazard_codes)]
        # Hazard Detail
        monty.hazard_detail = HazardDetail(
            severity_value=0.1,  # 0.1 is passed inorder to pass validation it means None in this case
            severity_unit="PDC Severity Score",
            severity_label=hazard_data.severity_ID,
            estimate_type=MontyEstimateType.PRIMARY,
        )

        return hazard_item

    def get_nested_data(self, data: List[AdminData], keys) -> Optional[Any]:
        for key in keys:
            if isinstance(data, list):
                data = [item.__getattribute__(key) for item in data]
            elif hasattr(data, key):
                data = data.__getattribute__(key)
            else:
                return None
        return data

    def make_impact_items(self, event_item: Item, exposure_detail: ExposureDetailValidator | None) -> List[Item]:
        """Create Impact Items"""
        impact_fields = {
            ("population", "total0_4", "value"): (MontyImpactExposureCategory.CHILDREN_0_4, MontyImpactType.TOTAL_AFFECTED),
            ("population", "total5_9", "value"): (MontyImpactExposureCategory.CHILDREN_5_9, MontyImpactType.TOTAL_AFFECTED),
            ("population", "total10_14", "value"): (MontyImpactExposureCategory.CHILDREN_10_14, MontyImpactType.TOTAL_AFFECTED),
            ("population", "total15_19", "value"): (MontyImpactExposureCategory.CHILDREN_15_19, MontyImpactType.TOTAL_AFFECTED),
            ("population", "total20_24", "value"): (MontyImpactExposureCategory.ADULT_20_24, MontyImpactType.TOTAL_AFFECTED),
            ("population", "total25_29", "value"): (MontyImpactExposureCategory.ADULT_25_29, MontyImpactType.TOTAL_AFFECTED),
            ("population", "total30_34", "value"): (MontyImpactExposureCategory.ADULT_30_34, MontyImpactType.TOTAL_AFFECTED),
            ("population", "total35_39", "value"): (MontyImpactExposureCategory.ADULT_35_39, MontyImpactType.TOTAL_AFFECTED),
            ("population", "total40_44", "value"): (MontyImpactExposureCategory.ADULT_40_44, MontyImpactType.TOTAL_AFFECTED),
            ("population", "total45_49", "value"): (MontyImpactExposureCategory.ADULT_45_49, MontyImpactType.TOTAL_AFFECTED),
            ("population", "total50_54", "value"): (MontyImpactExposureCategory.ADULT_50_54, MontyImpactType.TOTAL_AFFECTED),
            ("population", "total55_59", "value"): (MontyImpactExposureCategory.ADULT_55_59, MontyImpactType.TOTAL_AFFECTED),
            ("population", "total60_64", "value"): (MontyImpactExposureCategory.ADULT_60_64, MontyImpactType.TOTAL_AFFECTED),
            ("population", "total65_Plus", "value"): (MontyImpactExposureCategory.ELDERLY, MontyImpactType.TOTAL_AFFECTED),
            ("population", "total", "value"): (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.TOTAL_AFFECTED),
            ("population", "households", "value"): (MontyImpactExposureCategory.HOUSEHOLDS, MontyImpactType.TOTAL_AFFECTED),
            ("capital", "total", "value"): (MontyImpactExposureCategory.GLOBAL_CURRENCY, MontyImpactType.LOSS_COST),
            ("capital", "school", "value"): (MontyImpactExposureCategory.SCHOOLS, MontyImpactType.TOTAL_AFFECTED),
            ("capital", "hospital", "value"): (MontyImpactExposureCategory.HOSPITALS, MontyImpactType.TOTAL_AFFECTED),
        }
        if not event_item:
            return None

        impact_items = []
        if not exposure_detail:
            return impact_items
        impact_id_prefix = (
            f"{event_item.id.replace(STAC_EVENT_ID_PREFIX, STAC_IMPACT_ID_PREFIX)}"
            f"-{self.get_exposure_timestamp_ms(exposure_detail.timestamp)}-{self.episode_number}"
        )
        impact_datetime = self.get_exposure_datetime(exposure_detail.timestamp)
        admin_id_parts = self.get_admin_id_parts(exposure_detail.totalByAdmin)
        for field_key, field_values in impact_fields.items():
            for admin_item, admin_id_part in zip(exposure_detail.totalByAdmin, admin_id_parts):
                value = self.get_nested_data(admin_item, field_key)
                if value is None:
                    # NOTE: Skip only this impact item when the value is not available
                    continue
                impact_item = event_item.clone()
                impact_item.id = f"{impact_id_prefix}-{'-'.join(field_key[:-1])}-{admin_id_part}"
                impact_item.set_collection(self.get_impact_collection())
                impact_item.properties["roles"] = ["source", "impact"]
                # NOTE: Impact is a snapshot of the exposure; start/end datetime are kept from the event
                impact_item.datetime = impact_datetime
                monty = MontyExtension.ext(impact_item)
                monty.country_codes = [admin_item.country]  # type: ignore
                # Impact Detail
                category, impact_type = field_values
                monty.impact_detail = self.get_impact_detail(category, impact_type, value)
                # Title: the event title, the impact field and the place of the row
                field_label = PDC_IMPACT_FIELD_LABELS[field_key[:-1]]
                place = ", ".join(part for part in (admin_item.admin2, admin_item.admin1, admin_item.country) if part)
                impact_item.properties["title"] = f"{event_item.properties['title']} - {field_label} ({place})"
                # Keywords: those of the event, then the labels of the impact type and of the exposure category
                impact_item.properties["keywords"] = unique_in_order(
                    event_item.properties.get("keywords", [])
                    + [IMPACT_TYPE_LABELS[impact_type], IMPACT_CATEGORY_LABELS[category]]
                )
                impact_items.append(impact_item)
        return impact_items

    @staticmethod
    def get_exposure_timestamp_ms(timestamp: float) -> int:
        """Exposure snapshot timestamp (milliseconds or seconds since epoch) to milliseconds since epoch"""
        # NOTE: PDC sends milliseconds; values below 1e11 (year 5138 in seconds) are treated as seconds
        timestamp = float(timestamp)
        milliseconds = timestamp if timestamp >= 1e11 else timestamp * 1_000
        return int(round(milliseconds))

    @classmethod
    def get_exposure_datetime(cls, timestamp: float) -> datetime:
        """Exposure snapshot timestamp (milliseconds or seconds since epoch) to UTC datetime"""
        return datetime.fromtimestamp(cls.get_exposure_timestamp_ms(timestamp) / 1_000, tz=pytz.utc)

    @staticmethod
    def get_admin_id_part(admin_item: AdminData) -> str:
        """Id part of an admin row: country, and admin1/admin2 when available (unique per totalByAdmin row)"""
        parts = [admin_item.country]
        for admin in (admin_item.admin1, admin_item.admin2):
            if admin and (admin_part := PDCTransformer.get_ascii_id_part(admin)):
                parts.append(admin_part)
        return "-".join(parts)

    @staticmethod
    def get_ascii_id_part(name: str) -> str:
        """Lowercase ASCII id part of a name (eg: "San Martín" -> "san-martin"); empty if nothing is left"""
        ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
        return re.sub(r"[^a-z0-9_.]+", "-", ascii_name.lower()).strip("-")

    @classmethod
    def get_admin_id_parts(cls, admin_items: List[AdminData]) -> List[str]:
        """
        Id part of each totalByAdmin row.

        The last part is 0 for a row without collision, and the row index for a row with the same
        country/admin1/admin2 as a previous row (eg: same country without admin1/admin2).
        """
        id_parts: List[str] = []
        seen = set()
        for index, admin_item in enumerate(admin_items):
            id_part = cls.get_admin_id_part(admin_item)
            if id_part in seen:
                logger.warning(
                    "PDC totalByAdmin rows with the same country/admin1/admin2 (%s); using row index %s in the impact id",
                    id_part,
                    index,
                )
                id_parts.append(f"{id_part}-{index}")
            else:
                seen.add(id_part)
                id_parts.append(f"{id_part}-0")
        return id_parts

    def get_impact_detail(self, category: MontyImpactExposureCategory, impact_type: MontyImpactType, value: float):
        """Create an Impact detail object"""
        return ImpactDetail(category=category, type=impact_type, value=value, unit=None, estimate_type=MontyEstimateType.PRIMARY)
