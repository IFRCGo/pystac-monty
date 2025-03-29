import json
import logging
import os
from datetime import datetime
from typing import Any, Generator, List, Optional

import pytz
from markdownify import markdownify as md
from pystac import Asset, Item
from shapely.geometry import Point, mapping

from pystac_monty.validators.pdc import AdminData, ExposureDetailValidator, HazardEventValidator
from pystac_monty.extension import (
    HazardDetail,
    ImpactDetail,
    MontyEstimateType,
    MontyExtension,
    MontyImpactExposureCategory,
    MontyImpactType,
)
from pystac_monty.geocoding import MontyGeoCoder
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import MontyDataSource, MontyDataTransformer


logger = logging.getLogger(__name__)
# Constants

STAC_EVENT_ID_PREFIX = "pdc-event-"
STAC_HAZARD_ID_PREFIX = "pdc-hazard-"
STAC_IMPACT_ID_PREFIX = "pdc-impact-"


class PDCDataSource(MontyDataSource):
    """PDC Data from the source"""

    def __init__(self, source_url: str, data: Any):
        super().__init__(source_url, data)
        self.data = json.loads(data)


class PDCTransformer(MontyDataTransformer):
    """Transform the source data into the STAC items"""

    hazard_profiles = MontyHazardProfiles()
    source_name = "pdc"

    def __init__(self, pdc_data_src: PDCDataSource, geocoder: MontyGeoCoder):
        super().__init__(pdc_data_src, geocoder)
        self.config_data = pdc_data_src.data

        self.hazards_data = []
        self.exposure_detail = {}
        self.geojson_data = {}

        if "hazards_file_path" in self.config_data and os.path.exists(self.config_data["hazards_file_path"]):
            with open(self.config_data["hazards_file_path"], "r", encoding="utf-8") as f:
                self.hazards_data = json.loads(f.read())

        if "exposure_detail_file_path" in self.config_data and os.path.exists(self.config_data["exposure_detail_file_path"]):
            with open(self.config_data["exposure_detail_file_path"], "r", encoding="utf-8") as f:
                self.exposure_detail = json.loads(f.read())

        self.uuid = self.config_data.get("uuid", None)

        # NOTE Assigning -1 to episode_number incase of failure just to ignore the item formation (see make_items method)
        try:
            self.episode_number = int(float(self.config_data.get("exposure_timestamp", -1)))
        except ValueError:
            self.episode_number = -1

        if "geojson_file_path" in self.config_data and os.path.exists(self.config_data["geojson_file_path"]):
            with open(self.config_data["geojson_file_path"], "r", encoding="utf-8") as f:
                self.geojson_data = json.loads(f.read())

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

        pdc_hazard_data = self.hazards_data
        pdc_exposure_data = self.exposure_detail
        self.transform_summary.mark_as_started()

        for data in pdc_hazard_data:
            self.transform_summary.increment_rows()
            try:
                def parse_data(data: dict, validator):
                    obj = validator(**data)
                    return obj

                pdc_hazard_data = parse_data(data, HazardEventValidator)
                exposure_detail = parse_data(pdc_exposure_data, ExposureDetailValidator)

                if event_item := self.make_source_event_item(pdc_hazard_data, exposure_detail):
                    yield event_item
                    yield self.make_hazard_item(event_item, pdc_hazard_data)
                    yield from self.make_impact_items(event_item, exposure_detail)
                else:
                    self.transform_summary.increment_failed_rows()
            except Exception:
                self.transform_summary.increment_failed_rows()
                logger.error("Failed to process pdc", exc_info=True)
        self.transform_summary.mark_as_complete()

    def make_source_event_item(self, pdc_hazard_data: HazardEventValidator, pdc_exposure_data: ExposureDetailValidator):
        """Create an Event Item"""

        latitude = float(pdc_hazard_data.latitude)
        longitude = float(pdc_hazard_data.longitude)
        # Create the geojson point
        point = Point(longitude, latitude)
        geometry = mapping(point)
        bbox = [longitude, latitude, longitude, latitude]

        description = md(pdc_hazard_data.description).strip() or "NA"

        startdate = int(pdc_hazard_data.create_Date)
        enddate = int(pdc_hazard_data.end_Date)

        if startdate:
            startdate = pytz.utc.localize(datetime.fromtimestamp(startdate / 1_000))
        if enddate:
            enddate = pytz.utc.localize(datetime.fromtimestamp(enddate / 1_000))

        item = Item(
            id=f"{STAC_EVENT_ID_PREFIX}{pdc_hazard_data.uuid}-{pdc_hazard_data.hazard_ID}",
            geometry=geometry,
            bbox=bbox,
            datetime=startdate,
            properties={
                "title": pdc_hazard_data.hazard_Name,
                "description": description,
                "start_datetime": startdate.isoformat(),
                "end_datetime": enddate.isoformat(),
                "category_id": pdc_hazard_data.category_ID,
                "geometry_geojson": self.geojson_data,
            },
        )

        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        all_iso3 = []
        if pdc_exposure_data.totalByAdmin:
            all_iso3.extend([admin.country for admin in pdc_exposure_data.totalByAdmin if admin.country])
        if not all_iso3:
            return None

        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = self.episode_number
        monty.country_codes = list(set(all_iso3))

        monty.hazard_codes = self._map_pdc_to_hazard_codes(hazard=pdc_hazard_data.type_ID)
        # TODO: Deal with correlation id if country_codes is a empty list
        if monty.country_codes:
            monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        if pdc_hazard_data.snc_url:
            item.add_asset("report", Asset(href=pdc_hazard_data.snc_url, media_type="html", title="Report"))
        return item

    def _map_pdc_to_hazard_codes(self, hazard: str) -> List[str] | None:
        """Maps the hazard to the standard UNDRR-ISC 2020 Hazard Codes"""
        hazard_mapping = {
            "AVALANCHE": ["MH0050", "nat-geo-mmd-ava"],
            "DROUGHT": ["MH0035", "nat-cli-dro-dro"],
            "EARTHQUAKE": ["GH0001", "nat-geo-ear-gro"],
            "EXTREMETEMPERATURE": ["MH0040", "MH0047", "MH0041", "nat-met-ext-col", "nat-met-ext-hea", "nat-met-ext-sev"],
            "FLOOD": ["MH0012", "nat-hyd-flo-flo"],
            "HIGHWIND": ["MH0060", "nat-met-sto-sto"],
            "LANDSLIDE": ["nat-geo-mmd-lan"],
            "SEVEREWEATHER": ["nat-met-sto-sev"],
            "STORM": ["nat-met-sto-bli"],
            "TORNADO": ["nat-met-sto-tor"],
            "CYCLONE": ["nat-met-sto-tro"],
            "TSUNAMI": ["MH0029", "nat-geo-ear-tsu"],
            "VOLCANO": ["GH0020", "nat-geo-vol-vol"],
            "WILDFIRE": ["EN0013", "nat-cli-wil-for"],
            "WINTERSTORM": ["nat-met-sto-bli"],
            "STORMSURGE": ["MH0027", "nat-met-sto-sur"],
        }

        if hazard not in hazard_mapping:
            raise KeyError(f"The hazard {hazard} is not in the mapping.")

        return hazard_mapping.get(hazard)

    def make_hazard_item(self, event_item: Item, hazard_data: HazardEventValidator) -> Item:
        """Create Hazard Item"""
        if not event_item:
            return None

        hazard_item = event_item.clone()
        hazard_item.id = event_item.id.replace(STAC_EVENT_ID_PREFIX, STAC_HAZARD_ID_PREFIX)
        hazard_item.properties["roles"] = ["source", "hazard"]
        hazard_item.set_collection(self.get_hazard_collection())

        monty = MontyExtension.ext(hazard_item)
        # Hazard Detail
        monty.hazard_detail = HazardDetail(
            cluster=self.hazard_profiles.get_cluster_code(hazard_item),
            severity_value=None,
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
        if isinstance(data, list):
            return data[0] if data else None
        return data

    def make_impact_items(self, event_item: Item, exposure_detail: ExposureDetailValidator) -> List[Item]:
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
        for field_key, field_values in impact_fields.items():
            if not self.exposure_detail:
                continue

            impact_item = event_item.clone()
            impact_item.id = f"{impact_item.id.replace(STAC_EVENT_ID_PREFIX, STAC_IMPACT_ID_PREFIX)}-{self.episode_number}-{'-'.join(field_key[:-1])}"  # noqa
            impact_item.set_collection(self.get_impact_collection())
            impact_item.properties["roles"] = ["source", "impact"]

            monty = MontyExtension.ext(impact_item)
            country_codes = []
            if exposure_detail:
                country_codes.extend([admin.country for admin in exposure_detail.totalByAdmin if admin.country])
            monty.country_codes = country_codes
            # Impact Detail
            category, impact_type = field_values
            value = self.get_nested_data(exposure_detail.totalByAdmin, field_key)
            monty.impact_detail = self.get_impact_detail(category, impact_type, value)
            impact_items.append(impact_item)
        return impact_items

    def get_impact_detail(
        self, category: MontyImpactExposureCategory, impact_type: MontyImpactType, value: float
    ):
        """Create an Impact detail object"""
        return ImpactDetail(category=category, type=impact_type, value=value, unit=None, estimate_type=MontyEstimateType.PRIMARY)


# Run pre-commit checks for pdc.py
