import json
import os
from datetime import datetime
from typing import Any, List, Optional, Union

import pytz
from markdownify import markdownify as md
from pystac import Asset, Item
from shapely.geometry import Point, mapping

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

    def make_items(self) -> List[Item]:
        """Create items"""
        items = []

        if self.episode_number <= 0:
            return items

        event_item = self.make_source_event_item()
        items.append(event_item)

        hazard_item = self.make_hazard_item()
        items.append(hazard_item)

        impact_items = self.make_impact_items()
        items.extend(impact_items)

        return list(filter(lambda x: x is not None, items))

    def make_source_event_item(self) -> Optional[Item]:
        """Create an Event Item"""
        self.validate_pdc_data()

        latitude = float(self.hazard_data.get("latitude"))
        longitude = float(self.hazard_data.get("longitude"))
        # Create the geojson point
        point = Point(longitude, latitude)
        geometry = mapping(point)
        bbox = [longitude, latitude, longitude, latitude]

        description = md(self.hazard_data.get("description", "").strip()) or "NA"

        startdate = int(self.hazard_data.get("create_Date"))
        enddate = int(self.hazard_data.get("end_Date"))

        if startdate:
            startdate = pytz.utc.localize(datetime.fromtimestamp(startdate / 1_000))
        if enddate:
            enddate = pytz.utc.localize(datetime.fromtimestamp(enddate / 1_000))

        item = Item(
            id=f"{STAC_EVENT_ID_PREFIX}{self.hazard_data['uuid']}-{self.hazard_data['hazard_ID']}",
            geometry=geometry,
            bbox=bbox,
            datetime=startdate,
            properties={
                "title": self.hazard_data["hazard_Name"],
                "description": description,
                "start_datetime": startdate.isoformat(),
                "end_datetime": enddate.isoformat(),
                "category_id": self.hazard_data["category_ID"],
                "geometry_geojson": self.geojson_data,
            },
        )

        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        all_iso3 = []
        all_iso3.extend([i["country"] for i in self.exposure_detail["totalByCountry"]])
        if not all_iso3:
            return None

        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = self.episode_number
        monty.country_codes = list(set(all_iso3))

        monty.hazard_codes = self._map_pdc_to_hazard_codes(hazard=self.hazard_data["type_ID"])
        # TODO: Deal with correlation id if country_codes is a empty list
        if monty.country_codes:
            monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        if self.hazard_data["snc_url"]:
            item.add_asset("report", Asset(href=self.hazard_data["snc_url"], media_type="html", title="Report"))
        return item

    def _map_pdc_to_hazard_codes(self, hazard: str) -> List[str]:
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

    def make_hazard_item(self) -> Item:
        """Create Hazard Item"""
        event_item = self.make_source_event_item()
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
            severity_label=self.hazard_data["severity_ID"],
            estimate_type=MontyEstimateType.PRIMARY,
        )

        return hazard_item

    def get_nested_data(self, data, keys):
        """Get data from nested data structure"""
        for key in keys:
            if isinstance(data, dict) and key in data:
                data = data[key]
            else:
                return None
        return data

    def make_impact_items(self) -> List[Item]:
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
        event_item = self.make_source_event_item()
        if not event_item:
            return None

        impact_items = []
        for field_key, field_values in impact_fields.items():
            if not self.exposure_detail["totalByAdmin"]:
                continue
            for admin_item in self.exposure_detail["totalByAdmin"]:
                impact_item = event_item.clone()
                impact_item.id = f"{impact_item.id.replace(STAC_EVENT_ID_PREFIX, STAC_IMPACT_ID_PREFIX)}-{self.episode_number}-{'-'.join(field_key[:-1])}"  # noqa
                impact_item.set_collection(self.get_impact_collection())
                impact_item.properties["roles"] = ["source", "impact"]

                monty = MontyExtension.ext(impact_item)
                monty.country_codes = [admin_item["country"]]
                # Impact Detail
                category, impact_type = field_values
                value = self.get_nested_data(admin_item, field_key)
                monty.impact_detail = self.get_impact_detail(category, impact_type, value)
                impact_items.append(impact_item)
        return impact_items

    def get_impact_detail(
        self, category: MontyImpactExposureCategory, impact_type: MontyImpactType, value: Union[int, float, str]
    ):
        """Create an Impact detail object"""
        return ImpactDetail(category=category, type=impact_type, value=value, unit=None, estimate_type=MontyEstimateType.PRIMARY)

    def validate_pdc_data(self) -> dict:
        """Validate the source fields"""
        required_fields = ["latitude", "longitude"]

        if not self.hazard_data:
            raise ValueError("No PDC data found.")

        pdc_hazard_data_keys = list(self.hazard_data.keys())

        for field in required_fields:
            if field not in pdc_hazard_data_keys:
                raise ValueError(f"Missing required fields {field}.")
