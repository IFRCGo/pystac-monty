import json
from datetime import datetime
from typing import List, Optional, Union

import pytz
import requests
from markdownify import markdownify as md
from pystac import Asset, Collection, Item
from shapely.geometry import Point, mapping

from pystac_monty.extension import (
    HazardDetail,
    ImpactDetail,
    MontyEstimateType,
    MontyExtension,
    MontyImpactExposureCategory,
    MontyImpactType,
)
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import MontyDataSource

# Constants

STAC_EVENT_ID_PREFIX = "pdc-event-"
STAC_HAZARD_ID_PREFIX = "pdc-hazard-"
STAC_IMPACT_ID_PREFIX = "pdc-impact-"


class PDCDataSource(MontyDataSource):
    """PDC Data from the source"""

    def __init__(self, source_url: str, event_src_data: Union[str, List[dict]]):
        super().__init__(source_url, event_src_data)
        if isinstance(event_src_data, list):
            self.event_src_data = json.loads(event_src_data)


class PDCTransformer:
    """Transform the source data into the STAC items"""

    pdc_events_collection_id = "pdc-events"
    pdc_events_collection_url = ""
    pdc_hazards_collection_id = "pdc-hazards"
    pdc_hazards_collection_url = ""
    pdc_impacts_collection_id = "pdc-impacts"
    pdc_impacts_collection_url = ""

    hazard_profiles = MontyHazardProfiles()

    def __init__(self, data: PDCDataSource):
        self.data = data

    def make_items(self) -> List[Item]:
        """Create items"""
        items = []

        event_item = self.make_source_event_item()
        items.append(event_item)

        hazard_item = self.make_hazard_item()
        items.append(hazard_item)

        impact_items = self.make_impact_items()
        items.extend(impact_items)

        return items

    def get_event_collection(self, timeout: int = 30):
        """Get Event Collection"""
        response = requests.get(self.pdc_events_collection_url, timeout=timeout)
        collection_dict = response.json()
        return Collection.from_dict(collection_dict)

    def get_hazard_collection(self):
        """Get Hazard Collection"""
        return

    def get_impact_collection(self):
        """Get Impact Collection"""
        return

    def make_source_event_item(self) -> Optional[Item]:
        """Create an Event Item"""
        event_data = self.check_and_get_pdc_data()
        if not event_data:
            return None

        latitude = float(event_data.get("latitude"))
        longitude = float(event_data.get("longitude"))
        # Create the geojson point
        point = Point(longitude, latitude)
        geometry = mapping(point)
        bbox = [longitude, latitude, longitude, latitude]

        description = md(event_data.get("description", ""))

        startdate = event_data.get("create_Date")
        enddate = event_data.get("end_Date")

        if startdate:
            startdate = pytz.utc.localize(datetime.fromtimestamp(startdate / 1_000))
        if enddate:
            enddate = pytz.utc.localize(datetime.fromtimestamp(enddate / 1_000))

        episode_number = 1

        item = Item(
            id=f'{STAC_EVENT_ID_PREFIX}{event_data["uuid"]}-{event_data["hazard_ID"]}',
            geometry=geometry,
            bbox=bbox,
            datetime=startdate,
            properties={
                "title": event_data["hazard_Name"],
                "description": description,
                "start_datetime": startdate.isoformat(),
                "end_datetime": enddate.isoformat(),
                "category_id": event_data["category_ID"],
            },
        )

        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        all_iso3 = []
        for _, exposure_details in event_data["exposure"].items():
            all_iso3.extend([i["country"] for i in exposure_details["totalByCountry"]])

        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = episode_number
        monty.country_codes = list(set(all_iso3))
        monty.hazard_codes = self._map_pdc_to_hazard_codes(hazard=event_data["type_ID"])
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        if event_data["snc_url"]:
            item.add_asset("report", Asset(href=event_data["snc_url"], media_type="html", title="Report"))

        return item

    def _map_pdc_to_hazard_codes(self, hazard: str) -> List[str]:
        """Maps the hazard to the standard UNDRR-ISC 2020 Hazard Codes"""
        hazard_mapping = {
            "AVALANCHE": ["MH0050", "nat-geo-mmd-ava"],
            "DROUGHT": ["MH0035", "nat-cli-dro-dro"],
            "EARTHQUAKE": ["GH0001", "nat-geo-ear-gro"],
            "EXTREMETEMPERATURE": ["MH0040", "MH0047", "MH0041", "nat-met-ext-col", "nat-met-ext-hea", "nat-met-ext-sev"],
            "FLOOD": ["MH0012", "nat-hyd-flo-flo"],
            "HIGHSURF": [],
            "HIGHWIND": ["MH0060", "nat-met-sto-sto"],
            "LANDSLIDE": [],
            "SEVEREWEATHER": [],
            "STORM": [],
            "TORNADO": [],
            "CYCLONE": [],
            "TSUNAMI": [],
            "VOLCANO": [],
            "WILDFIRE": [],
            "WINTERSTORM": [],
        }

        if hazard not in hazard_mapping:
            raise Exception(f"The hazard {hazard} is not in the mapping.")

        return hazard_mapping.get(hazard)

    def make_hazard_item(self) -> Item:
        """Create Hazard Item"""
        event_data = self.check_and_get_pdc_data()
        event_item = self.make_source_event_item()

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
            severity_label=event_data["severity_ID"],
            estimate_type=MontyEstimateType.PRIMARY,
        )

        return hazard_item

    def get_nested_data(self, data, keys):
        """Get data from nested data structure"""
        for key in keys:
            if isinstance(data, dict) and key in data:
                data = data[key]  # Move deeper into the nested structure
            else:
                return None  # Return None if any key is missing
        return data  # Return the final value if founds

    def make_impact_items(self) -> List[Item]:
        """Create Impact Items"""
        event_data = self.check_and_get_pdc_data()
        event_item = self.make_source_event_item()

        impact_items = []
        for idx, (_, exposure_details) in enumerate(event_data["exposure"].items(), start=1):
            impact_item = self.make_imapct_items_per_field(
                event_item=event_item, exposure_details=exposure_details, episode_number=idx
            )
            impact_items.extend(impact_item)
        return impact_items

    def make_imapct_items_per_field(self, event_item: Item, exposure_details: dict, episode_number: int) -> Item:
        """Create Impact Item"""
        impact_fields = {
            ("population", "vulnerable0_14"): (MontyImpactExposureCategory.CHILDREN_UNDER_14, MontyImpactType.TOTAL_AFFECTED)
        }
        impact_values = [("population", "vulnerable0_14", "value")]
        impact_items = []
        for field_key, value in zip(impact_fields.keys(), impact_values):
            if not exposure_details["totalByAdmin"]:
                continue
            for admin_item in exposure_details["totalByAdmin"]:
                impact_item = event_item.clone()
                impact_item.id = (
                    f"{impact_item.id.replace(STAC_EVENT_ID_PREFIX, STAC_IMPACT_ID_PREFIX)} - {episode_number} - {field_key[-1]}"  # noqa
                )
                impact_item.set_collection(self.get_impact_collection())
                impact_item.properties["roles"] = ["source", "impact"]

                monty = MontyExtension.ext(impact_item)
                monty.episode_number = episode_number
                monty.country_codes = [admin_item["country"]]
                # Impact Detail
                category, impact_type = self.get_nested_data(admin_item, field_key)
                value = self.get_nested_data(admin_item, value)
                monty.impact_detail = self.get_impact_detail(category, impact_type, value)
                impact_items.append(impact_item)
        return impact_items

    def get_impact_detail(
        self, category: MontyImpactExposureCategory, impact_type: MontyImpactType, value: Union[int, float, str]
    ):
        """Create an Impact detail object"""
        return ImpactDetail(category=category, type=impact_type, value=value, unit=None, estimate_type=MontyEstimateType.PRIMARY)

    def check_and_get_pdc_data(self) -> dict:
        """Validate the source fields"""
        pdc_data = self.data.get_data()
        required_fields = ["latitude", "longitude", "event_id"]

        if not pdc_data:
            print(f"No PDC data found in {self.data.get_source_url()}")
            return []

        for item in pdc_data:
            missing_fields = [field for field in required_fields if field not in item]
            if missing_fields:
                raise ValueError(f"Missing required fields {missing_fields}.")
        return pdc_data
