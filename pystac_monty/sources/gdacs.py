import datetime
import json
import mimetypes
from dataclasses import dataclass
from enum import Enum
from typing import Any

import pytz
import requests
from markdownify import markdownify as md
from pystac import Asset, Collection, Item, Link
from shapely import simplify, to_geojson
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry

from pystac_monty.extension import (
    HazardDetail,
    ImpactDetail,
    MontyEstimateType,
    MontyExtension,
    MontyImpactExposureCategory,
    MontyImpactType,
)
from pystac_monty.hazard_profiles import HazardProfiles
from pystac_monty.sources.common import MontyDataSource

# Constants

GDACS_EVENT_STARTDATETIME_PROPERTY = "fromdate"
GDACS_EVENT_ENDDATETIME_PROPERTY = "todate"

STAC_EVENT_ID_PREFIX = "gdacs-event-"
STAC_HAZARD_ID_PREFIX = "gdacs-hazard-"
STAC_IMPACT_ID_PREFIX = "gdacs-impact-"


class GDACSDataSourceType(Enum):
    EVENT = "geteventdata"
    GEOMETRY = "getgeometry"


@dataclass
class GDACSDataSource(MontyDataSource):
    type: GDACSDataSourceType

    def __init__(self, source_url: str, data: Any, type: GDACSDataSourceType):
        super().__init__(source_url, data)
        self.type = type
        # all gdacs data are json
        self.data = json.loads(data)

    def get_type(self) -> GDACSDataSourceType:
        return self.type


class GDACSTransformer:
    """
    Transforms GDACS event data into STAC Items
    see https://github.com/IFRCGo/monty-stac-extension/tree/main/model/sources/GDACS
    """

    gdacs_events_collection_id = "gdacs-events"
    gdacs_events_collection_url = (
        "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/examples/gdacs-events/gdacs-events.json"
    )

    gdacs_hazards_collection_id = "gdacs-hazards"
    gdacs_hazards_collection_url = (
        "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/examples/gdacs-hazards/gdacs-hazards.json"
    )

    gdacs_impacts_collection_id = "gdacs-impacts"
    gdacs_impacts_collection_url = (
        "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/examples/gdacs-impacts/gdacs-impacts.json"
    )

    data: list[GDACSDataSource] = []
    hazard_profiles = HazardProfiles()

    def __init__(self, data: list[GDACSDataSource]) -> None:
        self.data = data

    def make_items(self) -> list[Item]:
        items = []

        """ 1. Create the source event item """
        source_event_item = self.make_source_event_item()
        items.append(source_event_item)

        # """ 2. Create the hazard item """
        hazard_event_item = self.make_hazard_event_item()
        items.append(hazard_event_item)

        """ 3. Create the impact items """
        impact_items = self.make_impact_items()
        items.extend(impact_items)

        return items

    def get_event_collection(self) -> Collection:
        response = requests.get(self.gdacs_events_collection_url)
        collection_dict = json.loads(response.text)
        return Collection.from_dict(collection_dict)

    def get_hazard_collection(self) -> Collection:
        response = requests.get(self.gdacs_hazards_collection_url)
        collection_dict = json.loads(response.text)
        return Collection.from_dict(collection_dict)

    def get_impact_collection(self) -> Collection:
        response = requests.get(self.gdacs_impacts_collection_url)
        collection_dict = json.loads(response.text)
        return Collection.from_dict(collection_dict)

    def check_and_get_event_data(self) -> GDACSDataSource:
        # first check that the event data is present in the data
        gdacs_event = next((x for x in self.data if x.get_type() == GDACSDataSourceType.EVENT), None)

        if not gdacs_event:
            raise ValueError("no GDACS event data found")

        if "geometry" not in gdacs_event.data:
            raise ValueError("event_data must contain a geometry")
        # check that the geometry is only a point
        if gdacs_event.data["geometry"]["type"] != "Point":
            raise ValueError("Geometry must be a point")
        # check the properties
        if "properties" not in gdacs_event.data:
            raise ValueError("event_data must contain properties")
        # check the datetime
        if GDACS_EVENT_STARTDATETIME_PROPERTY not in gdacs_event.data["properties"]:
            raise ValueError("event_data must contain a 'fromdate' property")

        return gdacs_event

    def check_and_get_geometry_data(self) -> GDACSDataSource:
        # first check that the geometry data is present in the data
        gdacs_geometry = next((x for x in self.data if x.get_type() == GDACSDataSourceType.GEOMETRY), None)

        if not gdacs_geometry:
            raise ValueError("no GDACS geometry data found")

        if "features" not in gdacs_geometry.data:
            raise ValueError("geometry_data must contain features")

        return gdacs_geometry

    def make_source_event_item(self) -> Item:
        # check event_data
        gdacs_event = self.check_and_get_event_data()

        # Build the identifier for the item
        id = STAC_EVENT_ID_PREFIX + gdacs_event.data["properties"]["eventid"].__str__()
        episode_number = 0
        if "episodeid" in gdacs_event.data["properties"]:
            episode_number = gdacs_event.data["properties"]["episodeid"]

        id += "-" + episode_number.__str__()

        # Select the description
        if "htmldescription" in gdacs_event.data["properties"]:
            # translate the description to markdown
            description = md(gdacs_event.data["properties"]["htmldescription"])
        else:
            description = gdacs_event.data["properties"]["description"]

        startdate_str = gdacs_event.data["properties"][GDACS_EVENT_STARTDATETIME_PROPERTY]
        startdate = pytz.utc.localize(datetime.datetime.fromisoformat(startdate_str))
        enddate_str = gdacs_event.data["properties"][GDACS_EVENT_ENDDATETIME_PROPERTY]
        enddate = pytz.utc.localize(datetime.datetime.fromisoformat(enddate_str))

        item = Item(
            id=id,
            geometry=gdacs_event.data["geometry"],
            bbox=gdacs_event.data["bbox"],
            datetime=startdate,
            properties={
                "title": gdacs_event.data["properties"]["name"],
                "description": description,
                "start_datetime": startdate.isoformat(),
                "end_datetime": enddate.isoformat(),
            },
        )

        # Monty extension fields

        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = episode_number
        monty.hazard_codes = [gdacs_event.data["properties"]["eventtype"]]
        cc = set([gdacs_event.data["properties"]["iso3"]])
        if "affectedcountries" in gdacs_event.data["properties"]:
            cc.update([cc["iso3"] for cc in gdacs_event.data["properties"]["affectedcountries"]])
        monty.country_codes = list(cc.union())
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        # assets

        # icon
        item.add_asset(
            "icon",
            Asset(
                href=gdacs_event.data["properties"]["icon"],
                media_type=mimetypes.guess_type(gdacs_event.data["properties"]["icon"])[0],
                title="Icon",
            ),
        )

        # report
        if "report" in gdacs_event.data["properties"]["url"]:
            item.add_asset(
                "report",
                Asset(
                    href=gdacs_event.data["properties"]["url"]["report"],
                    media_type=mimetypes.types_map[".html"],
                    title="Report",
                ),
            )

        # collection and roles

        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        # links

        item.add_link(Link("via", gdacs_event.source_url, "application/json", "GDACS Event Data"))

        return item

    def make_hazard_event_item(self) -> Item:
        item = self.make_source_event_item()
        gdacs_geometry = self.check_and_get_geometry_data()

        if not gdacs_geometry:
            raise ValueError("no GDACS geometry data found")

        item.id = item.id.replace(STAC_EVENT_ID_PREFIX, STAC_HAZARD_ID_PREFIX)
        item.set_collection(self.get_hazard_collection())
        item.properties["roles"] = ["source", "hazard"]

        hazard_geometry = None

        # geometry data is a FeatureCollection so we must find the proper feature
        # that has the properties.class == "Poly_Affected"
        for feature in gdacs_geometry.data["features"]:
            if feature["properties"].get("Class", None) == "Poly_Affected":
                hazard_geometry: BaseGeometry = shape(feature["geometry"])
                break

        if hazard_geometry:
            # We often need to simplify the geometry using shapely
            simplified_geometry = simplify(hazard_geometry, tolerance=0.1, preserve_topology=True)
            item.geometry = json.loads(to_geojson(simplified_geometry))
            item.bbox = list(simplified_geometry.bounds)

        # Monty extension fields
        monty = MontyExtension.ext(item)
        # hazard_detail
        monty.hazard_detail = self.get_hazard_detail(item)

        return item

    def get_hazard_detail(self, item: Item) -> HazardDetail:
        # get the hazard detail from the event data
        gdacs_event = self.check_and_get_event_data()
        monty = MontyExtension.ext(item)
        return HazardDetail(
            cluster=self.hazard_profiles.get_cluster_code(monty.hazard_codes),
            severity_value=gdacs_event.data["properties"].get("episodealertscore", None),
            severity_unit="GDACS Flood Severity Score",
            severity_label=gdacs_event.data["properties"].get("episodealertlevel", None),
            estimate_type=MontyEstimateType.PRIMARY,
        )

    def make_impact_items(self) -> list[Item]:
        impact_items = []
        # check event_data
        gdacs_event = self.check_and_get_event_data()
        # Search for Sendai fields
        if "sendai" in gdacs_event.data["properties"]:
            sendai = gdacs_event.data["properties"]["sendai"]
            for entry in sendai:
                impact_item = self.make_impact_item_from_sendai_entry(entry, gdacs_event)
                impact_items.append(impact_item)

        return impact_items

    def make_impact_item_from_sendai_entry(self, entry: dict, gdacs_event: GDACSDataSource) -> Item:
        item = self.make_source_event_item()
        item.id = (
            item.id.replace(STAC_EVENT_ID_PREFIX, STAC_IMPACT_ID_PREFIX)
            + "-"
            + entry["sendaitype"]
            + "-"
            + entry["sendainame"]
            + "-"
            + entry["country"]
            + "-"
            + entry["region"]
        )
        item.common_metadata.description = entry["description"]
        # TODO geolocate the with country and region metadata
        # item.geometry = self.geolocate(entry["country"], entry["region"])
        item.set_collection(self.get_impact_collection())
        item.properties["roles"] = ["source", "impact"]
        item.common_metadata.created = pytz.utc.localize(datetime.datetime.fromisoformat(entry["dateinsert"].split(".")[0]))
        item.common_metadata.start_datetime = pytz.utc.localize(datetime.datetime.fromisoformat(entry["onset_date"]))
        item.common_metadata.end_datetime = pytz.utc.localize(datetime.datetime.fromisoformat(entry["expires_date"]))

        # Monty extension fields
        monty = MontyExtension.ext(item)
        # impact_detail
        monty.impact_detail = self.get_impact_detail(entry)
        country_code = next(
            (cc["iso3"] for cc in gdacs_event.data["properties"]["affectedcountries"] if cc["countryname"] == entry["country"]),
            None,
        )
        monty.country_codes = [(country_code if country_code else gdacs_event.data["properties"]["iso3"])]

        return item

    def get_impact_detail(self, entry: dict) -> ImpactDetail:
        return ImpactDetail(
            category=self.get_impact_category_from_sendai_indicators(entry["sendaitype"], entry["sendainame"]),
            type=self.get_impact_type_from_sendai_indicators(entry["sendaitype"], entry["sendainame"]),
            value=int(entry["sendaivalue"]),
            unit="sendai",
            estimate_type=MontyEstimateType.PRIMARY,
        )

    @staticmethod
    def get_impact_category_from_sendai_indicators(sendaitype: str, sendainame: str) -> MontyImpactExposureCategory:
        if sendaitype == "A":
            return GDACSTransformer.get_impact_category_from_sendai_a(sendainame)
        elif sendaitype == "B":
            return GDACSTransformer.get_impact_category_from_sendai_b(sendainame)
        elif sendaitype == "C":
            return GDACSTransformer.get_impact_category_from_sendai_c(sendainame)
        elif sendaitype == "D":
            return GDACSTransformer.get_impact_category_from_sendai_d(sendainame)
        elif sendaitype == "E":
            return GDACSTransformer.get_impact_category_from_sendai_e(sendainame)
        elif sendaitype == "F":
            return GDACSTransformer.get_impact_category_from_sendai_f(sendainame)
        elif sendainame == "G":
            return GDACSTransformer.get_impact_category_from_sendai_g(sendainame)
        else:
            raise ValueError(f"Unknown sendai type {sendaitype} and name {sendainame}")

    @staticmethod
    def get_impact_category_from_sendai_a(
        sendainame: str,
    ) -> MontyImpactExposureCategory:
        return MontyImpactExposureCategory.ALL_PEOPLE

    @staticmethod
    def get_impact_category_from_sendai_b(
        sendainame: str,
    ) -> MontyImpactExposureCategory:
        if sendainame == "rescued" or sendainame == "displaced" or sendainame == "affected":
            return MontyImpactExposureCategory.ALL_PEOPLE
        else:
            raise ValueError(f"Unknown sendai name {sendainame} for indicators B")

    @staticmethod
    def get_impact_category_from_sendai_c(
        sendainame: str,
    ) -> MontyImpactExposureCategory:
        if sendainame == "houses damaged" or sendainame == "houses":
            return MontyImpactExposureCategory.BUILDINGS
        else:
            raise ValueError(f"Unknown sendai name {sendainame} for indicators C")

    @staticmethod
    def get_impact_category_from_sendai_d(
        sendainame: str,
    ) -> MontyImpactExposureCategory:
        if sendainame == "bridges destroyed":
            return MontyImpactExposureCategory.BUILDINGS
        else:
            raise ValueError(f"Unknown sendai name {sendainame} for indicators D")

    @staticmethod
    def get_impact_type_from_sendai_indicators(sendaitype: str, sendainame: str) -> MontyImpactType:
        if sendaitype == "A":
            return GDACSTransformer.get_impact_type_from_sendai_a(sendainame)
        elif sendaitype == "B":
            return GDACSTransformer.get_impact_type_from_sendai_b(sendainame)
        elif sendaitype == "C":
            return GDACSTransformer.get_impact_type_from_sendai_c(sendainame)
        elif sendaitype == "D":
            return GDACSTransformer.get_impact_type_from_sendai_d(sendainame)
        elif sendaitype == "E":
            return GDACSTransformer.get_impact_type_from_sendai_e(sendainame)
        elif sendaitype == "F":
            return GDACSTransformer.get_impact_type_from_sendai_f(sendainame)
        elif sendainame == "G":
            return GDACSTransformer.get_impact_type_from_sendai_g(sendainame)
        else:
            raise ValueError(f"Unknown sendai type {sendaitype} and name {sendainame}")

    @staticmethod
    def get_impact_type_from_sendai_a(sendainame: str) -> MontyImpactType:
        if sendainame == "death":
            return MontyImpactType.DEATHS
        elif sendainame == "missing":
            return MontyImpactType.MISSING
        else:
            raise ValueError(f"Unknown sendai name {sendainame} for indicators A")

    @staticmethod
    def get_impact_type_from_sendai_b(sendainame: str) -> MontyImpactType:
        if sendainame == "rescued":
            return MontyImpactType.ASSISTED
        elif sendainame == "displaced":
            return MontyImpactType.RELOCATED
        elif sendainame == "affected":
            return MontyImpactType.TOTAL_AFFECTED
        else:
            raise ValueError(f"Unknown sendai name {sendainame} for indicators B")

    @staticmethod
    def get_impact_type_from_sendai_c(sendainame: str) -> MontyImpactType:
        if sendainame == "houses damaged" or sendainame == "houses":
            return MontyImpactType.DAMAGED
        else:
            raise ValueError(f"Unknown sendai name {sendainame} for indicators C")

    @staticmethod
    def get_impact_type_from_sendai_d(sendainame: str) -> MontyImpactType:
        if sendainame == "bridges destroyed":
            return MontyImpactType.DESTROYED
        else:
            raise ValueError(f"Unknown sendai name {sendainame} for indicators D")
