import datetime
import json
import mimetypes
import typing
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

import pytz
from markdownify import markdownify as md
from pystac import Asset, Item, Link
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
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import MontyDataSource, MontyDataTransformer
from pystac_monty.validators.gdacs_events import GdacsDataValidatorEvents
from pystac_monty.validators.gdacs_geometry import GdacsDataValidatorGeometry

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

    def __init__(
        self,
        source_url: str,
        data: Any,
        episodes: Optional[list[Dict[GDACSDataSourceType, tuple[str, Any]]]] = None,
    ):
        super().__init__(source_url, data)
        # all gdacs data are json
        self.data = json.loads(data) if isinstance(data, str) else data
        self.episodes = episodes

    def get_episode_data(self) -> Optional[Dict[str, Any]]:
        return self.episodes


class GDACSTransformer(MontyDataTransformer[GDACSDataSource]):
    """
    Transforms GDACS event data into STAC Items
    see https://github.com/IFRCGo/monty-stac-extension/tree/main/model/sources/GDACS
    """

    hazard_profiles = MontyHazardProfiles()
    source_name = "gdacs"

    def make_items(self) -> list[Item]:
        items = []

        # Process the main event data
        source_event_item = self.make_source_event_item(self.data_source.data, self.data_source.source_url)
        items.append(source_event_item)

        # Process each episode
        if self.data_source.episodes:
            for episode_data in self.data_source.episodes:
                episode_hazard_item = self.make_hazard_event_item(episode_data)

                items.append(episode_hazard_item)

                # Create impact items for this episode
                impact_items = self.make_impact_items(episode_hazard_item, episode_data[GDACSDataSourceType.EVENT][1])
                items.extend(impact_items)

        return items

    def get_hazard_codes(self, hazard: str) -> List[str]:
        hazard_mapping = {
            "EQ": ["GH0001", "GH0002", "GH0003", "GH0004", "GH0005"],
            "TC": ["MH0030", "MH0031", "MH0032"],
            "FL": ["FL"],  # General flood
            "DR": ["MH0035"],
            "WF": ["EN0013"],
            "VO": ["GH009", "GH0013", "GH0014", "GH0015", "GH0016"],
            "TS": ["MH0029", "GH0006"],
        }
        if hazard not in hazard_mapping:
            raise KeyError(f"Hazard {hazard} not found.")
        return hazard_mapping.get(hazard)

    def make_source_event_item(self, data: Any, source_url: str) -> Item:
        # Build the identifier for the item
        id = STAC_EVENT_ID_PREFIX + data["properties"]["eventid"].__str__()

        # Select the description
        if "htmldescription" in data["properties"]:
            # translate the description to markdown
            description = md(data["properties"]["htmldescription"])
        else:
            description = data["properties"]["description"]

        startdate_str = data["properties"][GDACS_EVENT_STARTDATETIME_PROPERTY]
        startdate = pytz.utc.localize(datetime.datetime.fromisoformat(startdate_str))
        enddate_str = data["properties"][GDACS_EVENT_ENDDATETIME_PROPERTY]
        enddate = pytz.utc.localize(datetime.datetime.fromisoformat(enddate_str))

        item = Item(
            id=id,
            geometry=data["geometry"],
            bbox=data["bbox"],
            datetime=startdate,
            properties={
                "title": data["properties"]["name"],
                "description": description,
                "start_datetime": startdate.isoformat(),
                "end_datetime": enddate.isoformat(),
            },
        )

        # Monty extension fields
        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = 1
        monty.hazard_codes = self.get_hazard_codes(data["properties"]["eventtype"])
        cc = set([data["properties"]["iso3"]])
        if "affectedcountries" in data["properties"]:
            cc.update([cc["iso3"] for cc in data["properties"]["affectedcountries"]])
        monty.country_codes = list(cc)
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        # assets
        # icon
        item.add_asset(
            "icon",
            Asset(
                href=data["properties"]["icon"],
                media_type=mimetypes.guess_type(data["properties"]["icon"])[0],
                title="Icon",
            ),
        )

        # report
        if "report" in data["properties"]["url"]:
            item.add_asset(
                "report",
                Asset(
                    href=data["properties"]["url"]["report"],
                    media_type=mimetypes.types_map[".html"],
                    title="Report",
                ),
            )

        # collection and roles
        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        # links
        item.add_link(Link("via", source_url, "application/json", "GDACS Event Data"))

        return item

    def make_hazard_event_item(self, episode_source: Dict[GDACSDataSourceType, tuple[str, Any]]) -> Item:
        item = self.make_source_event_item(
            episode_source[GDACSDataSourceType.EVENT][1], episode_source[GDACSDataSourceType.EVENT][0]
        )

        episode_event = episode_source[GDACSDataSourceType.EVENT][1]
        episode_geometry = episode_source.get(GDACSDataSourceType.GEOMETRY, None)

        item.id = (
            item.id.replace(STAC_EVENT_ID_PREFIX, STAC_HAZARD_ID_PREFIX)
            + "-"
            + episode_event["properties"]["episodeid"].__str__()
        )
        item.set_collection(self.get_hazard_collection())
        item.properties["roles"] = ["source", "hazard"]
        item.properties["source"] = episode_event["properties"]["source"]

        hazard_geometry = None

        if episode_geometry is not None:
            episode_geometry = episode_geometry[1]
            # geometry data is a FeatureCollection so we must find the proper feature
            # that has the properties.class == "Poly_Affected"
            for feature in episode_geometry["features"]:
                if feature["properties"].get("Class", None) == "Poly_Affected":
                    hazard_geometry: BaseGeometry = shape(feature["geometry"])
                    break
                if feature["properties"].get("Class", None) == "Poly_area":
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
        monty.hazard_detail = self.get_hazard_detail(item, episode_event)

        return item

    def get_hazard_detail(self, item: Item, data: Any) -> HazardDetail:
        # Use episode-specific severity data
        severity_value = data["properties"].get("episodealertscore", None)
        severity_label = data["properties"].get("episodealertlevel", None)

        return HazardDetail(
            cluster=self.hazard_profiles.get_cluster_code(item),
            severity_value=severity_value,
            severity_unit="GDACS Severity Score",
            severity_label=severity_label,
            estimate_type=MontyEstimateType.PRIMARY,
        )

    def make_impact_items(self, hazard_item: Item, data: Any) -> list[Item]:
        impact_items = []

        # Search for Sendai fields
        if "sendai" in data["properties"]:
            sendai = data["properties"]["sendai"]
            for entry in sendai:
                impact_item = self.make_impact_item_from_sendai_entry(entry, hazard_item, data=data)
                impact_items.append(impact_item)

        return impact_items

    def make_impact_item_from_sendai_entry(self, entry: dict, hazard_item: Item, data: Any = None) -> Item:
        item = hazard_item.clone()
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
            (cc["iso3"] for cc in data["properties"]["affectedcountries"] if cc["countryname"] == entry["country"]),
            None,
        )
        monty.country_codes = [(country_code if country_code else data["properties"]["iso3"])]

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
        elif sendaitype == "G":
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
        if sendainame == "houses damaged" or sendainame == "houses" or sendainame == "houses destroyed":
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
    def get_impact_category_from_sendai_e(
        sendainame: str,
    ) -> MontyImpactExposureCategory:
        # Implement this method if needed
        raise ValueError(f"Method not implemented for sendai type E with name {sendainame}")

    @staticmethod
    def get_impact_category_from_sendai_f(
        sendainame: str,
    ) -> MontyImpactExposureCategory:
        # Implement this method if needed
        raise ValueError(f"Method not implemented for sendai type F with name {sendainame}")

    @staticmethod
    def get_impact_category_from_sendai_g(
        sendainame: str,
    ) -> MontyImpactExposureCategory:
        # Implement this method if needed
        raise ValueError(f"Method not implemented for sendai type G with name {sendainame}")

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
        elif sendaitype == "G":
            return GDACSTransformer.get_impact_type_from_sendai_g(sendainame)
        else:
            raise ValueError(f"Unknown sendai type {sendaitype} and name {sendainame}")

    @staticmethod
    def get_impact_type_from_sendai_a(sendainame: str) -> MontyImpactType:
        if sendainame == "death":
            return MontyImpactType.DEATH
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

    @staticmethod
    def get_impact_type_from_sendai_e(sendainame: str) -> MontyImpactType:
        # Implement this method if needed
        raise ValueError(f"Method not implemented for sendai type E with name {sendainame}")

    @staticmethod
    def get_impact_type_from_sendai_f(sendainame: str) -> MontyImpactType:
        # Implement this method if needed
        raise ValueError(f"Method not implemented for sendai type F with name {sendainame}")

    @staticmethod
    def get_impact_type_from_sendai_g(sendainame: str) -> MontyImpactType:
        # Implement this method if needed
        raise ValueError(f"Method not implemented for sendai type G with name {sendainame}")

    def get_stac_items(self) -> typing.Generator[Item, None, None]:
        # This is an abstract method from the parent class that needs to be implemented
        # For now, we'll just yield the items from make_items
        for item in self.make_items():
            yield item
