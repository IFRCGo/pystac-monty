import datetime
import json
import logging
import mimetypes
import typing
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Tuple, Optional

import pytz
from markdownify import markdownify as md
from pystac import Asset, Item, Link
from shapely import simplify, to_geojson
from shapely.geometry import shape

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
from pystac_monty.validators.gdacs_events import GdacsEventDataValidator, Sendai
from pystac_monty.validators.gdacs_geometry import GdacsGeometryDataValidator

# Constants

GDACS_EVENT_STARTDATETIME_PROPERTY = "fromdate"
GDACS_EVENT_ENDDATETIME_PROPERTY = "todate"

STAC_EVENT_ID_PREFIX = "gdacs-event-"
STAC_HAZARD_ID_PREFIX = "gdacs-hazard-"
STAC_IMPACT_ID_PREFIX = "gdacs-impact-"


logger = logging.getLogger(__name__)


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

    def get_episode_data(self) -> List[Dict[str, Tuple[str, dict]]]:
        """Get all episodes"""
        return self.episodes


class GDACSTransformer(MontyDataTransformer[GDACSDataSource]):
    """
    Transforms GDACS event data into STAC Items
    see https://github.com/IFRCGo/monty-stac-extension/tree/main/model/sources/GDACS
    """

    hazard_profiles = MontyHazardProfiles()
    source_name = "gdacs"

    def get_stac_items(self) -> typing.Generator[Item, None, None]:
        """Create STAC Items"""
        self.transform_summary.mark_as_started()
        self.transform_summary.increment_rows(1)

        try:
            validated_event_data = GdacsEventDataValidator(**self.data_source.data)
            source_event_item = self.make_source_event_item(data=validated_event_data, source_url=self.data_source.source_url)
            yield source_event_item

            if self.data_source.episodes:
                for episode_data in self.data_source.episodes:
                    validated_episode_data = GdacsEventDataValidator(**episode_data[GDACSDataSourceType.EVENT][1])
                    episode_data_url = episode_data[GDACSDataSourceType.EVENT][0]
                    if GDACSDataSourceType.GEOMETRY in episode_data:
                        validated_geometry_data = GdacsGeometryDataValidator(**episode_data[GDACSDataSourceType.GEOMETRY][1])
                        geometry_data_url = episode_data[GDACSDataSourceType.GEOMETRY][0]
                    else:
                        validated_geometry_data = None
                        geometry_data_url = None
                    episode_hazard_item = self.make_hazard_event_item(
                        episode_event_data=(validated_episode_data, episode_data_url),
                        episode_geometry_data=(validated_geometry_data, geometry_data_url)
                    )
                    yield episode_hazard_item
                    yield from self.make_impact_items(episode_hazard_item, validated_episode_data)
        except Exception:
            self.transform_summary.increment_failed_rows(1)
            logger.warning("Failed to process the GDACS data", exc_info=True)
        finally:
            self.transform_summary.mark_as_complete()

    # FIXME: This is deprecated
    def make_items(self) -> List[Item]:
        return list(self.get_stac_items())

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
            print(f"Hazard {hazard} not found.")
        return hazard_mapping.get(hazard)

    def make_source_event_item(self, data: GdacsEventDataValidator, source_url: str) -> Item:
        # Build the identifier for the item
        id = STAC_EVENT_ID_PREFIX + str(data.properties.eventid)

        # Select the description
        if data.properties.htmldescription:
            # translate the description to markdown
            description = md(data.properties.htmldescription)
        else:
            description = data.properties.description

        startdate_str = data.properties.fromdate
        if isinstance(startdate_str, str):
            startdate = pytz.utc.localize(datetime.datetime.fromisoformat(startdate_str))
        else:
            startdate = pytz.utc.localize(startdate_str)
        enddate_str = data.properties.todate
        if isinstance(enddate_str, str):
            enddate = pytz.utc.localize(datetime.datetime.fromisoformat(enddate_str))
        else:
            enddate = pytz.utc.localize(enddate_str)

        item = Item(
            id=id,
            geometry=dict(data.geometry),
            bbox=data.bbox,
            datetime=startdate,
            properties={
                "title": data.properties.name,
                "description": description,
                "start_datetime": startdate.isoformat(),
                "end_datetime": enddate.isoformat(),
            },
        )

        # Monty extension fields
        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = 1
        monty.hazard_codes = self.get_hazard_codes(data.properties.eventtype)
        cc = set([data.properties.iso3])
        if hasattr(data.properties, "affectedcountries"):
            cc.update([cc.iso3 for cc in data.properties.affectedcountries])
        monty.country_codes = list(cc)
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        # assets
        # icon
        item.add_asset(
            "icon",
            Asset(
                href=str(data.properties.icon),
                media_type=mimetypes.guess_type(str(data.properties.icon))[0],
                title="Icon"
            )
        )

        # report
        if hasattr(data.properties.url, "report"):
            item.add_asset(
                "report",
                Asset(
                    href=str(data.properties.url.report),
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

    def make_hazard_event_item(self,
        episode_event_data: Tuple[GdacsEventDataValidator, str],
        episode_geometry_data: Tuple[GdacsGeometryDataValidator | None, str | None]
    ) -> Item:
        item = self.make_source_event_item(*episode_event_data)

        #episode_event = episode_source[GDACSDataSourceType.EVENT][1]
        #episode_geometry = episode_source.get(GDACSDataSourceType.GEOMETRY, None)
        episode_event = episode_event_data[0]
        episode_geometry = episode_geometry_data[0]

        item.id = (
            item.id.replace(STAC_EVENT_ID_PREFIX, STAC_HAZARD_ID_PREFIX)
            + "-"
            + str(episode_event.properties.episodeid)
        )
        item.set_collection(self.get_hazard_collection())
        item.properties["roles"] = ["source", "hazard"]
        item.properties["source"] = episode_event.properties.source

        hazard_geometry = None

        if episode_geometry:
            # geometry data is a FeatureCollection so we must find the proper feature
            # that has the properties.class == "Poly_Affected"
            for feature in episode_geometry.features:
                if hasattr(feature.properties, "Class") and feature.properties.Class == "Poly_Affected":
                    hazard_geometry = shape(dict(feature.geometry))
                    break
                if hasattr(feature.properties, "Class") and feature.properties.Class == "Poly_area":
                    hazard_geometry = shape(dict(feature.geometry))
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

    def get_hazard_detail(self, item: Item, data: GdacsEventDataValidator) -> HazardDetail:
        # Use episode-specific severity data
        severity_value = data.properties.episodealertscore
        severity_label = data.properties.episodealertlevel

        return HazardDetail(
            cluster=self.hazard_profiles.get_cluster_code(item),
            severity_value=severity_value,
            severity_unit="GDACS Severity Score",
            severity_label=severity_label,
            estimate_type=MontyEstimateType.PRIMARY,
        )

    def make_impact_items(self, hazard_item: Item, data: GdacsEventDataValidator) -> list[Item]:
        impact_items = []

        # Search for Sendai fields
        if hasattr(data.properties, "sendai"):
            sendai = data.properties.sendai
            if sendai:
                for entry in sendai:
                    impact_item = self.make_impact_item_from_sendai_entry(entry, hazard_item, data=data)
                    impact_items.append(impact_item)

        return impact_items

    def make_impact_item_from_sendai_entry(self, entry: Sendai, hazard_item: Item, data: Optional[GdacsEventDataValidator] = None) -> Item:
        item = hazard_item.clone()
        item.id = (
            item.id.replace(STAC_EVENT_ID_PREFIX, STAC_IMPACT_ID_PREFIX)
            + "-"
            + entry.sendaitype
            + "-"
            + entry.sendainame
            + "-"
            + entry.country
            + "-"
            + entry.region
        )
        item.common_metadata.description = entry.description #entry["description"]
        # TODO geolocate the with country and region metadata
        # item.geometry = self.geolocate(entry["country"], entry["region"])
        item.set_collection(self.get_impact_collection())
        item.properties["roles"] = ["source", "impact"]
        if isinstance(entry.dateinsert, str):
            item.common_metadata.created = pytz.utc.localize(datetime.datetime.fromisoformat(entry.dateinsert))
        else:
            item.common_metadata.created = pytz.utc.localize(entry.dateinsert)
        if isinstance(entry.onset_date, str):
            item.common_metadata.start_datetime = pytz.utc.localize(datetime.datetime.fromisoformat(entry.onset_date))
        else:
            item.common_metadata.start_datetime = pytz.utc.localize(entry.onset_date)
        if isinstance(entry.expires_date, str):
            item.common_metadata.end_datetime = pytz.utc.localize(datetime.datetime.fromisoformat(entry.expires_date))
        else:
            item.common_metadata.end_datetime = pytz.utc.localize(entry.expires_date)

        # Monty extension fields
        monty = MontyExtension.ext(item)
        # impact_detail
        monty.impact_detail = self.get_impact_detail(entry)
        country_code = next(
            (cc.iso3 for cc in data.properties.affectedcountries if cc.countryname == entry.country),
            None,
        )
        monty.country_codes = [country_code if country_code else data.properties.iso3]

        return item

    def get_impact_detail(self, entry: Sendai) -> ImpactDetail:
        return ImpactDetail(
            category=self.get_impact_category_from_sendai_indicators(entry.sendaitype, entry.sendainame),
            type=self.get_impact_type_from_sendai_indicators(entry.sendaitype, entry.sendainame),
            value=int(entry.sendaivalue),
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
        if sendainame == "rescued" or sendainame == "displaced" or sendainame == "affected" or sendainame == "injured":
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
        elif sendainame == "transport":
            return MontyImpactType.TOTAL_AFFECTED
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
