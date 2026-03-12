import datetime
import json
import logging
import os
import typing
from dataclasses import dataclass
from typing import Dict, List, Tuple, Union

import pytz
from markdownify import markdownify as md
from pystac import Item, Link
from shapely.geometry import MultiPolygon, Polygon, mapping

from pystac_monty.extension import HazardDetail, MontyEstimateType, MontyExtension
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import DataType, File, GenericDataSource, Memory, MontyDataSourceV3, MontyDataTransformer
from pystac_monty.validators.alerthub import AlertHubDataValidator, AlertInfo, AlertInfoSummary, AlertItem, Area

logger = logging.getLogger(__name__)

STAC_EVENT_ID_PREFIX = "alerthub-event-"
STAC_HAZARD_ID_PREFIX = "alerthub-hazard-"
STAC_IMPACT_ID_PREFIX = "alerthub-impact-"


@dataclass
class AlertHubDataSource(MontyDataSourceV3):
    """Data source for Alert Hub"""

    event_data_file_path: str
    source_url: str
    event_data: Union[str, dict]
    data_source: Union[File, Memory]

    def __init__(self, data: GenericDataSource):
        super().__init__(data)

        def handle_file_data():
            if os.path.isfile(self.input_data.path):
                self.event_data_file_path = self.input_data.path
                return
            raise ValueError("File path does not exist")

        def handle_memory_data():
            if isinstance(self.input_data.content, dict):
                self.event_data = self.input_data.content
                return
            raise ValueError("Data must be in JSON")

        input_data_type = self.input_data.data_type
        match input_data_type:
            case DataType.FILE:
                handle_file_data()
            case DataType.MEMORY:
                handle_memory_data()
            case _:
                typing.assert_never(input_data_type)

    def get_data(self) -> Union[dict, str]:
        if self.input_data.data_type == DataType.FILE:
            with open(self.event_data_file_path, "r", encoding="utf-8") as f:
                self.event_data = json.loads(f.read())
        return self.event_data

    def get_input_data_type(self) -> DataType:
        return self.input_data.data_type


class AlertHubTransformer(MontyDataTransformer[AlertHubDataSource]):
    """Transforms Alert Hub event data into STAC items"""

    hazard_profiles = MontyHazardProfiles()
    source_name = "alerthub"

    def make_items(self) -> list[Item]:
        return list(self.get_stac_items())

    def get_stac_items(self) -> typing.Generator[Item, None, None]:
        self.transform_summary.mark_as_started()

        try:
            parsed = AlertHubDataValidator(**self.data_source.get_data())
            alerts = parsed.data.public.historicalAlerts.items

            for alert in alerts:
                try:
                    self.transform_summary.increment_rows(1)
                    source_event_item = self.make_source_event_item(alert=alert)
                    if source_event_item:
                        yield source_event_item
                        yield self.make_hazard_event_item(alert=alert, event_item=source_event_item)
                    else:
                        self.transform_summary.increment_failed_rows()
                except Exception:
                    self.transform_summary.increment_failed_rows()
        except Exception:
            logger.warning("Failed to process Alerthub data", exc_info=True)
        self.transform_summary.mark_as_complete()

    def _get_hazard_codes(self, category: str, event: str) -> List[str] | None:
        """Returns the hazard codes based on hazard"""

        # TODO: the event value might appear in different language.
        hazard_mapping: Dict[Tuple[str, str], List[str]] = {
            ("met", "flood watch"): ["MH0600", "nat-hyd-flo-flo", "FL"],  # Flooding (chapeau)
            ("met", "tropical cyclone"): ["MH0306"],  # Cyclone or depression
            ("met", "tsunami"): ["MH0705", "TS"],  # Tsunami
            ("met", "cold wave"): ["MH0502", "CW"],  # Cold wave
            ("met", "heat wave"): ["MH0501", "HT"],  # Heat wave
            ("met", "avalanches"): ["MH0801", "nat-geo-mmd-ava", "AV"],  # Avalanche
            ("met", "dangerous cold"): ["UNK"],  # Dangerous cold (TO FILL)
            ("geo", "earthquake"): ["GH0100", "EQ"],  # Earthquake
            ("geo", "volcanic eruption"): ["GH0201"],  # Volcanic eruption
            ("geo", "landslide"): ["GH0300"],  # Landslide
            ("env", "drought"): ["MH0401", "DR"],  # Drought
            ("safety", "cbrne event"): ["TE0100", "CE"],  # Chemical emergency
            ("health", "epidemic"): ["BI0100", "EP"],
        }
        key = (category.lower().strip(), event.lower().strip())
        return hazard_mapping.get(key)

    def _create_hazard_detail(self, severity_label: str) -> HazardDetail:
        """Create hazard detail based on severity label"""

        severity_mapping = {"EXTREME": 5, "SEVERE": 4, "MODERATE": 3, "MINOR": 2, "UNKNOWN": 0}

        return HazardDetail(
            severity_value=severity_mapping.get(severity_label, 0),
            severity_unit="alerthub",
            estimate_type=MontyEstimateType.PRIMARY,
        )

    def _get_geometry(self, areas: list[Area]) -> Union[Polygon, MultiPolygon] | None:
        polygons = []
        for area in areas:
            for polygon in area.polygons:
                coords = polygon.valuePolygon.coordinates
                shell = coords[0]
                holes = coords[1:] if len(coords) > 1 else []
                polygons.append(Polygon(shell, holes))

        if not polygons:
            return None

        if len(polygons) == 1:
            return polygons[0]

        return MultiPolygon(polygons)

    def _info_item(self, alert: AlertItem) -> AlertInfo | AlertInfoSummary | None:
        """Get the info item"""
        # Note: Get the Info data from `info` field except areas based on language
        # Note: Get the `areas` data always from `info` field to avoid timeout during extraction
        # as data in `info` and `infos` are quite repetitive (just few fields are in different lang)
        if not alert.info:
            return None

        if alert.info.language in ["en-US", "en"]:
            return alert.info

        for item in alert.infos:
            if item.language in ["en-US", "en"]:
                return AlertInfo(**item.model_dump(), areas=alert.info.areas)

        # If english language not found, fallback to whatever is available
        return alert.info

    def make_source_event_item(self, alert: AlertItem) -> Item | None:
        """Create event item"""
        # Identifier for the Item
        id = STAC_EVENT_ID_PREFIX + str(alert.id)

        info_item = self._info_item(alert=alert)

        if not info_item:
            return None

        description = md(info_item.description or " ")  # atleast 1 char in length

        ah_startdate = info_item.effective
        if isinstance(ah_startdate, str):
            startdate = pytz.utc.localize(datetime.datetime.fromisoformat(ah_startdate))
        else:
            startdate = ah_startdate

        ah_enddate = info_item.expires
        if isinstance(ah_enddate, str):
            enddate = pytz.utc.localize(datetime.datetime.fromisoformat(ah_enddate))
        else:
            enddate = ah_enddate

        if not startdate or not enddate:
            logging.info("Event start date or end date is missing")
            return None

        geometry = self._get_geometry(info_item.areas)
        if not geometry:
            logging.info("The geometry object is empty.")
            return None

        bbox = list(geometry.bounds)

        item = Item(
            id=id,
            geometry=mapping(geometry),
            bbox=bbox,
            datetime=startdate,
            properties={
                "title": info_item.headline,
                "description": description,
                "start_datetime": startdate.isoformat(),
                "end_datetime": enddate.isoformat(),
            },
        )

        # Monty extension fields
        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = 1
        monty.hazard_codes = self._get_hazard_codes(category=info_item.category, event=info_item.event)
        monty.hazard_codes = self.hazard_profiles.get_canonical_hazard_codes(item=item)
        monty.country_codes = [alert.country.iso3] if alert.country and alert.country.iso3 else None
        hazard_keywords = self.hazard_profiles.get_keywords(monty.hazard_codes)
        item.properties["keywords"] = list(set(hazard_keywords + monty.country_codes if monty.country_codes else []))
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        # links
        item.add_link(Link("via", alert.url, "application/xml", "Alerthub Event data"))
        return item

    def make_hazard_event_item(self, alert: AlertItem, event_item: Item) -> Item:
        """Create hazard item"""
        hazard_item = event_item.clone()
        hazard_item.id = hazard_item.id.replace(STAC_EVENT_ID_PREFIX, STAC_HAZARD_ID_PREFIX)
        hazard_item.set_collection(self.get_hazard_collection())
        hazard_item.properties["roles"] = ["source", "hazard"]

        # Add hazard details
        monty = MontyExtension.ext(hazard_item)
        monty.hazard_codes = [self.hazard_profiles.get_undrr_2025_code(hazard_codes=monty.hazard_codes)]

        monty.hazard_detail = self._create_hazard_detail(severity_label=alert.info.severity)

        return hazard_item
