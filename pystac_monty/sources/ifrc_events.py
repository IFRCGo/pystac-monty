import json
import logging
import typing
from typing import List

from pystac import Item

from pystac_monty.extension import (
    ImpactDetail,
    MontyEstimateType,
    MontyExtension,
    MontyImpactExposureCategory,
    MontyImpactType,
)
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import MontyDataSource, MontyDataTransformer
from pystac_monty.validators.ifrc import IFRCsourceValidator

logger = logging.getLogger(__name__)

STAC_EVENT_ID_PREFIX = "ifrcevent-event-"
STAC_IMPACT_ID_PREFIX = "ifrcevent-impact-"


class IFRCEventDataSource(MontyDataSource):
    def __init__(self, source_url: str, data: str):
        # FIXME: Why do we load using json
        super().__init__(source_url, json.loads(data))


class IFRCEventTransformer(MontyDataTransformer[IFRCEventDataSource]):
    hazard_profiles = MontyHazardProfiles()
    source_name = "ifrcevent"

    def make_items(self):
        return list(self.get_stac_items())

    def get_stac_items(self) -> typing.Generator[Item, None, None]:
        ifrcevent_data = self.data_source.data

        self.transform_summary.mark_as_started()
        for data in ifrcevent_data:
            self.transform_summary.increment_rows()
            try:
                def parse_data(row: dict):
                    return IFRCsourceValidator(**row)

                ifrcdata = parse_data(data)
                if event_item := self.make_source_event_item(ifrcdata):
                    yield event_item
                    yield from self.make_impact_items(event_item, ifrcdata)
                else:
                    self.transform_summary.increment_failed_rows()
            except Exception:
                self.transform_summary.increment_failed_rows()
                logger.error("Failed to process ifrc events", exc_info=True)

    def make_source_event_item(self, data: IFRCsourceValidator) -> Item:
        """Create an event item"""
        geometry = None
        bbox = None
        geom_data = self.geocoder.get_geometry_by_country_name(data.countries[0].name)

        if geom_data:
            geometry = geom_data["geometry"]
            bbox = geom_data["bbox"]

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
        items = []

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
        impact_item = event_item.clone()
        for impact_field, (category, impact_type) in impact_field_category_map.items():
            impact_item.id = f"{STAC_IMPACT_ID_PREFIX}-{ifrcevent_data.id}"
            impact_item.properties["roles"] = ["source", "impact"]
            impact_item.set_collection(self.get_impact_collection())

            monty = MontyExtension.ext(impact_item)

            # only save impact value if not null
            value = None
            for field in impact_field:
                if len(ifrcevent_data.field_reports) > 0 and hasattr(ifrcevent_data.field_reports[0], field):
                    value = getattr(ifrcevent_data.field_reports[0], field)
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

    def check_accepted_disaster_types(self, disaster):
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
