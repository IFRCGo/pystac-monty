"""USGS data transformer for STAC Items."""

import json
from datetime import datetime
from typing import List, Optional

import pytz
import requests
from pystac import Asset, Collection, Item, Link
from shapely.geometry import Point, mapping

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

STAC_EVENT_ID_PREFIX = "usgs-event-"
STAC_HAZARD_ID_PREFIX = "usgs-hazard-"
STAC_IMPACT_ID_PREFIX = "usgs-impact-"


class USGSDataSource(MontyDataSource):
    """USGS data source that can handle both event detail and losses data."""

    def __init__(self, source_url: str, data: str, losses_data: Optional[str] = None):
        """Initialize USGS data source.

        Args:
            source_url: URL where the data was retrieved from
            data: Event detail data as JSON string
            losses_data: Optional PAGER losses data as JSON string
        """
        super().__init__(source_url, data)
        self.data = json.loads(data)
        self.losses_data = json.loads(losses_data) if losses_data else None

    def get_data(self) -> dict:
        """Get the event detail data."""
        return self.data

    def get_losses_data(self) -> Optional[dict]:
        """Get the PAGER losses data if available."""
        return self.losses_data


class USGSTransformer:
    """Transforms USGS earthquake event data into STAC Items."""

    usgs_events_collection_url = (
        "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/usgs/examples/usgs-events/usgs-events.json"
    )
    usgs_hazards_collection_url = (
        "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/usgs/examples/usgs-hazards/usgs-hazards.json"
    )
    usgs_impacts_collection_url = (
        "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/usgs/examples/usgs-impacts/usgs-impacts.json"
    )

    hazard_profiles = HazardProfiles()

    def __init__(self, data: USGSDataSource) -> None:
        """Initialize USGS transformer.

        Args:
            data: USGSDataSource containing event detail and optional losses data
        """
        self.data = data

    def make_items(self) -> List[Item]:
        """Create STAC items from USGS data."""
        items = []

        # Create event item
        event_item = self.make_source_event_item()
        items.append(event_item)

        # Create hazard item (ShakeMap)
        hazard_item = self.make_hazard_event_item()
        items.append(hazard_item)

        # Create impact items (PAGER)
        if self.data.get_losses_data():
            impact_items = self.make_impact_items()
            items.extend(impact_items)

        return items

    def make_source_event_item(self) -> Item:
        """Create source event item from USGS data."""
        event_data = self.data.get_data()

        # Create geometry from coordinates
        longitude = event_data["geometry"]["coordinates"][0]
        latitude = event_data["geometry"]["coordinates"][1]
        point = Point(longitude, latitude)

        event_datetime = datetime.fromtimestamp(event_data["properties"]["time"] / 1000, pytz.UTC)

        item = Item(
            id=f"{STAC_EVENT_ID_PREFIX}{event_data['id']}",
            geometry=mapping(point),
            bbox=[longitude, latitude, longitude, latitude],
            datetime=event_datetime,
            properties={
                "title": event_data["properties"].get("title", ""),
                "description": event_data["properties"].get("place", ""),
                "eq:magnitude": event_data["properties"].get("mag"),
                "eq:magnitude_type": event_data["properties"].get("magType"),
                "eq:status": event_data["properties"].get("status"),
                "eq:tsunami": bool(event_data["properties"].get("tsunami")),
                "eq:felt": event_data["properties"].get("felt"),
                "eq:depth": event_data["properties"].get("depth"),
            },
        )

        # Add Monty extension
        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = 1
        monty.hazard_codes = ["GH0004"]  # Earthquake surface rupture code

        # Get country code from event data or geometry
        country_codes = ["CHN"]  # This should be derived from coordinates
        monty.country_codes = country_codes

        # Compute correlation ID
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        # Set collection and roles
        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        # Add source link and assets
        item.add_link(Link("via", self.data.get_source_url(), "application/json", "USGS Event Data"))
        item.add_asset(
            "source",
            Asset(
                href=self.data.get_source_url(),
                media_type="application/geo+json",
                title="USGS GeoJSON Source",
                roles=["source"],
            ),
        )

        return item

    def make_hazard_event_item(self) -> Item:
        """Create hazard item (ShakeMap) from USGS data."""
        event_item = self.make_source_event_item()
        hazard_item = event_item.clone()
        hazard_item.id = f"{STAC_HAZARD_ID_PREFIX}{hazard_item.id.replace(STAC_EVENT_ID_PREFIX, '')}-shakemap"

        # Set collection and roles
        hazard_item.set_collection(self.get_hazard_collection())
        hazard_item.properties["roles"] = ["source", "hazard"]

        # Add hazard detail
        monty = MontyExtension.ext(hazard_item)
        monty.hazard_detail = HazardDetail(
            cluster="GEO-SEIS",
            severity_value=float(event_item.properties.get("eq:magnitude", 0)),
            severity_unit=event_item.properties.get("eq:magnitude_type", ""),
            estimate_type=MontyEstimateType.PRIMARY,
        )

        # Add shakemap assets
        shakemap_assets = {
            "intensity_map": {
                "href": f"{self.data.get_source_url()}/download/intensity.jpg",
                "media_type": "image/jpeg",
                "title": "Intensity Map",
                "roles": ["overview"],
            },
            "intensity_overlay": {
                "href": f"{self.data.get_source_url()}/download/intensity_overlay.png",
                "media_type": "image/png",
                "title": "Intensity Overlay",
                "roles": ["visual"],
            },
            "mmi_contours": {
                "href": f"{self.data.get_source_url()}/download/cont_mi.json",
                "media_type": "application/json",
                "title": "MMI Contours",
                "roles": ["data"],
            },
            "grid": {
                "href": f"{self.data.get_source_url()}/download/grid.xml",
                "media_type": "application/xml",
                "title": "Ground Motion Grid",
                "roles": ["data"],
            },
        }

        for key, asset_info in shakemap_assets.items():
            hazard_item.add_asset(key, Asset(**asset_info))

        return hazard_item

    def make_impact_items(self) -> List[Item]:
        """Create impact items (PAGER) from USGS data."""
        impact_items = []
        losses_data = self.data.get_losses_data()

        if not losses_data:
            return impact_items

        # Create fatalities impact item
        if "empirical_fatality" in losses_data:
            fatalities_item = self._create_impact_item_from_losses(
                "fatalities",
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.DEATH,
                losses_data["empirical_fatality"]["total_fatalities"],
                "people",
            )
            impact_items.append(fatalities_item)

        # Create economic losses impact item
        if "empirical_economic" in losses_data:
            economic_item = self._create_impact_item_from_losses(
                "economic",
                MontyImpactExposureCategory.BUILDINGS,
                MontyImpactType.LOSS_COST,
                losses_data["empirical_economic"]["total_dollars"],
                "usd",
            )
            impact_items.append(economic_item)

        return impact_items

    def _create_impact_item_from_losses(
        self, impact_type: str, category: MontyImpactExposureCategory, imp_type: MontyImpactType, value: float, unit: str
    ) -> Item:
        """Helper method to create impact items from PAGER losses data."""
        event_item = self.make_source_event_item()
        impact_item = event_item.clone()
        impact_item.id = f"{STAC_IMPACT_ID_PREFIX}{impact_item.id.replace(STAC_EVENT_ID_PREFIX, '')}-{impact_type}"

        # Set title and description
        title_prefix = "Estimated Fatalities" if impact_type == "fatalities" else "Estimated Economic Losses"
        impact_item.properties["title"] = f"{title_prefix} for {event_item.properties['title']}"
        impact_item.properties["description"] = f"PAGER {title_prefix.lower()} for {event_item.common_metadata.title}"

        # Set collection and roles
        impact_item.set_collection(self.get_impact_collection())
        impact_item.properties["roles"] = ["source", "impact"]

        # Add impact detail
        monty = MontyExtension.ext(impact_item)
        monty.impact_detail = ImpactDetail(
            category=category, type=imp_type, value=float(value), unit=unit, estimate_type=MontyEstimateType.MODELLED
        )

        # Add PAGER assets
        pager_assets = {
            "pager_onepager": {
                "href": f"{self.data.get_source_url()}/onepager.pdf",
                "media_type": "application/pdf",
                "title": "PAGER One-Pager Report",
                "roles": ["data"],
            },
            "pager_exposure": {
                "href": f"{self.data.get_source_url()}/json/exposures.json",
                "media_type": "application/json",
                "title": "PAGER Exposure Data",
                "roles": ["data"],
            },
            "pager_alert": {
                "href": f"{self.data.get_source_url()}/alert{impact_type}.pdf",
                "media_type": "application/pdf",
                "title": f"PAGER {impact_type.title()} Alert",
                "roles": ["data"],
            },
        }

        for key, asset_info in pager_assets.items():
            impact_item.add_asset(key, Asset(**asset_info))

        return impact_item

    def get_event_collection(self) -> Collection:
        """Get event collection."""
        response = requests.get(self.usgs_events_collection_url)
        return Collection.from_dict(json.loads(response.text))

    def get_hazard_collection(self) -> Collection:
        """Get hazard collection."""
        response = requests.get(self.usgs_hazards_collection_url)
        return Collection.from_dict(json.loads(response.text))

    def get_impact_collection(self) -> Collection:
        """Get impact collection."""
        response = requests.get(self.usgs_impacts_collection_url)
        return Collection.from_dict(json.loads(response.text))
