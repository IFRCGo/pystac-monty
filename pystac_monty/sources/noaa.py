from pystac_monty.sources.common import MontyDataSource
from pystac_monty.hazard_profiles import HazardProfiles
import pandas as pd
from typing import Any, List, Optional
from shapely.geometry import Point, mapping
from datetime import datetime
import pytz
import requests
from pystac import Collection, Item, Link
from pystac_monty.extension import HazardDetail, MontyExtension, MontyEstimateType
from pystac_monty.hazard_profiles import HazardProfiles
from pystac_monty.sources.common import MontyDataSource
import json

STAC_EVENT_ID_PREFIX = "noaa-event-"
STAC_HAZARD_ID_PREFIX = "noaa-hazard-"


class NoaaIbtracsSource(MontyDataSource):
    df: pd.DataFrame

    def __init__(self, source_url: str, data: Any) -> None:
        super().__init__(source_url, data)
        self.data = pd.read_csv(data)


class NoaaIbtracsTransformer:
    print("NoaaIbtracsTransformer")
    """ 
    Transforms NOAA IBTrACS data into STAC Items
    """
    hazards_profiles = HazardProfiles()

    ibtracs_events_collection_url = (
        "https://gitea.local.togglecorp.com/togglecorp/pystac-extension/raw/branch/main/examples/noaa-events/noaa-events.json"
    )

    ibtracs_hazards_collection_url = (
        "https://gitea.local.togglecorp.com/togglecorp/pystac-extension/raw/branch/main/examples/noaa-events/noaa-events.json"
    )

    def __init__(self, data: NoaaIbtracsSource = None):
        self.data = data

    def get_event_collection(self) -> Collection:
        """Get event collection"""
        response = requests.get(self.ibtracs_events_collection_url)
        collection_dict = json.loads(response.text)
        return Collection.from_dict(collection_dict)
    
    def get_hazard_collection(self) -> Collection:
        """Get hazard collection"""
        response = requests.get(self.ibtracs_hazards_collection_url)
        collection_dict = json.loads(response.text)
        return Collection.from_dict(collection_dict)

    def make_items(self):
        """Create all STAC items from EM-DAT data"""
        items = []

        # Create event items
        event_items = self.make_source_event_items()
        items.extend(event_items)

        #Create hazard items
        hazard_items = self.make_hazard_event_items()
        items.extend(hazard_items)

        return items

    def make_source_event_items(self):
        """Create source event items from NOAA IBTrACS data"""
        event_items = []
        df = self.data.get_data()
        cleaned_data = df.iloc[2:].reset_index(drop=True)

        for _, row in cleaned_data.iterrows():

            try:
                item = self._create_event_item_from_row(row)
                print(item)
                if item:
                    event_items.append(item)
            except Exception as e:
                print(f"Error creating event item for SID {row.get('SID', 'unknown')}: {str(e)}")


        return event_items

    def _create_event_item_from_row(self, row):
        if pd.isna(row.get("SID")):
            return None

        geometry = None
        bbox = None
        if geometry is None and not pd.isna(row.get("LAT")) and not pd.isna(row.get("LON")):
            point = Point(float(row["LON"]), float(row["LAT"]))
            geometry = mapping(point)
            bbox = [float(row["LON"]), float(row["LAT"]), float(row["LON"]), float(row["LAT"])]
        item = Item(
            id=f"{STAC_EVENT_ID_PREFIX}{row['SID']}",
            geometry=geometry,
            bbox=bbox,
            datetime=datetime.strptime(row.get("ISO_TIME"), "%Y-%m-%d %H:%M:%S"),
            properties={},
        )
        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = 1  # This should be dynamically determined based on existing events
        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]


        return item

    def make_hazard_event_items(self) -> List:
        """Create hazard items based on event items"""
        hazard_items = []
        event_items = self.make_source_event_items()

        for event_item in event_items:
            hazard_item = self._create_hazard_item_from_event(event_item)
            if hazard_item:
                hazard_items.append(hazard_item)

        return hazard_items
    
    def _get_row_by_sid(self, sid: str) -> Optional[pd.Series]:
        """Get original DataFrame row by SID"""
        df = self.data.get_data()
        matching_rows = df[df["SID"] == sid]
        return matching_rows.iloc[0] if not matching_rows.empty else None

    def _create_hazard_item_from_event(self, event_item: Item) -> Optional[Item]:
        """Create a hazard item from an event item"""
        hazard_item = event_item.clone()
        hazard_item.id = hazard_item.id.replace(STAC_EVENT_ID_PREFIX, STAC_HAZARD_ID_PREFIX)
        hazard_item.set_collection(self.get_hazard_collection())
        hazard_item.properties["roles"] = ["source", "hazard"]

        # Add hazard detail
        monty = MontyExtension.ext(hazard_item)
        original_row = self._get_row_by_sid(hazard_item.id.replace(STAC_HAZARD_ID_PREFIX, ""))
        if original_row is not None:
            monty.hazard_detail = self._create_hazard_detail(original_row)

        return hazard_item
    
    def _create_hazard_detail(self, row: pd.Series) -> HazardDetail:
        """Create hazard detail from row data"""
        # First map EM-DAT classification to UNDRR-ISC codes

        return HazardDetail(
            cluster="TC",
            severity_value="",
            severity_unit="",
            estimate_type=MontyEstimateType.PRIMARY,
        )