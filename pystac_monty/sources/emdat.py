from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Union

import numpy as np
import pandas as pd
import pytz
from pystac import Item, Link
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
from pystac_monty.sources.common import MontyDataSource, MontyDataTransformer
from pystac_monty.utils import rename_columns

STAC_EVENT_ID_PREFIX = "emdat-event-"
STAC_HAZARD_ID_PREFIX = "emdat-hazard-"
STAC_IMPACT_ID_PREFIX = "emdat-impact-"


@dataclass
class EMDATDataSource(MontyDataSource):
    """EM-DAT data source that can handle both Excel files and pandas DataFrames"""

    df: pd.DataFrame

    def __init__(self, source_url: str, data: Union[str, pd.DataFrame]):
        super().__init__(source_url, data)
        if isinstance(data, str):
            # If data is a string, assume it's Excel content
            self.df = pd.read_excel(data)
        elif isinstance(data, pd.DataFrame):
            self.df = data
        elif isinstance(data, dict):
            # If data is a dict, assume it's Json content
            data = data["data"]["public_emdat"]["data"]
            df = pd.DataFrame(data)
            self.df = rename_columns(df)
        else:
            raise ValueError("Data must be either Excel content (str) or pandas DataFrame or Json")

    def get_data(self) -> pd.DataFrame:
        return self.df


class EMDATTransformer(MontyDataTransformer):
    """
    Transforms EM-DAT event data into STAC Items
    """

    hazard_profiles = MontyHazardProfiles()

    def __init__(self, data: EMDATDataSource) -> None:
        """
        Initialize EMDATTransformer

        Args:
            data: EMDATDataSource containing the EM-DAT data
            gaul_path: Path to the GAUL geopackage file or ZIP containing it
        """
        super().__init__("emdat")
        self.data = data

    def make_items(self) -> list[Item]:
        """Create all STAC items from EM-DAT data"""
        items = []

        # Create event items
        event_items = self.make_source_event_items()
        items.extend(event_items)

        # Create hazard items
        hazard_items = self.make_hazard_event_items()
        items.extend(hazard_items)

        # Create impact items
        impact_items = self.make_impact_items()
        items.extend(impact_items)

        return items

    def make_source_event_items(self) -> List[Item]:
        """Create source event items from EM-DAT data"""
        event_items = []
        df = self.data.get_data()

        for _, row in df.iterrows():
            try:
                item = self._create_event_item_from_row(row)
                if item:
                    event_items.append(item)
            except Exception as e:
                print(f"Error creating event item for DisNo {row.get('DisNo.', 'unknown')}: {str(e)}")
                continue

        return event_items

    def _create_event_item_from_row(self, row: pd.Series) -> Optional[Item]:
        """Create a single event item from a DataFrame row"""
        # Skip if required fields are missing
        if pd.isna(row.get("DisNo.")):
            return None

        # Create geometry from lat/lon if available
        # Try each geometry source in order of preference
        geometry = None
        bbox = None

        # 1. Try admin units first if geocoder is available
        if self.geocoder and np.any(pd.notna(row.get("Admin Units"))):
            geom_data = self.geocoder.get_geometry_from_admin_units(row.get("Admin Units"))
            if geom_data:
                geometry = geom_data["geometry"]
                bbox = geom_data["bbox"]

        # 2. Fall back to lat/lon if available
        if geometry is None and not pd.isna(row.get("Latitude")) and not pd.isna(row.get("Longitude")):
            point = Point(float(row["Longitude"]), float(row["Latitude"]))
            geometry = mapping(point)
            bbox = [float(row["Longitude"]), float(row["Latitude"]), float(row["Longitude"]), float(row["Latitude"])]

        # 3. Finally, try country geometry if geocoder is available
        if geometry is None and self.geocoder and not pd.isna(row.get("Country")):
            geom_data = self.geocoder.get_geometry_by_country_name(row["Country"])
            if geom_data:
                geometry = geom_data["geometry"]
                bbox = geom_data["bbox"]

        # Create event datetime
        start_date, end_date = self._create_datetimes(row)

        # Create item
        item = Item(
            id=f"{STAC_EVENT_ID_PREFIX}{row['DisNo.']}",
            geometry=geometry,
            bbox=bbox,
            datetime=start_date,
            start_datetime=start_date,
            end_datetime=end_date,
            properties={
                "title": self._create_title_from_row(row),
                "description": self._create_description_from_row(row),
            },
        )

        # Add Monty extension
        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = 1  # EM-DAT doesn't have episodes
        monty.hazard_codes = [row.get("Classification Key", "")]
        monty.country_codes = [row["ISO"]] if not pd.isna(row.get("ISO")) else []
        monty.compute_and_set_correlation_id()

        # Set collection and roles
        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        # Add source link
        item.add_link(Link("via", f"https://public.emdat.be/data/{row['DisNo.']}", "text/html", "EM-DAT Event Data"))

        return item

    def make_hazard_event_items(self) -> List[Item]:
        """Create hazard items based on event items"""
        hazard_items = []
        event_items = self.make_source_event_items()

        for event_item in event_items:
            hazard_item = self._create_hazard_item_from_event(event_item)
            if hazard_item:
                hazard_items.append(hazard_item)

        return hazard_items

    def _create_hazard_item_from_event(self, event_item: Item) -> Optional[Item]:
        """Create a hazard item from an event item"""
        hazard_item = event_item.clone()
        hazard_item.id = hazard_item.id.replace(STAC_EVENT_ID_PREFIX, STAC_HAZARD_ID_PREFIX)
        hazard_item.set_collection(self.get_hazard_collection())
        hazard_item.properties["roles"] = ["source", "hazard"]

        # Add hazard detail
        monty = MontyExtension.ext(hazard_item)
        original_row = self._get_row_by_disno(hazard_item.id.replace(STAC_HAZARD_ID_PREFIX, ""))
        if original_row is not None:
            monty.hazard_detail = self._create_hazard_detail(hazard_item, original_row)

        return hazard_item

    def make_impact_items(self) -> List[Item]:
        """Create impact items from EM-DAT data"""
        impact_items = []
        df = self.data.get_data()

        for _, row in df.iterrows():
            impact_items.extend(self._create_impact_items_from_row(row))

        return impact_items

    def _create_impact_items_from_row(self, row: pd.Series) -> List[Item]:
        """Create impact items from a single row"""
        impact_items = []
        impact_fields = {
            "Total Deaths": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.DEATH),
            "No Injured": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.INJURED),
            "No Affected": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.TOTAL_AFFECTED),
            "No Homeless": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.TOTAL_DISPLACED_PERSONS),
            "Total Affected": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.TOTAL_AFFECTED),
            "Total Damages ('000 US$)": (MontyImpactExposureCategory.TOTAL_AFFECTED, MontyImpactType.LOSS_COST),
        }

        for field, (category, impact_type) in impact_fields.items():
            if not pd.isna(row.get(field)) and float(row[field]) > 0:
                impact_item = self._create_impact_item(row, field, category, impact_type)
                if impact_item:
                    impact_items.append(impact_item)

        return impact_items

    def _create_impact_item(
        self, row: pd.Series, field: str, category: MontyImpactExposureCategory, impact_type: MontyImpactType
    ) -> Optional[Item]:
        """Create a single impact item"""
        try:
            base_item = self._create_event_item_from_row(row)
            if not base_item:
                return None

            impact_item = base_item.clone()
            # add in title
            impact_item.properties["title"] = f"{base_item.properties['title']} - {field}"
            impact_item.id = f"{STAC_IMPACT_ID_PREFIX}{row['DisNo.']}-{field.lower().replace(' ', '-')}"
            impact_item.set_collection(self.get_impact_collection())
            impact_item.properties["roles"] = ["source", "impact"]

            monty = MontyExtension.ext(impact_item)
            monty.impact_detail = ImpactDetail(
                category=category,
                type=impact_type,
                value=float(row[field]),
                unit="USD" if "Damages" in field else "count",
                estimate_type=MontyEstimateType.PRIMARY,
            )

            return impact_item
        except Exception as e:
            print(f"Error creating impact item for field {field}: {str(e)}")
            return None

    def _create_datetimes(self, row: pd.Series) -> (datetime, datetime):
        """Create datetime object from EM-DAT date fields"""
        start_year = int(row["Start Year"]) if not pd.isna(row.get("Start Year")) else None
        start_month = int(row["Start Month"]) if not pd.isna(row.get("Start Month")) else 1
        start_day = int(row["Start Day"]) if not pd.isna(row.get("Start Day")) else 1
        end_year = int(row["End Year"]) if not pd.isna(row.get("End Year")) else None
        end_month = int(row["End Month"]) if not pd.isna(row.get("End Month")) else 1
        end_day = int(row["End Day"]) if not pd.isna(row.get("End Day")) else 1

        if start_year:
            start_dt = datetime(start_year, start_month, start_day)
            if end_year:
                end_dt = datetime(end_year, end_month, end_day)
                return pytz.utc.localize(start_dt), pytz.utc.localize(end_dt)

            return pytz.utc.localize(start_dt), None
        return None

    def _create_hazard_detail(self, item: Item, row: pd.Series) -> HazardDetail:
        """Create hazard detail from row data"""
        # First map EM-DAT classification to UNDRR-ISC codes

        return HazardDetail(
            cluster=self.hazard_profiles.get_cluster_code(item),
            severity_value=float(row["Magnitude"]) if not pd.isna(row.get("Magnitude")) else None,
            severity_unit=row.get("Magnitude Scale", "emdat"),
            estimate_type=MontyEstimateType.PRIMARY,
        )

    def _get_row_by_disno(self, disno: str) -> Optional[pd.Series]:
        """Get original DataFrame row by DisNo"""
        df = self.data.get_data()
        matching_rows = df[df["DisNo."] == disno]
        return matching_rows.iloc[0] if not matching_rows.empty else None

    def _create_title_from_row(self, row: pd.Series) -> Optional[str]:
        """Create a descriptive title from row data when Event Name is missing"""
        if not pd.isna(row.get("Event Name")):
            return str(row["Event Name"])

        components = []

        # Add disaster type
        if not pd.isna(row.get("Disaster Type")):
            components.append(row["Disaster Type"])
            if not pd.isna(row.get("Disaster Subtype")) and row["Disaster Type"] != row["Disaster Subtype"]:
                components.append(f"({row['Disaster Subtype']})")

        # Add location info
        locations = []
        if not pd.isna(row.get("Country")):
            locations.append(row["Country"])
        if locations:
            components.append("in")
            components.append(", ".join(locations))

        # Add date
        date_str = None
        if not pd.isna(row.get("Start Year")):
            date_components = []
            # Add month if available
            if not pd.isna(row.get("Start Month")):
                try:
                    month_name = datetime(2000, int(row["Start Month"]), 1).strftime("%B")
                    date_components.append(month_name)
                except ValueError:
                    pass
            # Add year
            date_components.append(str(int(row["Start Year"])))
            if date_components:
                date_str = " ".join(date_components)

        if date_str:
            components.extend(["of", date_str])

        return " ".join(components) if components else None

    def _create_description_from_row(self, row: pd.Series) -> str:
        """Create a description from row data"""
        components = []

        # Add disaster type
        if not pd.isna(row.get("Disaster Type")):
            components.append(row["Disaster Type"])
            if not pd.isna(row.get("Disaster Subtype")) and row["Disaster Type"] != row["Disaster Subtype"]:
                components.append(f"({row['Disaster Subtype']})")

        # Add location info
        locations = []
        if not pd.isna(row.get("Location")):
            locations.append(row["Location"])
        if not pd.isna(row.get("Country")):
            locations.append(row["Country"])
        if locations:
            components.append("in")
            components.append(", ".join(locations))

        # Add date
        date_str = None
        if not pd.isna(row.get("Start Year")):
            date_components = []
            # Add month if available
            if not pd.isna(row.get("Start Month")):
                try:
                    month_name = datetime(2000, int(row["Start Month"]), 1).strftime("%B")
                    date_components.append(month_name)
                except ValueError:
                    pass
            # Add year
            date_components.append(str(int(row["Start Year"])))
            if date_components:
                date_str = " ".join(date_components)

        if date_str:
            components.extend(["of", date_str])

        return " ".join(components) if components else "Unnamed Event"
