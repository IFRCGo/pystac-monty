import json
import logging
import typing
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Union

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
from pystac_monty.validators.em_dat import EmdatDataValidator

logger = logging.getLogger(__name__)

STAC_EVENT_ID_PREFIX = "emdat-event-"
STAC_HAZARD_ID_PREFIX = "emdat-hazard-"
STAC_IMPACT_ID_PREFIX = "emdat-impact-"


@dataclass
class EMDATDataSource(MontyDataSource):
    """EM-DAT data source that can handle both Excel files and pandas DataFrames"""

    df: pd.DataFrame

    def __init__(self, source_url: str, data: Union[str, dict[str, typing.Any], pd.DataFrame]):
        super().__init__(source_url, data)

        def rename_excel_df(df: pd.DataFrame):
            df.rename(columns={
                "DisNo.": "disno",
                "Historic": "historic",
                "Classification Key": "classif_key",
                "Disaster Group": "group",
                "Disaster Subgroup": "subgroup",
                "Disaster Type": "type",
                "Disaster Subtype": "subtype",
                "External IDs": "external_ids",
                "Event Name": "name",
                "ISO": "iso",
                "Country": "country",
                "Subregion": "subregion",
                "Region": "region",
                "Location": "location",
                "Origin": "origin",
                "Associated Types": "associated_types",
                "OFDA/BHA Response": "ofda_response",
                "Appeal": "appeal",
                "Declaration": "declaration",
                "AID Contribution ('000 US$)": "aid_contribution",
                "Magnitude": "magnitude",
                "Magnitude Scale": "magnitude_scale",
                "Latitude": "latitude",
                "Longitude": "longitude",
                "River Basin": "river_basin",
                "Start Year": "start_year",
                "Start Month": "start_month",
                "Start Day": "start_day",
                "End Year": "end_year",
                "End Month": "end_month",
                "End Day": "end_day",
                "Total Deaths": "total_deaths",
                "No. Injured": "no_injured",
                "No. Affected": "no_affected",
                "No. Homeless": "no_homeless",
                "Total Affected": "total_affected",
                "Reconstruction Costs ('000 US$)": "reconstr_dam",
                "Reconstruction Costs, Adjusted ('000 US$)": "reconstr_dam_adj",
                "Insured Damage ('000 US$)": "insur_dam",
                "Insured Damage, Adjusted ('000 US$)": "insur_dam_adj",
                "Total Damage ('000 US$)": "total_dam",
                "Total Damage, Adjusted ('000 US$)": "total_dam_adj",
                "CPI": "cpi",
                "Admin Units": "admin_units",
                "Entry Date": "entry_date",
                "Last Update": "last_update"
            }, inplace=True)

        if isinstance(data, str):
            # If data is a string, assume it's Excel content
            self.df = pd.read_excel(data)
            rename_excel_df(self.df)
        elif isinstance(data, pd.DataFrame):
            self.df = data
            rename_excel_df(self.df)
        elif isinstance(data, dict):
            # If data is a dict, assume it's Json content
            data = data["data"]["public_emdat"]["data"]
            self.df = pd.DataFrame(data)
        else:
            raise ValueError("Data must be either Excel content (str) or pandas DataFrame or Json")

    def get_data(self) -> pd.DataFrame:
        return self.df


class EMDATTransformer(MontyDataTransformer[EMDATDataSource]):
    """
    Transforms EM-DAT event data into STAC Items
    """

    hazard_profiles = MontyHazardProfiles()
    source_name = "emdat"

    # FIXME: This is deprecated
    def make_items(self) -> list[Item]:
        return list(self.get_stac_items())

    def get_stac_items(self) -> typing.Generator[Item, None, None]:
        data = self.data_source.get_data()
        data = data.where(pd.notna(data), None)

        self.transform_summary.mark_as_started()
        for _, row in data.iterrows():
            self.transform_summary.increment_rows()
            row_dict = row.to_dict()
            try:
                data = EmdatDataValidator(**row_dict)
                if event_item := self.make_source_event_item(data):
                    yield event_item
                    yield self.make_hazard_event_item(event_item)
                    yield from self.make_impact_items(data, event_item)
                else:
                    self.transform_summary.increment_failed_rows()
            except Exception:
                self.transform_summary.increment_failed_rows()
                logger.error("Failed to process emdat", exc_info=True)
        self.transform_summary.mark_as_complete()

    def make_source_event_item(self, row: EmdatDataValidator) -> Optional[Item]:
        """Create a single event item from a DataFrame row"""

        # Create geometry from lat/lon if available
        # Try each geometry source in order of preference
        geometry = None
        bbox = None

        # 1. Try admin units first if geocoder is available
        if self.geocoder and row.admin_units:
            # FIXME: convert this to json str
            geom_data = self.geocoder.get_geometry_from_admin_units(
                json.dumps([unit.model_dump() for unit in row.admin_units])
            )
            if geom_data:
                geometry = geom_data["geometry"]
                bbox = geom_data["bbox"]

        # 2. Fall back to lat/lon if available
        if geometry is None and row.latitude is not None and row.longitude is not None:
            point = Point(row.longitude, row.latitude)
            geometry = mapping(point)
            bbox = [row.longitude, row.latitude, row.longitude, row.latitude]

        # 3. Finally, try country geometry if geocoder is available
        if geometry is None and self.geocoder and row.iso:
            geom_data = self.geocoder.get_geometry_from_iso3(row.iso)
            if geom_data:
                geometry = geom_data["geometry"]
                bbox = geom_data["bbox"]

        # 4. Finally, try country geometry if geocoder is available
        if geometry is None and self.geocoder and row.country:
            geom_data = self.geocoder.get_geometry_by_country_name(row.country)
            if geom_data:
                geometry = geom_data["geometry"]
                bbox = geom_data["bbox"]

        # Create event datetime
        start_date, end_date = self._create_datetimes(row)

        if not geometry:
            raise Exception("No geometry")

        # Create item
        item = Item(
            id=f"{STAC_EVENT_ID_PREFIX}{row.disno}",
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
        monty.hazard_codes = [row.classif_key]
        monty.country_codes = [row.iso] if row.iso else []

        monty.compute_and_set_correlation_id()

        # Set collection and roles
        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        # Add source link
        item.add_link(Link("via", f"https://public.emdat.be/data/{row.disno}", "text/html", "EM-DAT Event Data"))

        return item

    def make_hazard_event_item(self, event_item: Item) -> Item:
        """Create a hazard item from an event item"""
        hazard_item = event_item.clone()
        hazard_item.id = hazard_item.id.replace(STAC_EVENT_ID_PREFIX, STAC_HAZARD_ID_PREFIX)
        hazard_item.set_collection(self.get_hazard_collection())
        hazard_item.properties["roles"] = ["source", "hazard"]

        # Add hazard detail
        monty = MontyExtension.ext(hazard_item)

        original_row = self._get_row_by_disno(hazard_item.id.replace(STAC_HAZARD_ID_PREFIX, ""))
        if original_row is not None:
            monty.hazard_detail = self._create_hazard_detail(
                hazard_item,
                # FIXME: Do we need to do this
                EmdatDataValidator(**original_row.to_dict()),
            )

        return hazard_item

    def make_impact_items(self, row: EmdatDataValidator, event_item: Item) -> List[Item]:
        """Create impact items from a single row"""
        impact_items = []
        impact_fields = {
            "total_deaths": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.DEATH),
            "no_injured": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.INJURED),
            "no_affected": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.TOTAL_AFFECTED),
            "no_homeless": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.TOTAL_DISPLACED_PERSONS),
            "total_affected": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.TOTAL_AFFECTED),
            "total_dam": (MontyImpactExposureCategory.TOTAL_AFFECTED, MontyImpactType.LOSS_COST),
        }

        for field, (category, impact_type) in impact_fields.items():
            value = getattr(row, field, None)
            if value and float(value) > 0:
                impact_item = self._create_impact_item(row, field, category, impact_type, event_item)
                if impact_item:
                    impact_items.append(impact_item)

        return impact_items

    def _create_impact_item(
        self,
        row: EmdatDataValidator,
        # FIXME: Make this type script
        field: typing.Literal['total_deaths', 'no_injured', 'no_affected', 'total_affected', 'total_dam'],
        category: MontyImpactExposureCategory,
        impact_type: MontyImpactType,
        event_item: Item
    ) -> Optional[Item]:
        """Create a single impact item"""
        try:
            base_item = event_item
            if not base_item:
                return None

            impact_item = base_item.clone()
            # add in title
            impact_item.properties["title"] = f"{base_item.properties['title']} - {field}"
            impact_item.id = f"{STAC_IMPACT_ID_PREFIX}{row.disno}-{field.lower().replace(' ', '-')}"
            impact_item.set_collection(self.get_impact_collection())
            impact_item.properties["roles"] = ["source", "impact"]

            monty = MontyExtension.ext(impact_item)
            value: float | None = getattr(row, field, None)

            if not value:
                return None

            monty.impact_detail = ImpactDetail(
                category=category,
                type=impact_type,
                value=value,
                unit="USD" if "Damages" in field else "count",
                estimate_type=MontyEstimateType.PRIMARY,
            )

            return impact_item
        except Exception as e:
            print(f"Error creating impact item for field {field}: {str(e)}")
            return None

    def _create_datetimes(self, row: EmdatDataValidator) -> typing.Tuple[datetime | None, datetime | None]:
        """Create datetime object from EM-DAT date fields"""
        start_year = row.start_year or None
        start_month = row.start_month or 1
        start_day = row.start_day or 1
        end_year = row.end_year or None
        end_month = row.end_month or 1
        end_day = row.end_day or 1

        if start_year:
            start_dt = datetime(start_year, start_month, start_day)
            if end_year:
                end_dt = datetime(end_year, end_month, end_day)
                return pytz.utc.localize(start_dt), pytz.utc.localize(end_dt)

            return pytz.utc.localize(start_dt), None
        return (None, None)

    def _create_hazard_detail(self, item: Item, row: EmdatDataValidator) -> HazardDetail:
        """Create hazard detail from row data"""
        # First map EM-DAT classification to UNDRR-ISC codes

        return HazardDetail(
            cluster=self.hazard_profiles.get_cluster_code(item),
            severity_value=row.magnitude,
            severity_unit=row.magnitude_scale or "emdat",
            estimate_type=MontyEstimateType.PRIMARY,
        )

    # FIXME: Do we need to use pandas for this?
    def _get_row_by_disno(self, disno: str) -> Optional[pd.Series]:
        """Get original DataFrame row by DisNo"""
        df = self.data_source.get_data()
        matching_rows = df[df.disno == disno]
        return matching_rows.iloc[0] if not matching_rows.empty else None

    def _create_title_from_row(self, row: EmdatDataValidator) -> Optional[str]:
        """Create a descriptive title from row data when Event Name is missing"""
        if not row.name:
            return f"{row.subtype} in {row.country}" if row.subtype else "N/A"

        components = []

        # Add disaster type
        if row.type:
            components.append(row.type)
            if row.subtype and row.type != row.subtype:
                components.append(f"({row.subtype})")

        # Add location info
        locations = []
        if row.country:
            locations.append(row.country)
        if locations:
            components.append("in")
            components.append(", ".join(locations))

        # Add date
        date_str = None
        if row.start_year:
            date_components = []
            # Add month if available
            if row.start_year:
                try:
                    month_name = datetime(2000, row.start_month or 1, 1).strftime("%B")
                    date_components.append(month_name)
                except ValueError:
                    pass
            # Add year
            date_components.append(str(row.start_year))
            if date_components:
                date_str = " ".join(date_components)

        if date_str:
            components.extend(["of", date_str])

        return " ".join(components) if components else None

    def _create_description_from_row(self, row: EmdatDataValidator) -> str:
        """Create a description from row data"""
        components = []

        # Add disaster type
        if row.type:
            components.append(row.type)
            if not row.subtype and row.type != row.subtype:
                components.append(f"({row.subtype})")

        # Add location info
        locations = []
        if row.location:
            locations.append(row.location)
        if row.country:
            locations.append(row.country)
        if locations:
            components.append("in")
            components.append(", ".join(locations))

        # Add date
        date_str = None
        if row.start_year:
            date_components = []
            # Add month if available
            if row.start_month:
                try:
                    month_name = datetime(2000, row.start_month, 1).strftime("%B")
                    date_components.append(month_name)
                except ValueError:
                    pass
            # Add year
            date_components.append(str(int(row.start_year)))
            if date_components:
                date_str = " ".join(date_components)

        if date_str:
            components.extend(["of", date_str])

        return " ".join(components) if components else "Unnamed Event"
