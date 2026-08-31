"""IBTrACS data transformer for STAC Items."""

import csv
import io
import itertools
import logging
import typing
from dataclasses import dataclass
from typing import Dict, List, Union

import pandas as pd
import pytz
from pystac import Asset, Item, Link
from shapely.geometry import LineString, Point, mapping

from pystac_monty.extension import HazardDetail, MontyEstimateType, MontyExtension
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import DataType, File, GenericDataSource, Memory, MontyDataSourceV3, MontyDataTransformer
from pystac_monty.validators.ibtracs import IBTracsdataValidator

logger = logging.getLogger(__name__)


STAC_EVENT_ID_PREFIX = "ibtracs-event-"
STAC_HAZARD_ID_PREFIX = "ibtracs-hazard-"


@dataclass
class IBTrACSDataSource(MontyDataSourceV3):
    """IBTrACS data source that handles tropical cyclone track data."""

    source_url: str
    data_source: Union[File, Memory]

    def __init__(self, data: GenericDataSource, eoapi_url: str | None = None, version: str = "v04r01"):
        """Initialize IBTrACS data source.

        Args:
            source_url: URL where the data was retrieved from
            data: Tropical cyclone track data as CSV string
        """
        super().__init__(root=data, eoapi_url=eoapi_url)
        self.version = version

        def handle_file_data():
            df = pd.read_csv(self.input_data.path)
            df = df.sort_values(by=["SID", "ISO_TIME"])
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            self.file_content = buffer.getvalue()

        def handle_memory_data(): ...

        self.input_data_type = self.input_data.data_type
        match self.input_data_type:
            case DataType.FILE:
                handle_file_data()
            case DataType.MEMORY:
                handle_memory_data()
            case _:
                typing.assert_never(self.input_data_type)

    def _parse_csv(self) -> List[Dict[str, str]]:
        """Parse the CSV data into a list of dictionaries."""
        csv_data = []
        csv_reader = csv.DictReader(io.StringIO(self.input_data.content))
        for row in csv_reader:
            csv_data.append(row)
        return csv_data

    def get_data_for_file(self):
        """Yield storm data grouped by SID from a sorted CSV."""

        reader = csv.DictReader(io.StringIO(self.file_content))
        current_storm_id = None
        storm_data = []

        for row in reader:
            storm_id = row["SID"]
            if storm_id != current_storm_id:
                if storm_data:
                    yield storm_data
                storm_data = [row]
                current_storm_id = storm_id
            else:
                storm_data.append(row)
        if storm_data:
            yield storm_data

    def get_data_for_memory(self):
        parsed_data = self._parse_csv()
        parsed_data = [x for x in parsed_data if "SID" in x and "ISO_TIME" in x]
        parsed_data.sort(key=lambda x: (x.get("SID", " "), x.get("ISO_TIME", " ")))
        yield from [list(group) for _, group in itertools.groupby(parsed_data, key=lambda x: x.get("SID", " "))]

    def get_input_data_type(self) -> DataType:
        """Get the input data type"""
        return self.input_data.data_type


class IBTrACSTransformer(MontyDataTransformer[IBTrACSDataSource]):
    """Transforms IBTrACS tropical cyclone data into STAC Items."""

    hazard_profiles = MontyHazardProfiles()
    source_name = "ibtracs"

    @staticmethod
    def _get_wind_and_pressure(row: IBTracsdataValidator, pressure_default: float = 0) -> typing.Tuple[float, float]:
        """Get wind speed (knots) and pressure (mb), preferring USA_* then falling back to WMO_*."""
        try:
            wind = float(row.USA_WIND or 0)
        except (ValueError, TypeError):
            try:
                wind = float(row.WMO_WIND or 0)
            except (ValueError, TypeError):
                wind = 0

        try:
            pressure = float(row.USA_PRES or pressure_default)
        except (ValueError, TypeError):
            try:
                pressure = float(row.WMO_PRES or pressure_default)
            except (ValueError, TypeError):
                pressure = pressure_default

        return wind, pressure

    def _add_common_assets_and_links(self, item: Item, source_url: str) -> None:
        """Add the `via` link and data/documentation assets shared by event and hazard items."""
        item.add_link(Link("via", source_url, "text/csv"))

        item.add_asset(
            "data",
            Asset(
                href=source_url,
                title="IBTrACS Best Track Data",
                media_type="text/csv",
                extra_fields={"roles": ["data"]},
            ),
        )

        item.add_asset(
            "documentation",
            Asset(
                href="https://www.ncei.noaa.gov/products/international-best-track-archive",
                title="IBTrACS Documentation",
                media_type="text/html",
                extra_fields={"roles": ["documentation"]},
            ),
        )

    def get_stac_items(self) -> typing.Generator[Item, None, None]:
        self.transform_summary.mark_as_started()
        data_type = self.data_source.get_input_data_type()
        match data_type:
            case DataType.FILE:
                csv_data = self.data_source.get_data_for_file()
            case DataType.MEMORY:
                csv_data = self.data_source.get_data_for_memory()
            case _:
                typing.assert_never(data_type)

        for storm_data in csv_data:
            if storm_data[0].get("SID", "").strip() == "":
                logger.warning("SID is empty")
                continue

            self.transform_summary.increment_rows(len(storm_data))
            try:

                def parse_row_data(rows: list[dict]):
                    validated_data: list[IBTracsdataValidator] = []
                    for row in rows:
                        obj = IBTracsdataValidator(**row)
                        validated_data.append(obj)
                    return validated_data

                storm_data = parse_row_data(storm_data)
                if event_item := self.make_source_event_items(storm_data[0].SID, storm_data):
                    hazard_items = self.make_hazard_items(event_item, storm_data)

                    all_items = [event_item] + hazard_items
                    self.set_item_hrefs(items=all_items, eoapi_url=self.data_source.eoapi_url)
                    self.add_related_links(event_item=event_item, hazard_items=hazard_items)

                    yield event_item
                    yield from hazard_items
                else:
                    self.transform_summary.increment_failed_rows(len(storm_data))
            except Exception:
                self.transform_summary.increment_failed_rows(len(storm_data))
                logger.warning("Failed to process IBTrACS data", exc_info=True)
        self.transform_summary.mark_as_complete()

    # FIXME: This is deprecated
    def make_items(self):
        return list(self.get_stac_items())

    def make_source_event_items(self, storm_id: str, storm_data: list[IBTracsdataValidator]) -> Item | None:
        """Create source event items from IBTrACS data.

        Returns:
            List of event STAC Items
        """
        if not storm_data:
            # FIXME: Do we throw error?
            return None

        # Create track geometry from all positions
        track_coords: list[typing.Tuple[float, float]] = []
        for row in storm_data:
            lat = row.LAT or 0  # FIXME: Do we need these default values? Are these even correct?
            lon = row.LON or 0  # FIXME: Do we need these default values? Are these even correct?
            track_coords.append((lon, lat))

        if not track_coords:
            # FIXME: Do we throw error?
            return

        # Create LineString geometry for the complete track
        track_geometry = LineString(track_coords)
        geometry = mapping(track_geometry)

        # Calculate bounding box
        min_lon = min(coord[0] for coord in track_coords)
        min_lat = min(coord[1] for coord in track_coords)
        max_lon = max(coord[0] for coord in track_coords)
        max_lat = max(coord[1] for coord in track_coords)
        bbox = [min_lon, min_lat, max_lon, max_lat]

        # Get storm metadata
        name = (storm_data[0].NAME or "").strip()
        basin = (storm_data[0].BASIN or "").strip()
        season = storm_data[0].SEASON or ""

        # Get storm dates
        start_time = None
        end_time = None
        for row in storm_data:
            iso_time = row.ISO_TIME
            if iso_time:
                dt = iso_time
                # dt = datetime.strptime(iso_time, "%Y-%m-%d %H:%M:%S")
                dt = pytz.utc.localize(dt) if dt.tzinfo is None else dt

                if start_time is None or dt < start_time:
                    start_time = dt
                if end_time is None or dt > end_time:
                    end_time = dt

        if start_time is None or end_time is None:
            # FIXME: Do we throw error?
            return

        # Find maximum intensity
        max_wind = 0
        min_pressure = 9999

        for row in storm_data:
            wind, pressure = self._get_wind_and_pressure(row, pressure_default=9999)

            max_wind = max(max_wind, wind)
            min_pressure = min(min_pressure, pressure)

        # Determine storm category based on Saffir-Simpson scale
        if max_wind >= 137:  # Category 5
            category = "Category 5 hurricane"
        elif max_wind >= 113:  # Category 4
            category = "Category 4 hurricane"
        elif max_wind >= 96:  # Category 3
            category = "Category 3 hurricane"
        elif max_wind >= 83:  # Category 2
            category = "Category 2 hurricane"
        elif max_wind >= 64:  # Category 1
            category = "Category 1 hurricane"
        elif max_wind >= 34:  # Tropical Storm
            category = "tropical storm"
        else:  # Tropical Depression
            category = "tropical depression"

        # Convert knots to mph for description
        # FIXME: Why are we using int
        mph = int(max_wind * 1.15078)

        basin_name = self._get_basin_name(basin)

        # Create title and description
        title = f"Tropical Cyclone {name}" if name else f"Unnamed Tropical Cyclone {storm_id}"
        description = f"Tropical Cyclone {name} ({season}) in the {basin_name} basin. "
        description += f"Maximum intensity: {category} with {mph} mph ({max_wind} knots) winds"

        if min_pressure < 9999:
            description += f" and minimum pressure of {min_pressure} mb."
        else:
            description += "."

        # Create event item
        item = Item(
            id=storm_id,
            geometry=geometry,
            bbox=bbox,
            datetime=start_time,
            properties={
                "title": title,
                "description": description,
                "start_datetime": start_time.isoformat(),
                "end_datetime": end_time.isoformat(),
                "roles": ["source", "event"],
            },
        )

        # Set collection
        item.set_collection(self.get_event_collection())

        # Add Monty extension
        MontyExtension.add_to(item)
        monty_ext = MontyExtension.ext(item)
        # Set hazard codes
        monty_ext.hazard_codes = ["MH0309", "nat-met-sto-tro", "TC"]
        monty_ext.hazard_codes = self.hazard_profiles.get_canonical_hazard_codes(item=item)

        # Determine affected countries
        countries = self._get_countries_from_track(track_geometry)
        monty_ext.country_codes = countries or ["XYZ"]  # Default for international waters

        hazard_keywords = self.hazard_profiles.get_keywords(monty_ext.hazard_codes)
        item.properties["keywords"] = list(set(hazard_keywords + countries))
        monty_ext.src_event_id = storm_id
        monty_ext.episode_number = 1
        monty_ext.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        # Add keywords
        keywords = ["tropical cyclone"]
        if category.startswith("Category"):
            keywords.append("hurricane")
        elif "tropical storm" in category:
            keywords.append("tropical storm")
        else:
            keywords.append("tropical depression")

        if name:
            keywords.append(name)

        keywords.append(season)
        keywords.append(self._get_basin_name(basin))

        item.properties["keywords"] = keywords

        # Add links and assets
        source_url = self.data_source.get_source_url()
        self._add_common_assets_and_links(item, source_url)

        # Add track plot asset
        item.add_asset(
            "thumbnail",
            Asset(
                href=f"https://ncics.org/ibtracs/html/plots/{self.data_source.version}.{storm_id}.png",
                title="IBTrACS Track Plot",
                media_type="image/png",
                extra_fields={"roles": ["track-plot"]},
            ),
        )

        return item

    def make_hazard_items(self, event_item: Item, storm_data: list[IBTracsdataValidator]) -> list[Item]:
        """Create hazard items from IBTrACS data.

        Args:
            event_items: List of event STAC Items

        Returns:
            List of hazard STAC Items
        """
        hazard_items = []

        storm_id = event_item.id

        if not storm_data:
            return []

        # Sort storm data by time

        source_url = self.data_source.get_source_url()

        # Create a hazard item for each position
        track_coords = []

        # Countries affected by the track so far, accumulated incrementally so each
        # position only geocodes the points not already covered by earlier iterations.
        accumulated_countries: list[str] = []
        seen_countries: set[str] = set()
        geocoded_up_to = -1

        for i, row in enumerate(storm_data):
            lat = row.LAT or 0  # FIXME: Do we need these default values? Are these even correct?
            lon = row.LON or 0  # FIXME: Do we need these default values? Are these even correct?
            track_coords.append((lon, lat))

            # Get position time
            iso_time = row.ISO_TIME
            if not iso_time:
                logger.warning("Missing ISO_TIME for storm %s", storm_id)
                continue

            dt = iso_time
            dt = pytz.utc.localize(dt) if dt.tzinfo is None else dt

            # Format timestamp for ID
            timestamp = dt.strftime("%Y%m%dT%H%M%SZ")

            # Create geometry (Point for first position, LineString for subsequent positions)
            if i == 0:
                geometry = mapping(Point(lon, lat))
                bbox = [lon, lat, lon, lat]
            else:
                # Create LineString with all positions up to this point
                line_geometry = LineString(track_coords[: i + 1])
                geometry = mapping(line_geometry)

                # Calculate bounding box
                min_lon = min(coord[0] for coord in track_coords[: i + 1])
                min_lat = min(coord[1] for coord in track_coords[: i + 1])
                max_lon = max(coord[0] for coord in track_coords[: i + 1])
                max_lat = max(coord[1] for coord in track_coords[: i + 1])
                bbox = [min_lon, min_lat, max_lon, max_lat]

            # Get storm metadata
            name = row.NAME or ""
            basin = row.BASIN or ""
            season = row.SEASON or ""

            # Get wind and pressure data
            wind, pressure = self._get_wind_and_pressure(row)

            # Determine storm status
            status = row.USA_STATUS
            match status:
                case "HU":
                    status_text = "Hurricane"
                case "TS":
                    status_text = "Tropical Storm"
                case "TD":
                    status_text = "Tropical Depression"
                case _:
                    status_text = "Tropical Cyclone"

            basin_name = self._get_basin_name(basin)

            # Create title and description
            if i == 0:
                title = (
                    f"Tropical Cyclone {name} - Initial Position"
                    if name
                    else f"Unnamed Tropical Cyclone {storm_id} - Initial Position"
                )
                description = f"Initial position of Tropical Cyclone {name} ({season}) in the {basin_name} basin. "
            else:
                title = f"Tropical Cyclone {name}" if name else f"Unnamed Tropical Cyclone {storm_id}"
                description = f"Tropical Cyclone {name} ({season}) in the {basin_name} basin. "

            description += f"Current status: {status_text} with {int(wind)} knots wind speed."

            if pressure > 0:
                description += f" Pressure: {int(pressure)} mb."

            # Create hazard item ID
            hazard_id = f"{storm_id}-hazard-{timestamp}"

            # Create hazard item
            item = Item(
                id=hazard_id,
                geometry=geometry,
                bbox=bbox,
                datetime=dt,
                properties={
                    "title": title,
                    "description": description,
                    "start_datetime": event_item.properties["start_datetime"],
                    "end_datetime": dt.isoformat(),
                    "roles": ["source", "hazard"],
                },
            )

            # Set collection
            item.set_collection(self.get_hazard_collection())

            # Add Monty extension
            MontyExtension.add_to(item)
            monty_ext = MontyExtension.ext(item)

            monty_ext.src_event_id = event_item.properties["monty:src_event_id"]
            monty_ext.episode_number = 1

            # Set hazard codes
            monty_ext.hazard_codes = MontyExtension.ext(event_item).hazard_codes
            if monty_ext.hazard_codes and len(monty_ext.hazard_codes) >= 1:
                monty_ext.hazard_codes = [self.hazard_profiles.get_undrr_2025_code(hazard_codes=monty_ext.hazard_codes)]

            # Determine affected countries for the track up to this point
            if i == 0:
                # For the first position, there may not be any affected countries yet
                countries = []
            else:
                # Geocode only the points not yet covered by a previous iteration
                for j in range(geocoded_up_to + 1, i + 1):
                    country_code = self._get_country_for_point(*track_coords[j])
                    if country_code and country_code not in seen_countries:
                        seen_countries.add(country_code)
                        accumulated_countries.append(country_code)
                geocoded_up_to = i
                countries = list(accumulated_countries) if accumulated_countries else ["XYZ"]

            monty_ext.country_codes = countries

            # Set correlation ID (same as event)
            item.properties["monty:corr_id"] = event_item.properties.get("monty:corr_id")

            # Add hazard detail
            hazard_detail = HazardDetail(
                severity_value=int(wind),
                severity_unit="knots",
                estimate_type=MontyEstimateType.PRIMARY,
                pressure=int(pressure) if pressure > 0 else None,
                pressure_unit="mb" if pressure > 0 else None,
            )

            monty_ext.hazard_detail = hazard_detail

            # Add keywords (same as event)
            keywords = ["tropical cyclone"]
            if status == "HU":
                keywords.append("hurricane")
            elif status == "TS":
                keywords.append("tropical storm")
            else:
                keywords.append("tropical depression")

            if name:
                keywords.append(name)

            keywords.append(season)
            keywords.append(basin_name)

            item.properties["keywords"] = keywords

            # Add links and assets
            self._add_common_assets_and_links(item, source_url)

            hazard_items.append(item)

        return hazard_items

    def _get_basin_name(self, basin_code: str) -> str:
        """Get the full name of a basin from its code.

        Args:
            basin_code: Basin code (e.g., 'NA', 'EP', 'WP')

        Returns:
            Full basin name
        """
        basin_names = {
            "NA": "North Atlantic",
            "SA": "South Atlantic",
            "EP": "Eastern North Pacific",
            "WP": "Western North Pacific",
            "SP": "South Pacific",
            "SI": "South Indian",
            "NI": "North Indian",
            "AS": "Arabian Sea",
            "BB": "Bay of Bengal",
            "CP": "Central Pacific",
            "CS": "Caribbean Sea",
            "GM": "Gulf of Mexico",
            "IO": "Indian Ocean",
            "LS": "Labrador Sea",
            "MM": "Mediterranean",
            "SL": "Sulu Sea",
        }

        return basin_names.get(basin_code, "Unknown Basin")

    def _get_country_for_point(self, lon: float, lat: float) -> str | None:
        """Get the ISO3 country code for a single point, or None if unavailable/unresolvable."""
        if self.geocoder is None:
            return None

        try:
            return self.geocoder.get_iso3_from_point(Point(lon, lat))
        except Exception as e:
            logger.warning(f"Geocoding error: {e}", exc_info=True)
            return None

    def _get_countries_from_track(self, track_geometry: Union[LineString, Point]) -> List[str]:
        """Get a list of countries affected by a storm track.

        Args:
            track_geometry: Shapely geometry of the storm track

        Returns:
            List of ISO3 country codes
        """
        coords = track_geometry.coords if isinstance(track_geometry, LineString) else [(track_geometry.x, track_geometry.y)]

        countries = []
        seen = set()
        for lon, lat in coords:
            country_code = self._get_country_for_point(lon, lat)
            if country_code and country_code not in seen:
                seen.add(country_code)
                countries.append(country_code)

        # If no countries found, use XYZ for international waters
        # FIXME: Should we use ["UNK"] instead?
        return countries or ["XYZ"]
