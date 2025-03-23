"""IBTrACS data transformer for STAC Items."""

import csv
import io
import itertools
import logging
import typing
from typing import Dict, List, Union

import pytz
from pystac import Asset, Item, Link
from shapely.geometry import LineString, Point, mapping

from pystac_monty.extension import HazardDetail, MontyEstimateType, MontyExtension
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import MontyDataSource, MontyDataTransformer
from pystac_monty.validators.ibtracs import IBTracsdataValidator

logger = logging.getLogger(__name__)


STAC_EVENT_ID_PREFIX = "ibtracs-event-"
STAC_HAZARD_ID_PREFIX = "ibtracs-hazard-"


class IBTrACSDataSource(MontyDataSource):
    """IBTrACS data source that handles tropical cyclone track data."""

    def __init__(self, source_url: str, data: str):
        """Initialize IBTrACS data source.

        Args:
            source_url: URL where the data was retrieved from
            data: Tropical cyclone track data as CSV string
        """
        super().__init__(source_url, data)
        self.data = data
        self._parsed_data = None

    def get_data(self) -> List[Dict[str, str]]:
        """Get the tropical cyclone track data as a list of dictionaries."""
        if self._parsed_data is None:
            self._parsed_data = self._parse_csv()
        return self._parsed_data

    def _parse_csv(self) -> List[Dict[str, str]]:
        """Parse the CSV data into a list of dictionaries."""
        csv_data = []
        csv_reader = csv.DictReader(io.StringIO(self.data))
        for row in csv_reader:
            csv_data.append(row)
        return csv_data


class IBTrACSTransformer(MontyDataTransformer[IBTrACSDataSource]):
    """Transforms IBTrACS tropical cyclone data into STAC Items."""

    hazard_profiles = MontyHazardProfiles()
    source_name = 'ibtracs'

    def get_stac_items(self) -> typing.Generator[Item, None, None]:
        # # TODO: Use sax xml parser for memory efficient usage
        failed_items_count = 0
        total_items_count = 0

        csv_data = self.data_source._parse_csv()
        csv_data.sort(key=lambda x: x.get("SID", " "))
        for storm_id, storm_data_iterator in itertools.groupby(csv_data, key=lambda x: x.get("SID", " ")):
            storm_data = list(storm_data_iterator)
            total_items_count += len(storm_data)

            try:
                def parse_row_data(rows: list[dict]):
                    validated_data: list[IBTracsdataValidator] = []
                    for row in rows:
                        obj = IBTracsdataValidator(**row)
                        validated_data.append(obj)
                    return validated_data

                storm_data = parse_row_data(storm_data)
                if event_item := self.make_source_event_items(storm_id, storm_data):
                    yield event_item
                    yield from self.make_hazard_items(event_item, storm_data)
                else:
                    failed_items_count += len(storm_data)
            except Exception:
                failed_items_count += len(storm_data)
                logger.error("Failed to process ibtracs", exc_info=True)

        print(failed_items_count)

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
        name = (storm_data[0].NAME or '').strip()
        basin = (storm_data[0].BASIN or '').strip()
        season = storm_data[0].SEASON or ''

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
            # Try to get wind speed from USA_WIND or WMO_WIND
            # FIXME: Need to simplify this logic
            try:
                wind = float(row.USA_WIND or 0)
            except (ValueError, TypeError):
                try:
                    wind = float(row.WMO_WIND or 0)
                except (ValueError, TypeError):
                    wind = 0

            # Try to get pressure from USA_PRES or WMO_PRES
            # FIXME: Need to simplify this logic
            try:
                pressure = float(row.USA_PRES or 9999)
            except (ValueError, TypeError):
                try:
                    pressure = float(row.WMO_PRES or 9999)
                except (ValueError, TypeError):
                    pressure = 9999

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
        monty_ext.hazard_codes = ["MH0057", "nat-met-sto-tro", "TC"]

        # Determine affected countries
        countries = self._get_countries_from_track(track_geometry)
        monty_ext.country_codes = countries

        # Set correlation ID
        # Format: [datetime]-[country]-[hazard type]-[sequence]-[source]
        # Example: 20240626T000000-XYZ-NAT-MET-STO-TRO-001-GCDB
        start_date_str = start_time.strftime("%Y%m%dT%H%M%S")

        country_code: str | None = None
        if countries and countries[0]:
            country_code = countries[0]
        country_code = country_code or "XYZ"  # Default for international waters

        monty_ext.correlation_id = f"{start_date_str}-{country_code}-NAT-MET-STO-TRO-001-GCDB"

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
        item.add_link(Link("via", source_url, "text/csv"))

        # Add data asset
        item.add_asset(
            "data",
            Asset(
                href=source_url,
                title="IBTrACS North Atlantic Basin Data",
                media_type="text/csv",
                extra_fields={"roles": ["data"]},
            ),
        )

        # Add documentation asset
        item.add_asset(
            "documentation",
            Asset(
                href="https://www.ncei.noaa.gov/products/international-best-track-archive",
                title="IBTrACS Documentation",
                media_type="text/html",
                extra_fields={"roles": ["documentation"]},
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
        storm_data.sort(key=lambda x: x.ISO_TIME if x.ISO_TIME else "")

        # Create a hazard item for each position
        track_coords = []

        for i, row in enumerate(storm_data):
            lat = row.LAT or 0  # FIXME: Do we need these default values? Are these even correct?
            lon = row.LON or 0  # FIXME: Do we need these default values? Are these even correct?
            track_coords.append((lon, lat))

            # Get position time
            iso_time = row.ISO_TIME
            if not iso_time:
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
            try:
                wind = float(row.USA_WIND or 0)
            except (ValueError, TypeError):
                try:
                    wind = float(row.WMO_WIND or 0)
                except (ValueError, TypeError):
                    wind = 0

            try:
                pressure = float(row.USA_PRES or 0)
            except (ValueError, TypeError):
                try:
                    pressure = float(row.WMO_PRES or 0)
                except (ValueError, TypeError):
                    pressure = 0

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
                description = (
                    f"Initial position of Tropical Cyclone {name} ({season}) in the {basin_name} basin. "
                )
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

            # Set hazard codes
            monty_ext.hazard_codes = ["nat-met-sto-tro"]

            # Determine affected countries for the track up to this point
            if i == 0:
                # For the first position, there may not be any affected countries yet
                countries = []
            else:
                # For subsequent positions, get countries from the track so far
                track_so_far = LineString(track_coords[: i + 1])
                countries = self._get_countries_from_track(track_so_far)

            monty_ext.country_codes = countries

            # Set correlation ID (same as event)
            monty_ext.correlation_id = MontyExtension.ext(event_item).correlation_id

            # Add hazard detail
            hazard_detail = HazardDetail(
                cluster="nat-met-sto-tro",
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
            source_url = self.data_source.get_source_url()
            item.add_link(Link("via", source_url, "text/csv"))

            # Add data asset
            item.add_asset(
                "data",
                Asset(
                    href=source_url,
                    title="IBTrACS North Atlantic Basin Data",
                    media_type="text/csv",
                    extra_fields={"roles": ["data"]},
                ),
            )

            # Add documentation asset
            item.add_asset(
                "documentation",
                Asset(
                    href="https://www.ncei.noaa.gov/products/international-best-track-archive",
                    title="IBTrACS Documentation",
                    media_type="text/html",
                    extra_fields={"roles": ["documentation"]},
                ),
            )

            # Add link to related event
            item.add_link(
                Link(
                    rel="related",
                    target=f"../ibtracs-events/{storm_id}.json",
                    media_type="application/json",
                    extra_fields={"roles": ["event", "source"]},
                )
            )

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

    def _get_countries_from_track(self, track_geometry: Union[LineString, Point]) -> List[str]:
        """Get a list of countries affected by a storm track.

        Args:
            track_geometry: Shapely geometry of the storm track

        Returns:
            List of ISO3 country codes
        """
        if self.geocoder is None:
            # FIXME: Should we use ["UNK"] instead?
            return ["XYZ"]  # Default to international waters if no geocoder

        # Use the geocoder to find countries
        countries = []

        try:
            # For LineString, check each point
            if isinstance(track_geometry, LineString):
                for point in track_geometry.coords:
                    lon, lat = point
                    country_code = self.geocoder.get_iso3_from_geometry(Point(lon, lat))
                    if country_code:
                        countries.append(country_code)
            # For Point, check the single point
            elif isinstance(track_geometry, Point):
                lon, lat = track_geometry.x, track_geometry.y
                country_code = self.geocoder.get_iso3_from_geometry(track_geometry)
                if country_code:
                    countries.append(country_code)
        except Exception as e:
            # If geocoding fails, default to international waters
            logger.error(f"Geocoding error: {e}", exc_info=True)
            # FIXME: Should we use ["UNK"] instead?
            return ["XYZ"]

        # Remove duplicates and sort
        countries = list(dict.fromkeys(countries))

        # If no countries found, use XYZ for international waters
        if not countries:
            # FIXME: Should we use ["UNK"] instead?
            return ["XYZ"]

        return countries
