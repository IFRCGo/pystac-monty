"""IBTrACS data transformer for STAC Items."""

import csv
import io
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import pytz
from pystac import Asset, Item, Link
from shapely.geometry import LineString, Point, mapping, shape

from pystac_monty.extension import (
    HazardDetail,
    MontyEstimateType,
    MontyExtension,
)
from pystac_monty.geocoding import MontyGeoCoder
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import MontyDataSource, MontyDataTransformer

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
            # Skip header row or rows with empty SID
            if not row.get('SID') or row.get('SID') == ' ':
                continue
            csv_data.append(row)
        return csv_data
    
    def get_storm_ids(self) -> List[str]:
        """Get a list of unique storm IDs from the data."""
        data = self.get_data()
        return list(set(row.get('SID', '').strip() for row in data if row.get('SID')))
    
    def get_storm_data(self, storm_id: str) -> List[Dict[str, str]]:
        """Get all data rows for a specific storm ID."""
        data = self.get_data()
        return [row for row in data if row.get('SID', '').strip() == storm_id]


class IBTrACSTransformer(MontyDataTransformer):
    """Transforms IBTrACS tropical cyclone data into STAC Items."""
    
    ibtracs_events_collection_id = "ibtracs-events"
    ibtracs_events_collection_url = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/examples/ibtracs-events/ibtracs-events.json"  # noqa

    ibtracs_hazards_collection_id = "ibtracs-hazards"
    ibtracs_hazards_collection_url = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/examples/ibtracs-hazards/ibtracs-hazards.json"  # noqa

    hazard_profiles = MontyHazardProfiles()

    def __init__(self, data_source: IBTrACSDataSource, geocoder: MontyGeoCoder):
        """Initialize IBTrACS transformer.

        Args:
            data_source: IBTrACS data source
            geocoder: Geocoder for determining affected countries
        """
        super().__init__("ibtracs")
        self.events_collection_id = self.ibtracs_events_collection_id
        self.events_collection_url = self.ibtracs_events_collection_url
        self.hazards_collection_id = self.ibtracs_hazards_collection_id
        self.hazards_collection_url = self.ibtracs_hazards_collection_url
        
        if geocoder is None:
            raise ValueError("Geocoder is required for IBTrACS transformer")
        
        self.data_source = data_source
        self.geocoder = geocoder
    
    def make_items(self) -> List[Item]:
        """Create STAC Items from IBTrACS data.
        
        Returns:
            List of STAC Items (events and hazards)
        """
        items = []
        
        # Create event items (one per storm)
        event_items = self.make_source_event_items()
        items.extend(event_items)
        
        # Create hazard items (one per position)
        hazard_items = self.make_hazard_items(event_items)
        items.extend(hazard_items)
        
        return items
    
    def make_source_event_items(self) -> List[Item]:
        """Create source event items from IBTrACS data.
        
        Returns:
            List of event STAC Items
        """
        event_items = []
        
        # Get unique storm IDs
        storm_ids = self.data_source.get_storm_ids()
        
        for storm_id in storm_ids:
            # Get all data for this storm
            storm_data = self.data_source.get_storm_data(storm_id)
            
            if not storm_data:
                continue
            
            # Create track geometry from all positions
            track_coords = []
            for row in storm_data:
                try:
                    lat = float(row.get('LAT', 0))
                    lon = float(row.get('LON', 0))
                    track_coords.append((lon, lat))
                except (ValueError, TypeError):
                    # Skip invalid coordinates
                    continue
            
            if not track_coords:
                continue
            
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
            name = storm_data[0].get('NAME', '').strip()
            basin = storm_data[0].get('BASIN', '').strip()
            season = storm_data[0].get('SEASON', '').strip()
            
            # Get storm dates
            start_time = None
            end_time = None
            
            for row in storm_data:
                iso_time = row.get('ISO_TIME', '')
                if iso_time:
                    dt = datetime.strptime(iso_time, '%Y-%m-%d %H:%M:%S')
                    dt = pytz.utc.localize(dt) if dt.tzinfo is None else dt
                    
                    if start_time is None or dt < start_time:
                        start_time = dt
                    
                    if end_time is None or dt > end_time:
                        end_time = dt
            
            if start_time is None or end_time is None:
                continue
            
            # Find maximum intensity
            max_wind = 0
            min_pressure = 9999
            
            for row in storm_data:
                # Try to get wind speed from USA_WIND or WMO_WIND
                try:
                    wind = float(row.get('USA_WIND', 0))
                except (ValueError, TypeError):
                    try:
                        wind = float(row.get('WMO_WIND', 0))
                    except (ValueError, TypeError):
                        wind = 0
                
                # Try to get pressure from USA_PRES or WMO_PRES
                try:
                    pressure = float(row.get('USA_PRES', 9999))
                except (ValueError, TypeError):
                    try:
                        pressure = float(row.get('WMO_PRES', 9999))
                    except (ValueError, TypeError):
                        pressure = 9999
                
                max_wind = max(max_wind, wind)
                min_pressure = min(min_pressure, pressure)
            
            # Determine storm category based on Saffir-Simpson scale
            category = ""
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
            mph = int(max_wind * 1.15078)
            
            # Create title and description
            title = f"Tropical Cyclone {name}" if name else f"Unnamed Tropical Cyclone {storm_id}"
            description = f"Tropical Cyclone {name} ({season}) in the {self._get_basin_name(basin)} basin. "
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
                    "roles": ["source", "event"]
                }
            )
            
            # Set collection
            item.collection = self.get_event_collection()
            
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
            country_code = "XYZ"  # Default for international waters
            if countries and countries[0] != "XYZ":
                country_code = countries[0]
            
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
                    extra_fields={"roles": ["data"]}
                )
            )
            
            # Add documentation asset
            item.add_asset(
                "documentation",
                Asset(
                    href="https://www.ncei.noaa.gov/products/international-best-track-archive",
                    title="IBTrACS Documentation",
                    media_type="text/html",
                    extra_fields={"roles": ["documentation"]}
                )
            )
            
            event_items.append(item)
        
        return event_items
    
    def make_hazard_items(self, event_items: List[Item]) -> List[Item]:
        """Create hazard items from IBTrACS data.
        
        Args:
            event_items: List of event STAC Items
            
        Returns:
            List of hazard STAC Items
        """
        hazard_items = []
        
        for event_item in event_items:
            storm_id = event_item.id
            storm_data = self.data_source.get_storm_data(storm_id)
            
            if not storm_data:
                continue
            
            # Sort storm data by time
            storm_data.sort(key=lambda x: x.get('ISO_TIME', ''))
            
            # Create a hazard item for each position
            track_coords = []
            
            for i, row in enumerate(storm_data):
                try:
                    lat = float(row.get('LAT', 0))
                    lon = float(row.get('LON', 0))
                    track_coords.append((lon, lat))
                except (ValueError, TypeError):
                    # Skip invalid coordinates
                    continue
                
                # Get position time
                iso_time = row.get('ISO_TIME', '')
                if not iso_time:
                    continue
                
                dt = datetime.strptime(iso_time, '%Y-%m-%d %H:%M:%S')
                dt = pytz.utc.localize(dt) if dt.tzinfo is None else dt
                
                # Format timestamp for ID
                timestamp = dt.strftime("%Y%m%dT%H%M%SZ")
                
                # Create geometry (Point for first position, LineString for subsequent positions)
                if i == 0:
                    geometry = mapping(Point(lon, lat))
                    bbox = [lon, lat, lon, lat]
                else:
                    # Create LineString with all positions up to this point
                    line_geometry = LineString(track_coords[:i+1])
                    geometry = mapping(line_geometry)
                    
                    # Calculate bounding box
                    min_lon = min(coord[0] for coord in track_coords[:i+1])
                    min_lat = min(coord[1] for coord in track_coords[:i+1])
                    max_lon = max(coord[0] for coord in track_coords[:i+1])
                    max_lat = max(coord[1] for coord in track_coords[:i+1])
                    bbox = [min_lon, min_lat, max_lon, max_lat]
                
                # Get storm metadata
                name = row.get('NAME', '').strip()
                basin = row.get('BASIN', '').strip()
                season = row.get('SEASON', '').strip()
                
                # Get wind and pressure data
                try:
                    wind = float(row.get('USA_WIND', 0))
                except (ValueError, TypeError):
                    try:
                        wind = float(row.get('WMO_WIND', 0))
                    except (ValueError, TypeError):
                        wind = 0
                
                try:
                    pressure = float(row.get('USA_PRES', 0))
                except (ValueError, TypeError):
                    try:
                        pressure = float(row.get('WMO_PRES', 0))
                    except (ValueError, TypeError):
                        pressure = 0
                
                # Determine storm status
                status = row.get('USA_STATUS', '').strip()
                if status == 'HU':
                    status_text = "Hurricane"
                elif status == 'TS':
                    status_text = "Tropical Storm"
                elif status == 'TD':
                    status_text = "Tropical Depression"
                else:
                    status_text = "Tropical Cyclone"
                
                # Create title and description
                if i == 0:
                    title = f"Tropical Cyclone {name} - Initial Position" if name else f"Unnamed Tropical Cyclone {storm_id} - Initial Position"
                    description = f"Initial position of Tropical Cyclone {name} ({season}) in the {self._get_basin_name(basin)} basin. "
                else:
                    title = f"Tropical Cyclone {name}" if name else f"Unnamed Tropical Cyclone {storm_id}"
                    description = f"Tropical Cyclone {name} ({season}) in the {self._get_basin_name(basin)} basin. "
                
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
                        "roles": ["source", "hazard"]
                    }
                )
                
                # Set collection
                item.collection = self.get_hazard_collection()
                
                # Add Monty extension
                MontyExtension.add_to(item)
                monty_ext = MontyExtension.ext(item)
                
                # Set hazard codes
                monty_ext.hazard_codes = ["MH0057", "nat-met-sto-tro", "TC"]
                
                # Determine affected countries for the track up to this point
                if i == 0:
                    # For the first position, there may not be any affected countries yet
                    countries = []
                else:
                    # For subsequent positions, get countries from the track so far
                    track_so_far = LineString(track_coords[:i+1])
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
                    pressure_unit="mb" if pressure > 0 else None
                )
                
                monty_ext.hazard_detail = hazard_detail
                
                # Add keywords (same as event)
                keywords = ["tropical cyclone"]
                if status == 'HU':
                    keywords.append("hurricane")
                elif status == 'TS':
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
                        extra_fields={"roles": ["data"]}
                    )
                )
                
                # Add documentation asset
                item.add_asset(
                    "documentation",
                    Asset(
                        href="https://www.ncei.noaa.gov/products/international-best-track-archive",
                        title="IBTrACS Documentation",
                        media_type="text/html",
                        extra_fields={"roles": ["documentation"]}
                    )
                )
                
                # Add link to related event
                item.add_link(
                    Link(
                        rel="related",
                        target=f"../ibtracs-events/{storm_id}.json",
                        media_type="application/json",
                        extra_fields={"roles": ["event", "source"]}
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
            'NA': 'North Atlantic',
            'SA': 'South Atlantic',
            'EP': 'Eastern North Pacific',
            'WP': 'Western North Pacific',
            'SP': 'South Pacific',
            'SI': 'South Indian',
            'NI': 'North Indian',
            'AS': 'Arabian Sea',
            'BB': 'Bay of Bengal',
            'CP': 'Central Pacific',
            'CS': 'Caribbean Sea',
            'GM': 'Gulf of Mexico',
            'IO': 'Indian Ocean',
            'LS': 'Labrador Sea',
            'MM': 'Mediterranean',
            'SL': 'Sulu Sea'
        }
        
        return basin_names.get(basin_code, 'Unknown Basin')
    
    def _get_countries_from_track(self, track_geometry: Union[LineString, Point]) -> List[str]:
        """Get a list of countries affected by a storm track.
        
        Args:
            track_geometry: Shapely geometry of the storm track
            
        Returns:
            List of ISO3 country codes
        """
        if self.geocoder is None:
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
                        countries.extend(country_code)
            # For Point, check the single point
            elif isinstance(track_geometry, Point):
                lon, lat = track_geometry.x, track_geometry.y
                country_code = self.geocoder.get_iso3_from_geometry(track_geometry)
                if country_code:
                    countries.extend(country_code)
        except Exception as e:
            # If geocoding fails, default to international waters
            print(f"Geocoding error: {e}")
            return ["XYZ"]
        
        # Remove duplicates and sort
        countries = list(dict.fromkeys(countries))
        
        # If no countries found, use XYZ for international waters
        if not countries:
            countries = ["XYZ"]
        
        return countries
