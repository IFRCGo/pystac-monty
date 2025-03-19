"""IBTrACS data transformer for STAC Items."""

import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple

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
            data: Tropical cyclone track data as JSON string
        """
        super().__init__(source_url, data)
        self.data = json.loads(data) if isinstance(data, str) else data

    def get_data(self) -> dict:
        """Get the tropical cyclone track data."""
        return self.data


class IBTrACSTransformer(MontyDataTransformer):
    """Transforms IBTrACS tropical cyclone data into STAC Items."""

    hazard_profiles = MontyHazardProfiles()

    def __init__(self, data: IBTrACSDataSource, geocoder: MontyGeoCoder) -> None:
        """Initialize IBTrACS transformer.

        Args:
            data: IBTrACSDataSource containing tropical cyclone track data
            geocoder: MontyGeoCoder for determining affected countries
        """
        super().__init__("ibtracs")
        self.data = data
        self.geocoder = geocoder
        if not self.geocoder:
            raise ValueError("Geocoder is required for IBTrACS transformer")

    def make_items(self) -> List[Item]:
        """Create STAC items from IBTrACS data."""
        items = []

        # Create event item (represents the entire storm lifecycle)
        event_item = self.make_source_event_item()
        items.append(event_item)

        # Create hazard items (one for each position in the track)
        hazard_items = self.make_hazard_items()
        items.extend(hazard_items)

        return items

    def make_source_event_item(self) -> Item:
        """Create source event item from IBTrACS data."""
        track_data = self.data.get_data()
        
        # Extract storm ID and name
        storm_id = track_data.get("sid", "")
        storm_name = track_data.get("name", "")
        
        # Extract track coordinates
        coordinates = track_data.get("track", [])
        if not coordinates:
            raise ValueError("No track coordinates found in IBTrACS data")
        
        # Create LineString geometry from all track positions
        line_string = LineString(coordinates)
        
        # Get time information
        times = track_data.get("times", [])
        if not times:
            raise ValueError("No time information found in IBTrACS data")
        
        start_time = datetime.fromisoformat(times[0].replace("Z", "+00:00"))
        end_time = datetime.fromisoformat(times[-1].replace("Z", "+00:00"))
        
        # Get maximum intensity information
        max_wind = max(track_data.get("wind_speed", [0]))
        min_pressure = min(filter(lambda p: p > 0, track_data.get("pressure", [1013])))
        
        # Get basin information
        basin = track_data.get("basin", "")
        
        # Create item
        item = Item(
            id=storm_id,
            geometry=mapping(line_string),
            bbox=line_string.bounds,
            datetime=start_time,
            properties={
                "title": f"Tropical Cyclone {storm_name}",
                "description": f"Tropical Cyclone {storm_name} ({start_time.year}) in the {self._get_basin_name(basin)} basin. "
                               f"Maximum intensity: {self._get_storm_category(max_wind)} with {max_wind} knots winds "
                               f"and minimum pressure of {min_pressure} mb.",
                "start_datetime": start_time.isoformat() + "Z",
                "end_datetime": end_time.isoformat() + "Z",
            },
        )

        # Add Monty extension
        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.hazard_codes = ["MH0057"]  # Tropical cyclone code
        
        # Determine affected countries
        country_codes = self._get_affected_countries(coordinates)
        # For tropical cyclones initiated in international waters, use XYZ as the first country code
        if not country_codes:
            country_codes = ["XYZ"]
        monty.country_codes = country_codes
        
        # Compute correlation ID
        # Format: [datetime]-[country]-[hazard type]-[sequence]-[source]
        # Example: 20240626T000000-XYZ-NAT-MET-STO-TRO-001-GCDB
        start_datetime_str = start_time.strftime("%Y%m%dT%H%M%S")
        first_country = country_codes[0]
        monty.corr_id = f"{start_datetime_str}-{first_country}-NAT-MET-STO-TRO-001-GCDB"
        
        # Set collection and roles
        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["event", "source"]
        
        # Add keywords
        item.properties["keywords"] = [
            "tropical cyclone",
            storm_name,
            str(start_time.year),
            self._get_basin_name(basin)
        ]
        
        # Add storm category to keywords if applicable
        category = self._get_storm_category(max_wind)
        if "category" in category.lower():
            item.properties["keywords"].append(category.lower())
        elif "hurricane" in category.lower() or "typhoon" in category.lower() or "cyclone" in category.lower():
            item.properties["keywords"].append(category.lower())
        
        # Add source link and assets
        item.add_link(Link("via", self.data.get_source_url(), "application/json", "IBTrACS Data"))
        
        # Add assets based on basin
        basin_code = basin.upper() if basin else "NA"
        item.add_asset(
            "data",
            Asset(
                href=f"https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r00/access/csv/ibtracs.{basin_code}.list.v04r00.csv",
                media_type="text/csv",
                title=f"IBTrACS {self._get_basin_name(basin)} Basin Data",
                roles=["data"],
            ),
        )
        
        item.add_asset(
            "documentation",
            Asset(
                href="https://www.ncei.noaa.gov/products/international-best-track-archive",
                media_type="text/html",
                title="IBTrACS Documentation",
                roles=["documentation"],
            ),
        )

        return item

    def make_hazard_items(self) -> List[Item]:
        """Create hazard items from IBTrACS data, one for each position in the track."""
        track_data = self.data.get_data()
        
        # Extract storm ID and name
        storm_id = track_data.get("sid", "")
        storm_name = track_data.get("name", "")
        
        # Extract track coordinates, times, wind speeds, and pressures
        coordinates = track_data.get("track", [])
        times = track_data.get("times", [])
        wind_speeds = track_data.get("wind_speed", [])
        pressures = track_data.get("pressure", [])
        
        if not coordinates or not times:
            raise ValueError("Missing track coordinates or times in IBTrACS data")
        
        # Ensure all lists have the same length by padding with None if necessary
        max_length = max(len(coordinates), len(times), len(wind_speeds), len(pressures))
        coordinates = coordinates + [None] * (max_length - len(coordinates))
        times = times + [None] * (max_length - len(times))
        wind_speeds = wind_speeds + [None] * (max_length - len(wind_speeds))
        pressures = pressures + [None] * (max_length - len(pressures))
        
        # Get basin information
        basin = track_data.get("basin", "")
        
        hazard_items = []
        
        # Create a hazard item for each position
        for i, (coord, time_str, wind, pressure) in enumerate(zip(coordinates, times, wind_speeds, pressures)):
            if coord is None or time_str is None:
                continue
                
            # Parse time
            time = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            
            # Create track up to this point
            track_so_far = coordinates[:i+1]
            line_string = LineString(track_so_far)
            
            # Create hazard item ID with timestamp
            timestamp = time.strftime("%Y%m%dT%H%M%SZ")
            hazard_id = f"{storm_id}-hazard-{timestamp}"
            
            # Determine storm status based on wind speed
            storm_status = self._get_storm_category(wind)
            
            # Create item
            item = Item(
                id=hazard_id,
                geometry=mapping(line_string),
                bbox=line_string.bounds,
                datetime=time,
                properties={
                    "title": f"Tropical Cyclone {storm_name} - {storm_status}",
                    "description": f"Tropical Cyclone {storm_name} ({time.year}) in the {self._get_basin_name(basin)} basin "
                                   f"at {time.isoformat()}Z. Current status: {storm_status} with {wind} knots "
                                   f"wind speed and {pressure} mb pressure.",
                    "start_datetime": track_data.get("times", [time_str])[0],
                    "end_datetime": time_str,
                },
            )
            
            # Add Monty extension
            MontyExtension.add_to(item)
            monty = MontyExtension.ext(item)
            monty.hazard_codes = ["MH0057"]  # Tropical cyclone code
            
            # Determine affected countries up to this point
            country_codes = self._get_affected_countries(track_so_far)
            if not country_codes:
                country_codes = ["XYZ"]  # International waters
            monty.country_codes = country_codes
            
            # Add hazard detail
            monty.hazard_detail = HazardDetail(
                cluster="nat-met-sto-tro",
                severity_value=float(wind) if wind is not None else 0,
                severity_unit="knots",
                pressure=float(pressure) if pressure is not None else 1013,
                pressure_unit="mb",
                estimate_type=MontyEstimateType.PRIMARY,
            )
            
            # Use same correlation ID as the event
            start_time = datetime.fromisoformat(track_data.get("times", [time_str])[0].replace("Z", "+00:00"))
            start_datetime_str = start_time.strftime("%Y%m%dT%H%M%S")
            first_country = country_codes[0]
            monty.corr_id = f"{start_datetime_str}-{first_country}-NAT-MET-STO-TRO-001-GCDB"
            
            # Set collection and roles
            item.set_collection(self.get_hazard_collection())
            item.properties["roles"] = ["hazard", "source"]
            
            # Add keywords
            keywords = [
                "tropical cyclone",
                storm_name,
                str(time.year),
                self._get_basin_name(basin),
            ]
            
            # Add storm category to keywords
            if "category" in storm_status.lower():
                keywords.append(storm_status.lower())
            elif "hurricane" in storm_status.lower() or "typhoon" in storm_status.lower() or "cyclone" in storm_status.lower():
                keywords.append(storm_status.lower())
            
            # Add regional keywords based on affected countries
            if any(c in country_codes for c in ["USA", "MEX", "CUB", "JAM", "HTI", "DOM"]):
                keywords.append("Caribbean")
            if any(c in country_codes for c in ["JPN", "PHL", "CHN", "TWN", "VNM", "KOR"]):
                keywords.append("Western Pacific")
            if any(c in country_codes for c in ["AUS", "FJI", "VUT", "NCL"]):
                keywords.append("South Pacific")
            if any(c in country_codes for c in ["IND", "BGD", "MMR", "THA", "LKA"]):
                keywords.append("Indian Ocean")
            
            item.properties["keywords"] = keywords
            
            # Add source link and assets
            item.add_link(Link("via", self.data.get_source_url(), "application/json", "IBTrACS Data"))
            
            # Add link to event item
            item.add_link(
                Link(
                    "related",
                    f"../ibtracs-events/{storm_id}.json",
                    "application/json",
                    title="Related Event",
                    roles=["event", "source"],
                )
            )
            
            # Add assets
            basin_code = basin.upper() if basin else "NA"
            item.add_asset(
                "data",
                Asset(
                    href=f"https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r00/access/csv/ibtracs.{basin_code}.list.v04r00.csv",
                    media_type="text/csv",
                    title=f"IBTrACS {self._get_basin_name(basin)} Basin Data",
                    roles=["data"],
                ),
            )
            
            item.add_asset(
                "documentation",
                Asset(
                    href="https://www.ncei.noaa.gov/products/international-best-track-archive",
                    media_type="text/html",
                    title="IBTrACS Documentation",
                    roles=["documentation"],
                ),
            )
            
            hazard_items.append(item)
        
        return hazard_items

    def _get_affected_countries(self, coordinates: List[List[float]]) -> List[str]:
        """Determine countries affected by the storm track."""
        countries = set()
        
        for coord in coordinates:
            point = Point(coord)
            country_code = self.geocoder.get_iso3_from_geometry(point)
            if country_code and country_code != "":
                countries.add(country_code)
        
        return list(countries)

    def _get_basin_name(self, basin_code: str) -> str:
        """Convert basin code to full basin name."""
        basin_names = {
            "NA": "North Atlantic",
            "SA": "South Atlantic",
            "EP": "Eastern North Pacific",
            "WP": "Western North Pacific",
            "SP": "South Pacific",
            "SI": "South Indian",
            "NI": "North Indian",
            "": "Unknown"
        }
        return basin_names.get(basin_code.upper(), "Unknown")

    def _get_storm_category(self, wind_speed: Optional[float]) -> str:
        """Determine storm category based on wind speed in knots."""
        if wind_speed is None:
            return "Unknown"
            
        # Saffir-Simpson Hurricane Wind Scale (for Atlantic and Eastern Pacific)
        if wind_speed < 34:
            return "Tropical Depression"
        elif wind_speed < 64:
            return "Tropical Storm"
        elif wind_speed < 83:
            return "Category 1 Hurricane"
        elif wind_speed < 96:
            return "Category 2 Hurricane"
        elif wind_speed < 113:
            return "Category 3 Hurricane"
        elif wind_speed < 137:
            return "Category 4 Hurricane"
        else:
            return "Category 5 Hurricane"
