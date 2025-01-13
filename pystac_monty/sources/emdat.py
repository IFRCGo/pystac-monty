import json
import mimetypes
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import geopandas as gpd
import pandas as pd
import pytz
import requests
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from pystac import Asset, Collection, Item, Link
from shapely.geometry import MultiPoint, Point, mapping
from shapely.geometry.base import BaseGeometry

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
        else:
            raise ValueError("Data must be either Excel content (str) or pandas DataFrame")

    def get_data(self) -> pd.DataFrame:
        return self.df

class EMDATTransformer:
    """
    Transforms EM-DAT event data into STAC Items
    """
    emdat_events_collection_id = "emdat-events"
    emdat_events_collection_url = (
        "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/examples/emdat-events/emdat-events.json"
    )

    emdat_hazards_collection_id = "emdat-hazards"
    emdat_hazards_collection_url = (
        "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/examples/emdat-hazards/emdat-hazards.json"
    )

    emdat_impacts_collection_id = "emdat-impacts"
    emdat_impacts_collection_url = (
        "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/examples/emdat-impacts/emdat-impacts.json"
    )

    hazard_profiles = HazardProfiles()

    def __init__(self, data: EMDATDataSource, gaul_gpkg_path: str = None) -> None:
        """
        Initialize EMDATTransformer
        
        Args:
            data: EMDATDataSource containing the EM-DAT data
            gaul_gpkg_path: Path to the GAUL geopackage file for admin boundaries
        """
        self.data = data
        self.admin_gdf = None
        if gaul_gpkg_path:
            self.load_admin_boundaries(gaul_gpkg_path)
            
        # Initialize geocoder with rate limiting
        self.geolocator = Nominatim(user_agent="emdat_geocoder")
        self.geocode = RateLimiter(self.geolocator.geocode, min_delay_seconds=1)
        
        # Cache for geocoding results to avoid repeated API calls
        self.geocoding_cache = {}
            
    def load_admin_boundaries(self, gpkg_path: str) -> None:
        """
        Load administrative boundaries from GAUL geopackage
        
        Args:
            gpkg_path: Path to the GAUL geopackage file
        """
        try:
            # Load admin level 2 boundaries
            self.admin_gdf = gpd.read_file(gpkg_path, layer='level2')
            
            # Load admin level 1 boundaries by dissolving level 2
            self.admin1_gdf = self.admin_gdf.dissolve(by='ADM1_CODE')
            
        except Exception as e:
            print(f"Warning: Could not load admin boundaries: {str(e)}")
            self.admin_gdf = None
            self.admin1_gdf = None
            
    def geocode_location(self, location: str, country_code: str) -> Optional[Tuple[float, float]]:
        """
        Geocode a location string using Nominatim
        
        Args:
            location: Location name to geocode
            country_code: ISO country code to restrict search
            
        Returns:
            Tuple of (longitude, latitude) if found, None otherwise
        """
        if not location or not country_code:
            return None
            
        cache_key = f"{location}_{country_code}"
        if cache_key in self.geocoding_cache:
            return self.geocoding_cache[cache_key]
            
        try:
            # Add country code to improve geocoding accuracy
            search_text = f"{location}, {country_code}"
            location_data = self.geocode(search_text)
            
            if location_data:
                result = (location_data.longitude, location_data.latitude)
                self.geocoding_cache[cache_key] = result
                return result
                
        except Exception as e:
            print(f"Error geocoding location '{location}': {str(e)}")
            
        return None

    def get_geometry_from_location_string(self, location_str: str, country_code: str) -> Optional[Dict]:
        """
        Get geometry from a location string (semicolon-separated geonames)
        
        Args:
            location_str: Semicolon-separated location names
            country_code: ISO country code for the locations
            
        Returns:
            Dictionary containing geometry and bbox if any location was found
        """
        if not location_str or not country_code:
            return None
            
        try:
            # Split locations and geocode each one
            locations = [loc.strip() for loc in location_str.split(';')]
            coords = []
            
            for loc in locations:
                result = self.geocode_location(loc, country_code)
                if result:
                    coords.append(result)
                    
            if not coords:
                return None
                
            # If only one point, return point geometry
            if len(coords) == 1:
                point = Point(coords[0])
                return {
                    'geometry': mapping(point),
                    'bbox': [coords[0][0], coords[0][1], coords[0][0], coords[0][1]]
                }
                
            # If multiple points, create a MultiPoint geometry
            # This could be enhanced to create a convex hull or other area geometry
            multi_point = MultiPoint(coords)
            return {
                'geometry': mapping(multi_point),
                'bbox': list(multi_point.bounds)
            }
            
        except Exception as e:
            print(f"Error processing location string '{location_str}': {str(e)}")
            return None

    def get_geometry_from_admin_units(self, admin_units: str) -> Optional[Dict]:
        """
        Get geometry from admin units JSON string
        
        Args:
            admin_units: JSON string containing admin unit information
            
        Returns:
            Dictionary containing geometry and bbox if found, None otherwise
        """
        if not admin_units or not self.admin_gdf is not None:
            return None
            
        try:
            # Parse admin units JSON
            admin_list = json.loads(admin_units) if isinstance(admin_units, str) else None
            if not admin_list:
                return None
                
            geometries = []
            admin1_codes = set()
            
            # Collect all relevant geometries
            for entry in admin_list:
                if 'adm1_code' in entry:
                    admin1_codes.add(entry['adm1_code'])
                elif 'adm2_code' in entry:
                    # Find corresponding admin1 code
                    matching = self.admin_gdf[self.admin_gdf['ADM2_CODE'] == entry['adm2_code']]
                    if not matching.empty:
                        admin1_codes.add(matching.iloc[0]['ADM1_CODE'])
                        
            if not admin1_codes:
                return None
                
            # Get geometries for all admin1 codes
            geom_gdf = self.admin1_gdf.loc[list(admin1_codes)]
            if geom_gdf.empty:
                return None
                
            # Dissolve all geometries into one
            combined_geom = geom_gdf.geometry.unary_union
            
            # Create GeoJSON geometry
            geometry = mapping(combined_geom)
            bbox = list(combined_geom.bounds)
            
            return {
                'geometry': geometry,
                'bbox': bbox
            }
            
        except Exception as e:
            print(f"Error getting geometry from admin units: {str(e)}")
            return None

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
                print(f"Error creating event item for DisNo {row.get('DisNo', 'unknown')}: {str(e)}")
                continue

        return event_items

    def _create_event_item_from_row(self, row: pd.Series) -> Optional[Item]:
        """Create a single event item from a DataFrame row"""
        # Skip if required fields are missing
        if pd.isna(row.get('DisNo.')):
            return None

        # Create geometry from lat/lon if available
        # Try each geometry source in order of preference
        geometry = None
        bbox = None
        
        # 1. Try admin units first
        if not pd.isna(row.get('Admin Units')):
            geom_data = self.get_geometry_from_admin_units(row.get('Admin Units'))
            if geom_data:
                geometry = geom_data['geometry']
                bbox = geom_data['bbox']
                
        # 2. Fall back to lat/lon if available
        if geometry is None and not pd.isna(row.get('Latitude')) and not pd.isna(row.get('Longitude')):
            point = Point(float(row['Longitude']), float(row['Latitude']))
            geometry = mapping(point)
            bbox = [float(row['Longitude']), float(row['Latitude']), 
                   float(row['Longitude']), float(row['Latitude'])]
                   
        # 3. Finally, try geocoding location string if available
        if geometry is None and not pd.isna(row.get('Location')) and not pd.isna(row.get('ISO')):
            geom_data = self.get_geometry_from_location_string(row['Location'], row['ISO'])
            if geom_data:
                geometry = geom_data['geometry']
                bbox = geom_data['bbox']

        # Create event datetime
        start_date = self._create_datetime(row)
        
        # Create item
        item = Item(
            id=f"{STAC_EVENT_ID_PREFIX}{row['DisNo.']}",
            geometry=geometry,
            bbox=bbox,
            datetime=start_date,
            properties={
                "title": row.get('Event Name', ''),
                "description": f"EM-DAT disaster event: {row.get('Event Name', '')}",
                "start_datetime": start_date.isoformat() if start_date else None,
            }
        )

        # Add Monty extension
        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = 1  # EM-DAT doesn't have episodes
        monty.hazard_codes = [row.get('Classification Key', '')]
        monty.country_codes = [row['ISO']] if not pd.isna(row.get('ISO')) else []
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        # Set collection and roles
        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        # Add source link
        item.add_link(
            Link("via", 
                 f"https://public.emdat.be/data/{row['DisNo']}", 
                 "text/html",
                 "EM-DAT Event Data")
        )

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
            monty.hazard_detail = self._create_hazard_detail(original_row)

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
            'Total Deaths': (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.DEATHS),
            'No Injured': (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.INJURED),
            'No Affected': (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.TOTAL_AFFECTED),
            'No Homeless': (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.DISPLACED),
            'Total Affected': (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.TOTAL_AFFECTED),
            'Total Damages (\'000 US$)': (MontyImpactExposureCategory.ECONOMIC, MontyImpactType.ECONOMIC_LOSS)
        }

        for field, (category, impact_type) in impact_fields.items():
            if not pd.isna(row.get(field)) and float(row[field]) > 0:
                impact_item = self._create_impact_item(row, field, category, impact_type)
                if impact_item:
                    impact_items.append(impact_item)

        return impact_items

    def _create_impact_item(self, row: pd.Series, field: str, category: MontyImpactExposureCategory, 
                          impact_type: MontyImpactType) -> Optional[Item]:
        """Create a single impact item"""
        try:
            base_item = self._create_event_item_from_row(row)
            if not base_item:
                return None

            impact_item = base_item.clone()
            impact_item.id = f"{STAC_IMPACT_ID_PREFIX}{row['DisNo.']}-{field.lower().replace(' ', '-')}"
            impact_item.set_collection(self.get_impact_collection())
            impact_item.properties["roles"] = ["source", "impact"]

            monty = MontyExtension.ext(impact_item)
            monty.impact_detail = ImpactDetail(
                category=category,
                type=impact_type,
                value=float(row[field]),
                unit='USD' if 'Damages' in field else 'count',
                estimate_type=MontyEstimateType.PRIMARY
            )

            return impact_item
        except Exception as e:
            print(f"Error creating impact item for field {field}: {str(e)}")
            return None

    def _create_datetime(self, row: pd.Series) -> datetime:
        """Create datetime object from EM-DAT date fields"""
        year = int(row['Start Year']) if not pd.isna(row.get('Start Year')) else None
        month = int(row['Start Month']) if not pd.isna(row.get('Start Month')) else 1
        day = int(row['Start Day']) if not pd.isna(row.get('Start Day')) else 1

        if year:
            dt = datetime(year, month, day)
            return pytz.utc.localize(dt)
        return None

    def _create_hazard_detail(self, row: pd.Series) -> HazardDetail:
        """Create hazard detail from row data"""
        return HazardDetail(
            cluster=self.hazard_profiles.get_cluster_code([row.get('Classification Key', '')]),
            severity_value=float(row['Magnitude']) if not pd.isna(row.get('Magnitude')) else None,
            severity_unit=row.get('Magnitude Scale', 'emdat'),
            estimate_type=MontyEstimateType.PRIMARY
        )

    def _get_row_by_disno(self, disno: str) -> Optional[pd.Series]:
        """Get original DataFrame row by DisNo"""
        df = self.data.get_data()
        matching_rows = df[df['DisNo.'] == disno]
        return matching_rows.iloc[0] if not matching_rows.empty else None

    def get_event_collection(self) -> Collection:
        """Get event collection"""
        response = requests.get(self.emdat_events_collection_url)
        collection_dict = json.loads(response.text)
        return Collection.from_dict(collection_dict)

    def get_hazard_collection(self) -> Collection:
        """Get hazard collection"""
        response = requests.get(self.emdat_hazards_collection_url)
        collection_dict = json.loads(response.text)
        return Collection.from_dict(collection_dict)

    def get_impact_collection(self) -> Collection:
        """Get impact collection"""
        response = requests.get(self.emdat_impacts_collection_url)
        collection_dict = json.loads(response.text)
        return Collection.from_dict(collection_dict)