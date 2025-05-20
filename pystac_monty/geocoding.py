import json
import typing
import zipfile
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set, Union

import fiona  # type: ignore
import requests
from shapely.geometry import Point, mapping, shape  # type: ignore
from shapely.ops import unary_union  # type: ignore

WORLD_ADMIN_BOUNDARIES_FGB = "world_admin_boundaries.fgb"
GAUL2014_2015_GPCK_ZIP = "gaul2014_2015.gpkg"


class MontyGeoCoder(ABC):
    @abstractmethod
    def get_geometry_from_admin_units(self, admin_units: str) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_geometry_by_country_name(self, country_name: str) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_iso3_from_point(self, point: Point) -> Optional[str]:
        pass

    @abstractmethod
    def get_iso3_from_geometry(self, geometry: Dict[str, Any]) -> Optional[str]:
        pass

    @abstractmethod
    def get_geometry_from_iso3(self, iso3: str) -> Optional[Dict[str, Any]]:
        pass


class TheirGeocoder(MontyGeoCoder):
    _base_url: str

    def __init__(self, url: str):
        self._base_url = url

    def _request(self, url: str, params: dict[str, typing.Any]):
        response = requests.get(
            f"{self._base_url}{url}",
            params=params,
            timeout=30,
        )
        if response.status_code == 200:
            return response.json()
        return None

    def get_geometry_from_admin_units(self, admin_units: str) -> Optional[Dict[str, Any]]:
        admin_list = json.loads(admin_units)
        # Collect admin1 codes from both direct references and admin2 mappings
        admin1_codes: Set[int] = set()
        admin2_codes: Set[int] = set()
        for entry in admin_list:
            if "adm1_code" in entry:
                admin1_codes.add(int(entry["adm1_code"]))
            elif "adm2_code" in entry:
                admin2_codes.add(int(entry["adm2_code"]))
        return self._request(
            "/admin2/geometries",
            {
                "admin1_codes": list(admin1_codes),
                "admin2_codes": list(admin2_codes),
            },
        )

    def get_geometry_by_country_name(self, country_name: str) -> Optional[Dict[str, Any]]:
        return self._request("/country/geometry", {"country_name": country_name})

    def get_iso3_from_point(self, point: Point) -> Optional[str]:
        response = self._request("/country/iso3", {"lat": point.y, "lng": point.x})
        return response["iso3"] if response else "UNK"

    # FIXME: This is not implemented
    def get_iso3_from_geometry(self, geometry: Dict[str, Any]) -> Optional[str]:
        return "UNK"

    def get_geometry_from_iso3(self, iso3: str) -> Optional[Dict[str, Any]]:
        return self._request("/country/geometry", {"iso3": iso3})


class WorldAdministrativeBoundariesGeocoder(MontyGeoCoder):
    def __init__(self, fgb_path: str, simplify_tolerance: float = 0.01) -> None:
        self.fgb_path = fgb_path
        self._path = ""
        self._layer = "Layer1"
        self._simplify_tolerance = simplify_tolerance
        self._cache: Dict[str, Union[Dict[str, Any], int, None]] = {}
        self._file_handle: fiona.Collection | None = None
        self._initialize_path()
        self._open_file()

    def __enter__(self) -> "WorldAdministrativeBoundariesGeocoder":
        """Context manager entry point"""
        return self

    def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[Any]) -> None:
        """Context manager exit point"""
        self.close()

    def _initialize_path(self) -> None:
        if self._is_zip_file(self.fgb_path):
            fgb_name = self._find_fgb_in_zip(self.fgb_path)
            if not fgb_name:
                raise ValueError("No .fgb file found in ZIP archive")
            self._path = f"zip://{self.fgb_path}!/{fgb_name}"
        else:
            self._path = self.fgb_path

    def _open_file(self) -> None:
        """Open the file and keep the handle in memory"""
        if self._path and not self._file_handle:
            try:
                self._file_handle = fiona.open(self._path, layer=self._layer)
            except Exception as e:
                print(f"Error opening file: {str(e)}")
                self._file_handle = None

    def close(self) -> None:
        """Close the file handle if it's open"""
        if self._file_handle:
            self._file_handle.close()
            self._file_handle = None

    def _is_zip_file(self, file_path: str) -> bool:
        """Check if a file is a ZIP file"""
        try:
            with zipfile.ZipFile(file_path, "r"):
                return True
        except zipfile.BadZipFile:
            return False

    def _find_fgb_in_zip(self, zip_path: str) -> Optional[str]:
        """Find the first .fgb file in a ZIP archive"""
        with zipfile.ZipFile(zip_path, "r") as zf:
            names: List[str] = zf.namelist()
            for name in names:
                if name.lower().endswith(".fgb"):
                    return name
        return None

    def get_iso3_from_geometry(self, geometry: Dict[str, Any]) -> Optional[str]:
        if not geometry or not self._path:
            return None

        # Create a cache key based on the geometry
        # Using a hash of the stringified geometry as the key
        geom_str = json.dumps(mapping(shape(geometry)), sort_keys=True)
        cache_key = f"geom_iso3_{hash(geom_str)}"

        # Check cache first
        cached_value = self._cache.get(cache_key)
        if cached_value is not None:
            return cached_value if cached_value else None  # Handle None values in cache

        # Reopen file if handle is closed
        if self._file_handle is None or self._file_handle.closed:
            self._open_file()

        if self._file_handle is None:
            return None

        try:
            # Convert input geometry to a shapely object
            point = shape(geometry)

            # Use the spatial filter if available in the file handle
            # This leverages FlatGeobuf's spatial indexing capabilities
            if hasattr(self._file_handle, "filter") and callable(getattr(self._file_handle, "filter")):
                # Get a small bounding box around the point to use for filtering
                # This is more efficient than checking against all features
                bbox = (point.x - 0.001, point.y - 0.001, point.x + 0.001, point.y + 0.001)
                filtered_features = self._file_handle.filter(bbox=bbox)
                features_to_check = filtered_features
            else:
                # Reset cursor to beginning of file if spatial filtering is not available
                self._file_handle.reset()
                features_to_check = self._file_handle

            # Check each feature to see if it contains the point
            for feature in features_to_check:
                if shape(feature["geometry"]).contains(point):
                    iso3 = feature["properties"]["iso3"]
                    # Cache the result
                    self._cache[cache_key] = iso3
                    return iso3

            # Cache negative result to avoid repeated lookups
            self._cache[cache_key] = None
        except Exception as e:
            print(f"Error getting ISO3 from geometry: {str(e)}")
            return None

        return None

    def get_iso3_from_point(self, point: Point) -> Optional[str]:
        self.get_iso3_from_geometry(point.__geo_interface__)

    def get_geometry_from_admin_units(self, admin_units: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError("Method not implemented")

    def get_geometry_by_country_name(self, country_name: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError("Method not implemented")

    def get_geometry_from_iso3(self, iso3: str) -> Optional[Dict[str, Any]]:
        # Check cache first
        cache_key = f"iso3_geom_{iso3}"
        cached_value = self._cache.get(cache_key)
        if cached_value is not None and isinstance(cached_value, dict):
            return cached_value

        if not iso3 or not self._path:
            return None

        # Reopen file if handle is closed
        if self._file_handle is None or self._file_handle.closed:
            self._open_file()

        if self._file_handle is None:
            return None

        try:
            # Reset cursor to beginning of file
            for feature in self._file_handle:
                if feature["properties"]["iso3"] == iso3:
                    geom = shape(feature["geometry"]).simplify(self._simplify_tolerance, preserve_topology=True)
                    result = {"geometry": mapping(geom), "bbox": list(geom.bounds)}
                    # Cache the result
                    self._cache[cache_key] = result
                    return result
        except Exception as e:
            print(f"Error getting geometry from ISO3: {str(e)}")
            return None

        return None


class GAULGeocoder(MontyGeoCoder):
    """
    Implementation of MontyGeoCoder using GAUL geopackage for geocoding.
    Loads features dynamically as needed.
    """

    def __init__(self, gpkg_path: str, simplify_tolerance: float = 0.01) -> None:
        """
        Initialize GAULGeocoder

        Args:
            gpkg_path: Path to the GAUL geopackage file or ZIP containing it
            simplify_tolerance: Tolerance for polygon simplification using Douglas-Peucker algorithm.
                              Higher values result in more simplification. Default is 0.01 degrees.
        """
        self.gpkg_path = gpkg_path
        self._path = ""  # Initialize as empty string instead of None
        self._layer = "level2"
        self._simplify_tolerance = simplify_tolerance
        self._cache: Dict[str, Union[Dict[str, Any], int, None]] = {}  # Cache for frequently accessed geometries
        self._file_handle = None

        self._initialize_path()
        self._open_file()

    def __enter__(self) -> "GAULGeocoder":
        """Context manager entry point"""
        return self

    def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[Any]) -> None:
        """Context manager exit point"""
        self.close()

    def _initialize_path(self) -> None:
        """Set up the correct path for fiona to read"""
        if self._is_zip_file(self.gpkg_path):
            gpkg_name = self._find_gpkg_in_zip(self.gpkg_path)
            if not gpkg_name:
                raise ValueError("No .gpkg file found in ZIP archive")
            self._path = f"zip://{self.gpkg_path}!/{gpkg_name}"
        else:
            self._path = self.gpkg_path

    def _open_file(self) -> None:
        """Open the file and keep the handle in memory"""
        if self._path and not self._file_handle:
            try:
                self._file_handle = fiona.open(self._path, layer=self._layer)
            except Exception as e:
                print(f"Error opening file: {str(e)}")
                self._file_handle = None

    def close(self) -> None:
        """Close the file handle if it's open"""
        if self._file_handle:
            self._file_handle.close()
            self._file_handle = None

    def _is_zip_file(self, file_path: str) -> bool:
        """Check if a file is a ZIP file"""
        try:
            with zipfile.ZipFile(file_path, "r"):
                return True
        except zipfile.BadZipFile:
            return False

    def _find_gpkg_in_zip(self, zip_path: str) -> Optional[str]:
        """Find the first .gpkg file in a ZIP archive"""
        with zipfile.ZipFile(zip_path, "r") as zf:
            names: List[str] = zf.namelist()
            for name in names:
                if name.lower().endswith(".gpkg"):
                    return name
        return None

    def _get_admin1_for_admin2(self, adm2_code: int) -> Optional[int]:
        """Get admin1 code for an admin2 code"""
        cache_key = f"adm2_{adm2_code}"
        cached_value = self._cache.get(cache_key)
        if cached_value is not None and isinstance(cached_value, int):
            return cached_value

        if not self._path:
            return None

        # Reopen file if handle is closed
        if not self._file_handle:
            self._open_file()

        if not self._file_handle:
            return None

        # Reset cursor to beginning of file
        self._file_handle.reset()
        for feature in self._file_handle:
            if feature["properties"]["ADM2_CODE"] == adm2_code:
                adm1_code = int(feature["properties"]["ADM1_CODE"])
                self._cache[cache_key] = adm1_code
                return adm1_code
        return None

    def _get_admin1_geometry(self, adm1_code: int) -> Optional[Dict[str, Any]]:
        """Get geometry for an admin1 code"""
        cache_key = f"adm1_geom_{adm1_code}"
        cached_value = self._cache.get(cache_key)
        if cached_value is not None and isinstance(cached_value, dict):
            return cached_value

        if not self._path:
            return None

        # Reopen file if handle is closed
        if not self._file_handle:
            self._open_file()

        if not self._file_handle:
            return None

        features = []
        # Reset cursor to beginning of file
        self._file_handle.reset()
        for feature in self._file_handle:
            if feature["properties"]["ADM1_CODE"] == adm1_code:
                features.append(shape(feature["geometry"]))

        if not features:
            return None

        # Combine all geometries and simplify
        combined = unary_union(features)
        simplified = combined.simplify(self._simplify_tolerance, preserve_topology=True)
        result = {"geometry": mapping(simplified), "bbox": list(simplified.bounds)}
        self._cache[cache_key] = result
        return result

    def _get_country_geometry_by_adm0(self, adm0_code: int) -> Optional[Dict[str, Any]]:
        """Get geometry for a country by ADM0 code"""
        cache_key = f"adm0_geom_{adm0_code}"
        cached_value = self._cache.get(cache_key)
        if cached_value is not None and isinstance(cached_value, dict):
            return cached_value

        if not self._path:
            return None

        # Reopen file if handle is closed
        if not self._file_handle:
            self._open_file()

        if not self._file_handle:
            return None

        features = []
        # Reset cursor to beginning of file
        self._file_handle.reset()
        for feature in self._file_handle:
            if feature["properties"]["ADM0_CODE"] == adm0_code:
                features.append(shape(feature["geometry"]))

        if not features:
            return None

        # Combine all geometries and simplify
        combined = unary_union(features)
        simplified = combined.simplify(self._simplify_tolerance, preserve_topology=True)
        result = {"geometry": mapping(simplified), "bbox": list(simplified.bounds)}
        self._cache[cache_key] = result
        return result

    def _get_name_to_adm0_mapping(self, name: str) -> Optional[int]:
        """Get ADM0 code for an country name"""
        cache_key = f"country_{name}"
        cached_value = self._cache.get(cache_key)
        if cached_value is not None and isinstance(cached_value, int):
            return cached_value

        if not self._path:
            return None

        # Reopen file if handle is closed
        if not self._file_handle:
            self._open_file()

        if not self._file_handle:
            return None

        # Reset cursor to beginning of file
        self._file_handle.reset()
        # Check first few records until we find a match
        for feature in self._file_handle:
            if feature["properties"].get("ADM0_NAME", "").lower() == name.lower():
                adm0_code = int(feature["properties"]["ADM0_CODE"])
                self._cache[cache_key] = adm0_code
                return adm0_code
        return None

    def get_geometry_from_admin_units(self, admin_units: str) -> Optional[Dict[str, Any]]:
        """
        Get geometry from admin units JSON string

        Args:
            admin_units: JSON string containing admin unit information

        Returns:
            Dictionary containing geometry and bbox if found
        """
        # Check if we have a valid path and admin_units
        if not admin_units or not self._path:
            return None

        # Create a cache key based on the admin_units string
        cache_key = f"admin_units_{hash(admin_units)}"
        cached_value = self._cache.get(cache_key)
        if cached_value is not None and isinstance(cached_value, dict):
            return cached_value

        try:
            # Parse admin units JSON
            admin_list = json.loads(admin_units) if isinstance(admin_units, str) else None
            if not admin_list:
                return None

            # Collect admin1 codes from both direct references and admin2 mappings
            admin1_codes: Set[int] = set()
            for entry in admin_list:
                if "adm1_code" in entry:
                    admin1_codes.add(int(entry["adm1_code"]))
                elif "adm2_code" in entry:
                    adm2_code = int(entry["adm2_code"])
                    adm1_code = self._get_admin1_for_admin2(adm2_code)
                    if adm1_code is not None:
                        admin1_codes.add(adm1_code)

            if not admin1_codes:
                return None

            # Get and combine geometries
            geoms: List[Any] = []
            for adm1_code in admin1_codes:
                geom_data = self._get_admin1_geometry(adm1_code)
                if geom_data and isinstance(geom_data, dict):
                    geoms.append(shape(geom_data["geometry"]))

            if not geoms:
                return None

            # Combine geometries and simplify
            combined = unary_union(geoms)
            simplified = combined.simplify(self._simplify_tolerance, preserve_topology=True)
            result = {"geometry": mapping(simplified), "bbox": list(simplified.bounds)}

            # Cache the result
            self._cache[cache_key] = result
            return result

        except Exception as e:
            print(f"Error getting geometry from admin units: {str(e)}")
            return None

    def get_geometry_by_country_name(self, country_name: str) -> Optional[Dict[str, Any]]:
        """
        Get geometry for a country by its name

        Args:
            country_name: Country name

        Returns:
            Dictionary containing geometry and bbox if found
        """

        if not country_name or not self._path:
            return None

        # Create a cache key based on the country name
        cache_key = f"country_geom_{country_name.lower()}"
        cached_value = self._cache.get(cache_key)
        if cached_value is not None and isinstance(cached_value, dict):
            return cached_value

        try:
            # Get ADM0 code for the country name
            adm0_code = self._get_name_to_adm0_mapping(country_name)
            if not adm0_code:
                return None

            # Get country geometry
            result = self._get_country_geometry_by_adm0(adm0_code)
            if result:
                # Cache the result
                self._cache[cache_key] = result
            return result

        except Exception as e:
            print(f"Error getting country geometry for {country_name}: {str(e)}")
            return None

    # FIXME: This is not implemented
    def get_iso3_from_point(self, point: Point) -> Optional[str]:
        return "UNK"

    # FIXME: This is not being used
    def get_iso3_from_geometry(self, geometry: Dict[str, Any]) -> Optional[str]:
        return "UNK"

    # FIXME: This is not implemented
    def get_geometry_from_iso3(self, iso3: str) -> Optional[Dict[str, Any]]:
        return None


class MockGeocoder(MontyGeoCoder):
    """
    Mock implementation of MontyGeoCoder for testing purposes.
    Returns simplified test geometries without requiring GAUL data.
    """

    def __init__(self) -> None:
        """Initialize mock geocoder with test geometries"""
        # Test geometries for Spain and its admin units
        self._test_geometries: Dict[str, Dict[str, Any]] = {
            # Simplified polygon for Spain
            "USA": {
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-125.0, 25.0],  # Southwest
                            [-125.0, 50.0],  # Northwest
                            [-67.0, 50.0],  # Northeast
                            [-67.0, 25.0],  # Southeast
                            [-125.0, 25.0],  # Close polygon
                        ]
                    ],
                },
                "bbox": [
                    -125.0,
                    25.0,
                    -67.0,
                    50.0,
                ],
            },
            "ESP": {
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-9.0, 36.0],  # Southwest
                            [-9.0, 44.0],  # Northwest
                            [3.0, 44.0],  # Northeast
                            [3.0, 36.0],  # Southeast
                            [-9.0, 36.0],  # Close polygon
                        ]
                    ],
                },
                "bbox": [-9.0, 36.0, 3.0, 44.0],
            },
            # Test admin unit geometry
            "admin1": {
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-2.0, 40.0], [-2.0, 42.0], [0.0, 42.0], [0.0, 40.0], [-2.0, 40.0]]],
                },
                "bbox": [-2.0, 40.0, 0.0, 42.0],
            },
            "NPL": {
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [80.058, 26.347],  # Southwest
                            [80.058, 30.447],  # Northwest
                            [88.201, 30.447],  # Northeast
                            [88.201, 26.347],  # Southeast
                            [80.058, 26.347],  # Close polygon
                        ]
                    ],
                },
                "bbox": [80.058, 26.347, 88.201, 30.447],
            },
            "MAR": {
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-13.0, 27.6],  # Southwest
                            [-13.0, 35.9],  # Northwest
                            [-0.9, 35.9],  # Northeast
                            [-0.9, 27.6],  # Southeast
                            [-13.0, 27.6],  # Close polygon
                        ]
                    ],
                },
                "bbox": [-13.0, 27.6, -0.9, 35.9],
            },
        }

    def get_geometry_from_admin_units(self, admin_units: str) -> Optional[Dict[str, Any]]:
        """
        Get mock geometry for admin units.
        Returns a simple test polygon for any valid admin unit JSON.

        Args:
            admin_units: JSON string containing admin unit information

        Returns:
            Dictionary containing geometry and bbox if found
        """
        if not admin_units:
            return None

        try:
            admin_list = json.loads(admin_units) if isinstance(admin_units, str) else None
            if not admin_list:
                return None

            # Return test geometry for any valid admin unit request
            if isinstance(admin_list, list) and len(admin_list) > 0:
                return self._test_geometries["admin1"]
            return None

        except Exception as e:
            print(f"Error getting mock geometry from admin units: {str(e)}")
            return None

    def get_geometry_by_country_name(self, country_name: str) -> Optional[Dict[str, Any]]:
        """
        Get mock geometry for a country.
        Returns a simple test polygon for Spain ("ESP").

        Args:
            country_name: Country name

        Returns:
            Dictionary containing geometry and bbox if found
        """
        if not country_name:
            return None

        try:
            # Return test geometry for Spain
            if country_name.lower() == "spain":
                return self._test_geometries["ESP"]
            if country_name.lower() == "united states of america":
                return self._test_geometries["ESP"]
            return None
            return None

        except Exception as e:
            print(f"Error getting mock country geometry: {str(e)}")
            return None

    def get_iso3_from_geometry(self, geometry: Dict[str, Any]) -> Optional[str]:
        """
        Get ISO3 code for a geometry.
        Returns the ISO3 code of the first test geometry that intersects with the input geometry.

        Args:
            geometry: GeoJSON geometry dict

        Returns:
            Optional[str]: ISO3 code if geometry intersects with any test geometry, None otherwise
        """
        if not geometry:
            return None

        try:
            # Convert input geometry to shapely
            input_shape = shape(geometry)

            # Test intersection with all test geometries
            for iso3, test_geom in self._test_geometries.items():
                # Skip non-country geometries (like 'admin1')
                if len(iso3) != 3:
                    continue

                test_shape = shape(test_geom["geometry"])
                if input_shape.intersects(test_shape):
                    return iso3

            return None

        except Exception as e:
            print(f"Error getting mock ISO3 from geometry: {str(e)}")
            return None

    def get_iso3_from_point(self, point: Point) -> Optional[str]:
        """
        Get ISO3 code for point
        Returns the ISO3 code of the first test geometry that intersects with the input point.

        Args:
            geometry: Point

        Returns:
            Optional[str]: ISO3 code if geometry intersects with any test point, None otherwise
        """
        if not Point:
            return None

        return self.get_iso3_from_geometry(point.__geo_interface__)

    def get_geometry_from_iso3(self, iso3: str) -> Optional[Dict[str, Any]]:
        """
        Get geometry for an ISO3 code.
        Returns the test geometry for the given ISO3 code.

        Args:
            iso3: ISO3 code

        Returns:
            Optional[Dict[str, Any]]: Geometry and bbox if found, None otherwise
        """
        if not iso3:
            return None

        try:
            return self._test_geometries.get(iso3)
        except Exception as e:
            print(f"Error getting mock geometry from ISO3: {str(e)}")

        return None
