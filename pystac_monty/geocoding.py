import json
import zipfile
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set, Union

import fiona  # type: ignore
import requests
from shapely.geometry import mapping, shape  # type: ignore
from shapely.ops import unary_union  # type: ignore


class MontyGeoCoder(ABC):
    @abstractmethod
    def get_geometry_from_admin_units(self, admin_units: str) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_geometry_by_country_name(self, country_name: str) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_iso3_from_geometry(self, geometry: Dict[str, Any]) -> Optional[str]:
        pass

    @abstractmethod
    def get_geometry_from_iso3(self, iso3: str) -> Optional[Dict[str, Any]]:
        pass


WORLD_ADMIN_BOUNDARIES_FGB = "world_admin_boundaries.fgb"


class WorldAdministrativeBoundariesGeocoder(MontyGeoCoder):
    def __init__(self, fgb_path: str, simplify_tolerance: float = 0.01) -> None:
        self.fgb_path = fgb_path
        self._path = ""
        self._layer = "Layer1"
        self._simplify_tolerance = simplify_tolerance
        self._cache: Dict[str, Union[Dict[str, Any], int, None]] = {}
        self._initialize_path()

    def _initialize_path(self) -> None:
        if self._is_zip_file(self.fgb_path):
            fgb_name = self._find_fgb_in_zip(self.fgb_path)
            if not fgb_name:
                raise ValueError("No .fgb file found in ZIP archive")
            self._path = f"zip://{self.fgb_path}!/{fgb_name}"
        else:
            self._path = self.fgb_path

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

        try:
            point = shape(geometry)
            with fiona.open(self._path, layer=self._layer) as src:
                for feature in src:
                    if shape(feature["geometry"]).contains(point):
                        return feature["properties"]["iso3"]
        except Exception as e:
            print(f"Error getting ISO3 from geometry: {str(e)}")
            return None

        return None

    def get_geometry_from_admin_units(self, admin_units: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError("Method not implemented")

    def get_geometry_by_country_name(self, country_name: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError("Method not implemented")

    def get_geometry_from_iso3(self, iso3: str) -> Optional[Dict[str, Any]]:
        if not iso3 or not self._path:
            return None

        try:
            with fiona.open(self._path, layer=self._layer) as src:
                for feature in src:
                    if feature["properties"]["iso3"] == iso3:
                        geom = shape(feature["geometry"]).simplify(self._simplify_tolerance, preserve_topology=True)
                        return {"geometry": mapping(geom), "bbox": list(geom.bounds)}
        except Exception as e:
            print(f"Error getting geometry from ISO3: {str(e)}")
            return None

        return None


GAUL2014_2015_GPCK_ZIP = "gaul2014_2015.gpkg"


class GAULGeocoder(MontyGeoCoder):
    """
    Implementation of MontyGeoCoder using GAUL geopackage for geocoding.
    Loads features dynamically as needed.
    """

    def __init__(self, gpkg_path: Optional[str], service_base_url: Optional[str], simplify_tolerance: float = 0.01) -> None:
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

        if not gpkg_path and not service_base_url:
            raise ValueError("Atleast the gpkg_path or service_base_url should be set.")

        if self.gpkg_path:
            self._initialize_path()
        else:
            self.service_base_url = service_base_url
            self.request_timeout = 30

    def _initialize_path(self) -> None:
        """Set up the correct path for fiona to read"""
        if self._is_zip_file(self.gpkg_path):
            gpkg_name = self._find_gpkg_in_zip(self.gpkg_path)
            if not gpkg_name:
                raise ValueError("No .gpkg file found in ZIP archive")
            self._path = f"zip://{self.gpkg_path}!/{gpkg_name}"
        else:
            self._path = self.gpkg_path

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

        with fiona.open(self._path, layer=self._layer) as src:
            for feature in src:
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

        features = []
        with fiona.open(self._path, layer=self._layer) as src:
            for feature in src:
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

        features = []
        with fiona.open(self._path, layer=self._layer) as src:
            for feature in src:
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

        with fiona.open(self._path, layer=self._layer) as src:
            # Check first few records until we find a match
            for feature in src:
                if feature["properties"].get("ADM0_NAME", "").lower() == name.lower():
                    adm0_code = int(feature["properties"]["ADM0_CODE"])
                    self._cache[cache_key] = adm0_code
                    return adm0_code
        return None

    def _service_request_handler(self, service_url: str, params: dict):
        response = requests.get(service_url, params=params, timeout=self.request_timeout)
        if response.status_code == 200:
            return response.json()
        return None

    def get_geometry_from_admin_units(self, admin_units: str) -> Optional[Dict[str, Any]]:
        """
        Get geometry from admin units JSON string

        Args:
            admin_units: JSON string containing admin unit information

        Returns:
            Dictionary containing geometry and bbox if found
        """
        if not self.gpkg_path:
            params = {"admin_units": admin_units}
            service_url = f"{self.service_base_url}/by_admin_units"
            return self._service_request_handler(service_url=service_url, params=params)

        if not admin_units or not self._path:
            return None

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
            return {"geometry": mapping(simplified), "bbox": list(simplified.bounds)}

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

        if not self.gpkg_path:
            params = {"country_name": country_name}
            service_url = f"{self.service_base_url}/by_country_name"
            return self._service_request_handler(service_url=service_url, params=params)

        if not country_name or not self._path:
            return None

        try:
            # Get ADM0 code for the country name
            adm0_code = self._get_name_to_adm0_mapping(country_name)
            if not adm0_code:
                return None

            # Get country geometry
            return self._get_country_geometry_by_adm0(adm0_code)

        except Exception as e:
            print(f"Error getting country geometry for {country_name}: {str(e)}")
            return None

    def get_iso3_from_geometry(self, geometry: Dict[str, Any]) -> Optional[str]:
        raise NotImplementedError("Method not implemented")

    def get_geometry_from_iso3(self, iso3: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError("Method not implemented")


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
