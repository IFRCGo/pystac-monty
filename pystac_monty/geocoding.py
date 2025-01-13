import json
import zipfile
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, Set, cast

import fiona  # type: ignore
from shapely.geometry import mapping, shape  # type: ignore
from shapely.ops import unary_union  # type: ignore


class MontyGeoCoder(ABC):
    @abstractmethod
    def get_geometry_from_admin_units(self, admin_units: str) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_geometry_by_country_name(self, country_name: str) -> Optional[Dict[str, Any]]:
        pass


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
        self._initialize_path()

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
            with zipfile.ZipFile(file_path, "r") as zf:
                return True
        except zipfile.BadZipFile:
            return False

    def _find_gpkg_in_zip(self, zip_path: str) -> Optional[str]:
        """Find the first .gpkg file in a ZIP archive"""
        with zipfile.ZipFile(zip_path, "r") as zf:
            for name in zf.namelist():
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
            return cast(Dict[str, Any], cached_value)

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
            return cast(Dict[str, Any], cached_value)

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

    def get_geometry_from_admin_units(self, admin_units: str) -> Optional[Dict[str, Any]]:
        """
        Get geometry from admin units JSON string

        Args:
            admin_units: JSON string containing admin unit information

        Returns:
            Dictionary containing geometry and bbox if found
        """
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
