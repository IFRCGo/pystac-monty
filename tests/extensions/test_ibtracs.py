"""Tests for pystac.tests.extensions.monty IBTrACS source"""

import json
import unittest
from os import makedirs

import pytest
import requests
from parameterized import parameterized
from shapely.geometry import LineString

from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import WorldAdministrativeBoundariesGeocoder
from pystac_monty.sources.ibtracs import IBTrACSDataSource, IBTrACSTransformer
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator

CURRENT_SCHEMA_URI = "https://ifrcgo.github.io/monty/v0.1.0/schema.json"
CURRENT_SCHEMA_MAPURL = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/json-schema/schema.json"

geocoder = WorldAdministrativeBoundariesGeocoder(get_data_file("world-administrative-boundaries.fgb"), 0.1)


# Sample IBTrACS CSV data for testing
SAMPLE_IBTRACS_CSV = """SID,SEASON,BASIN,NAME,ISO_TIME,LAT,LON,WMO_WIND,WMO_PRES,USA_WIND,USA_PRES,TRACK_TYPE,DIST2LAND,LANDFALL,USA_SSHS,USA_STATUS
2024178N09335,2024,NA,BERYL,2024-06-26 00:00:00,9.4,-25.5,15,1008,15,1008,main,1000,0,TD,TD
2024178N09335,2024,NA,BERYL,2024-06-26 06:00:00,9.5,-26.2,20,1007,20,1007,main,1050,0,TD,TD
2024178N09335,2024,NA,BERYL,2024-06-26 12:00:00,9.6,-27.0,25,1006,25,1006,main,1100,0,TD,TD
2024178N09335,2024,NA,BERYL,2024-06-26 18:00:00,9.7,-27.9,30,1005,30,1005,main,1150,0,TD,TD
2024178N09335,2024,NA,BERYL,2024-06-27 00:00:00,9.8,-28.8,35,1004,35,1004,main,1200,0,TS,TS
2024178N09335,2024,NA,BERYL,2024-06-27 06:00:00,9.9,-29.5,40,1003,40,1003,main,1250,0,TS,TS
2024178N09335,2024,NA,BERYL,2024-06-27 12:00:00,9.9,-30.1,45,1002,45,1002,main,1300,0,TS,TS
2024178N09335,2024,NA,BERYL,2024-06-27 18:00:00,10.0,-30.8,50,1000,50,1000,main,1350,0,TS,TS
2024178N09335,2024,NA,BERYL,2024-06-28 00:00:00,10.0,-31.5,55,998,55,998,main,1400,0,TS,TS
2024178N09335,2024,NA,BERYL,2024-06-28 06:00:00,10.0,-32.0,60,996,60,996,main,1450,0,TS,TS
2024178N09335,2024,NA,BERYL,2024-06-28 12:00:00,10.0,-32.5,65,994,65,994,main,1500,0,HU,HU1
2024178N09335,2024,NA,BERYL,2024-06-28 18:00:00,10.0,-33.1,70,992,70,992,main,1550,0,HU,HU1
2024178N09335,2024,NA,BERYL,2024-06-29 00:00:00,9.9,-33.8,75,990,75,990,main,1600,0,HU,HU1
2024178N09335,2024,NA,BERYL,2024-06-29 06:00:00,9.8,-34.6,80,988,80,988,main,1650,0,HU,HU1
2024178N09335,2024,NA,BERYL,2024-06-29 12:00:00,9.7,-35.4,85,986,85,986,main,1700,0,HU,HU2
2024178N09335,2024,NA,BERYL,2024-06-29 18:00:00,9.6,-36.0,90,984,90,984,main,1750,0,HU,HU2
2024178N09335,2024,NA,BERYL,2024-06-30 00:00:00,9.5,-36.7,95,982,95,982,main,1800,0,HU,HU2
2024178N09335,2024,NA,BERYL,2024-06-30 06:00:00,9.4,-37.4,100,980,100,980,main,1850,0,HU,HU3
2024178N09335,2024,NA,BERYL,2024-06-30 12:00:00,9.3,-38.1,105,978,105,978,main,1900,0,HU,HU3
2024178N09335,2024,NA,BERYL,2024-06-30 18:00:00,9.2,-38.8,110,976,110,976,main,1950,0,HU,HU3
2024178N09335,2024,NA,BERYL,2024-07-01 00:00:00,9.1,-39.5,115,974,115,974,main,2000,0,HU,HU4
2024178N09335,2024,NA,BERYL,2024-07-01 06:00:00,9.0,-40.2,120,972,120,972,main,2050,0,HU,HU4
2024178N09335,2024,NA,BERYL,2024-07-01 12:00:00,9.0,-41.0,125,970,125,970,main,2100,0,HU,HU4
"""  # noqa: E501

# Sample IBTrACS CSV data with multiple storms
MULTI_STORM_IBTRACS_CSV = """SID,SEASON,BASIN,NAME,ISO_TIME,LAT,LON,WMO_WIND,WMO_PRES,USA_WIND,USA_PRES,TRACK_TYPE,DIST2LAND,LANDFALL,USA_SSHS,USA_STATUS
2024178N09335,2024,NA,BERYL,2024-06-26 00:00:00,9.4,-25.5,15,1008,15,1008,main,1000,0,TD,TD
2024178N09335,2024,NA,BERYL,2024-06-26 06:00:00,9.5,-26.2,20,1007,20,1007,main,1050,0,TD,TD
2024178N09335,2024,NA,BERYL,2024-06-26 12:00:00,9.6,-27.0,25,1006,25,1006,main,1100,0,TD,TD
2024178N09335,2024,NA,BERYL,2024-06-26 18:00:00,9.7,-27.9,30,1005,30,1005,main,1150,0,TD,TD
2024200N12345,2024,NA,CHRIS,2024-07-18 00:00:00,12.0,-45.0,15,1008,15,1008,main,1000,0,TD,TD
2024200N12345,2024,NA,CHRIS,2024-07-18 06:00:00,12.1,-45.5,20,1007,20,1007,main,1050,0,TD,TD
2024200N12345,2024,NA,CHRIS,2024-07-18 12:00:00,12.2,-46.0,25,1006,25,1006,main,1100,0,TD,TD
2024200N12345,2024,NA,CHRIS,2024-07-18 18:00:00,12.3,-46.5,30,1005,30,1005,main,1150,0,TD,TD
"""  # noqa: E501

# Sample IBTrACS CSV data with landfall
LANDFALL_IBTRACS_CSV = """SID,SEASON,BASIN,NAME,ISO_TIME,LAT,LON,WMO_WIND,WMO_PRES,USA_WIND,USA_PRES,TRACK_TYPE,DIST2LAND,LANDFALL,USA_SSHS,USA_STATUS
2024178N09335,2024,NA,BERYL,2024-07-12 00:00:00,17.5,-77.5,30,1005,30,1005,main,0,1,TD,TD
2024178N09335,2024,NA,BERYL,2024-07-12 06:00:00,17.8,-78.3,25,1006,25,1006,main,0,1,TD,TD
2024178N09335,2024,NA,BERYL,2024-07-12 12:00:00,18.1,-79.2,20,1007,20,1007,main,0,1,TD,TD
2024178N09335,2024,NA,BERYL,2024-07-12 18:00:00,18.3,-80.1,15,1008,15,1008,main,0,1,TD,TD
"""  # noqa: E501

# Invalid CSV data for testing error handling
INVALID_IBTRACS_CSV = """INVALID,CSV,DATA
no,proper,headers
"""

# CSV data with missing required fields
MISSING_FIELDS_CSV = """SID,SEASON,BASIN,NAME
2024178N09335,2024,NA,BERYL
"""

# Empty CSV data for testing error handling
EMPTY_IBTRACS_CSV = """SID,SEASON,BASIN,NAME,ISO_TIME,LAT,LON,WMO_WIND,WMO_PRES,USA_WIND,USA_PRES
"""


# Test scenarios
beryl_scenario = (
    "beryl",  # Scenario name
    SAMPLE_IBTRACS_CSV,  # CSV data
    "https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r00/access/csv/ibtracs.NA.list.v04r00.csv",  # noqa
)

multi_storm_scenario = (
    "multi_storm",  # Scenario name
    MULTI_STORM_IBTRACS_CSV,  # CSV data
    "https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r00/access/csv/ibtracs.NA.list.v04r00.csv",  # noqa
)

landfall_scenario = (
    "landfall",  # Scenario name
    LANDFALL_IBTRACS_CSV,  # CSV data
    "https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r00/access/csv/ibtracs.NA.list.v04r00.csv",  # noqa
)

na_scenario = (
    "North Atlantic",  # Scenario name
    "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/model/sources/IBTrACS/ibtracs.NA.list.v04r01.csv",  # noqa
    "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/model/sources/IBTrACS/ibtracs.NA.list.v04r01.csv",  # noqa
)


def load_scenarios(scenarios) -> list[tuple[str, IBTrACSTransformer]]:  # type: ignore
    """Load test scenarios for IBTrACS transformation testing.

    Args:
        scenarios: List of tuples containing (name, csv_data, source_url)

    Returns:
        List of IBTrACSTransformer instances initialized with test data
    """
    transformers = []
    for scenario in scenarios:
        name, csv_data, source_url = scenario

        # Create data source and transformer
        # fetch the data if url is provided
        if csv_data.startswith("http"):
            csv_data = requests.get(csv_data).text

        data_source = IBTrACSDataSource(source_url, csv_data)
        transformers.append((name, IBTrACSTransformer(data_source, geocoder)))

    return transformers


class IBTrACSTest(unittest.TestCase):
    """Test suite for IBTrACS transformation functionality"""

    scenarios = [beryl_scenario, multi_storm_scenario, landfall_scenario, na_scenario]

    def setUp(self) -> None:
        """Set up test environment"""
        super().setUp()
        self.validator = CustomValidator()
        # Create temporary folder for test outputs
        makedirs(get_data_file("temp/ibtracs"), exist_ok=True)

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_transformer(self, name: str, transformer: IBTrACSTransformer) -> None:
        """Test IBTrACS transformation to STAC items

        Args:
            name: Name of the test scenario
            transformer: IBTrACSTransformer instance to test

        Tests:
            - Items are created
            - Items validate against schema
            - Source event and hazard items are present
            - Items can be serialized to JSON
        """
        items = list(transformer.make_items())
        self.assertTrue(len(items) > 0)

        # Track items we find
        source_event_items = []
        source_hazard_items = []

        for item in items:
            # Write pretty JSON in temporary folder for manual inspection
            item_path = get_data_file(f"temp/ibtracs/{item.id}.json")
            with open(item_path, "w", encoding="utf-8") as f:
                json.dump(item.to_dict(), f, indent=2, ensure_ascii=False)

            # Validate item against schema
            item.validate(validator=self.validator)

            # Check item type
            roles = item.properties.get("roles", [])
            if "event" in roles and "source" in roles:
                source_event_items.append(item)
            elif "hazard" in roles and "source" in roles:
                source_hazard_items.append(item)

        # Verify required items were created
        if name == "beryl":
            # For single storm scenario, we should have 1 event and multiple hazards
            self.assertEqual(len(source_event_items), 1)
            self.assertGreater(len(source_hazard_items), 0)

            # Check that the event item has the correct storm ID
            self.assertEqual(source_event_items[0].id, "2024178N09335")

            # Check that hazard items have the correct prefix
            for hazard_item in source_hazard_items:
                self.assertTrue(hazard_item.id.startswith("2024178N09335-hazard-"))

        elif name == "multi_storm":
            # For multi-storm scenario, we should have 2 events
            self.assertEqual(len(source_event_items), 2)
            self.assertGreater(len(source_hazard_items), 0)

            # Check that we have both storm IDs in the event items
            storm_ids = [item.id for item in source_event_items]
            self.assertIn("2024178N09335", storm_ids)
            self.assertIn("2024200N12345", storm_ids)

    def test_data_source_parsing(self) -> None:
        """Test IBTrACSDataSource parsing functionality

        Tests:
            - CSV data is correctly parsed
            - Storm IDs are correctly extracted
            - Storm data is correctly filtered
        """
        # Test with sample data
        data_source = IBTrACSDataSource("test_url", SAMPLE_IBTRACS_CSV)

        # Check that data is parsed
        parsed_data = data_source.get_data()
        self.assertIsNotNone(parsed_data)
        self.assertEqual(len(parsed_data), 23)  # 23 rows in sample data

        # Check storm IDs
        storm_ids = data_source.get_storm_ids()
        self.assertEqual(len(storm_ids), 1)
        self.assertEqual(storm_ids[0], "2024178N09335")

        # Check storm data filtering
        storm_data = data_source.get_storm_data("2024178N09335")
        self.assertEqual(len(storm_data), 23)

        # Test with multi-storm data
        multi_data_source = IBTrACSDataSource("test_url", MULTI_STORM_IBTRACS_CSV)

        # Check storm IDs
        multi_storm_ids = multi_data_source.get_storm_ids()
        self.assertEqual(len(multi_storm_ids), 2)
        self.assertIn("2024178N09335", multi_storm_ids)
        self.assertIn("2024200N12345", multi_storm_ids)

        # Check storm data filtering
        beryl_data = multi_data_source.get_storm_data("2024178N09335")
        self.assertEqual(len(beryl_data), 4)

        chris_data = multi_data_source.get_storm_data("2024200N12345")
        self.assertEqual(len(chris_data), 4)

        # Test with landfall data
        landfall_data_source = IBTrACSDataSource("test_url", LANDFALL_IBTRACS_CSV)
        landfall_data = landfall_data_source.get_data()
        self.assertEqual(len(landfall_data), 4)

        print(landfall_data)

        # Check landfall flag
        for row in landfall_data:
            self.assertEqual(row.LANDFALL, "1")

    def test_invalid_data(self) -> None:
        """Test handling of invalid data

        Tests that:
            - Invalid CSV data is handled gracefully
            - Empty CSV data is handled gracefully
            - Missing fields are handled gracefully
        """
        # Test with invalid CSV data
        invalid_data_source = IBTrACSDataSource("test_url", INVALID_IBTRACS_CSV)
        parsed_data = invalid_data_source.get_data()
        print(parsed_data)
        self.assertEqual(len(parsed_data), 0)  # Should return empty list

        # Test with empty CSV data
        empty_data_source = IBTrACSDataSource("test_url", EMPTY_IBTRACS_CSV)
        empty_parsed_data = empty_data_source.get_data()
        print(empty_parsed_data)
        self.assertEqual(len(empty_parsed_data), 0)  # Should return empty list

        # Test transformer with empty data
        transformer = IBTrACSTransformer(empty_data_source, geocoder)
        items = list(transformer.make_items())
        self.assertEqual(len(items), 0)  # Should return empty list

        # Test with missing fields
        missing_fields_source = IBTrACSDataSource("test_url", MISSING_FIELDS_CSV)
        missing_fields_data = missing_fields_source.get_data()
        print(missing_fields_data)
        self.assertEqual(len(missing_fields_data), 1)  # Should parse the row

        # Check that missing fields don't cause errors
        storm_ids = missing_fields_source.get_storm_ids()
        self.assertEqual(len(storm_ids), 1)
        self.assertEqual(storm_ids[0], "2024178N09335")

    def test_event_item_properties(self) -> None:
        """Test that event items have the correct properties"""
        data_source = IBTrACSDataSource("test_url", SAMPLE_IBTRACS_CSV)
        transformer = IBTrACSTransformer(data_source, geocoder)

        # items = list(transformer.make_items())

        # Find the event item
        event_items = [item for item in transformer.make_items() if "event" in item.properties.get("roles", [])]
        print("the type of event items is", type(event_items))

        self.assertEqual(len(event_items), 1)

        event_item = event_items[0]

        # Check basic properties
        self.assertEqual(event_item.id, "2024178N09335")
        self.assertEqual(event_item.properties.get("title"), "Tropical Cyclone BERYL")
        self.assertIn("description", event_item.properties)
        self.assertIn("start_datetime", event_item.properties)
        self.assertIn("end_datetime", event_item.properties)

        # Check Monty extension properties
        monty_ext = MontyExtension.ext(event_item)
        self.assertIsNotNone(monty_ext)
        self.assertIn("MH0057", monty_ext.hazard_codes)
        self.assertIn("TC", monty_ext.hazard_codes)
        self.assertIsNotNone(monty_ext.correlation_id)
        self.assertTrue(monty_ext.correlation_id.startswith("20240626T000000"))

    def test_hazard_item_properties(self) -> None:
        """Test that hazard items have the correct properties"""
        data_source = IBTrACSDataSource("test_url", SAMPLE_IBTRACS_CSV)
        transformer = IBTrACSTransformer(data_source, geocoder)

        # Find hazard items
        hazard_items = [item for item in transformer.make_items() if "hazard" in item.properties.get("roles", [])]
        self.assertGreater(len(hazard_items), 0)

        # Check first hazard item
        hazard_item = hazard_items[0]

        # Check basic properties
        self.assertTrue(hazard_item.id.startswith("2024178N09335-hazard-"))
        self.assertIn("title", hazard_item.properties)
        self.assertIn("description", hazard_item.properties)
        self.assertIn("start_datetime", hazard_item.properties)
        self.assertIn("end_datetime", hazard_item.properties)

        # Check Monty extension properties
        monty_ext = MontyExtension.ext(hazard_item)
        self.assertIsNotNone(monty_ext)
        self.assertIn("nat-met-sto-tro", monty_ext.hazard_codes)
        self.assertIsNotNone(monty_ext.correlation_id)

        # Check hazard detail
        hazard_detail = monty_ext.hazard_detail
        self.assertIsNotNone(hazard_detail)
        self.assertEqual(hazard_detail.cluster, "nat-met-sto-tro")
        self.assertIsNotNone(hazard_detail.severity_value)
        self.assertEqual(hazard_detail.severity_unit, "knots")

    def test_item_links_and_assets(self) -> None:
        """Test that items have the correct links and assets"""
        data_source = IBTrACSDataSource("test_url", SAMPLE_IBTRACS_CSV)
        transformer = IBTrACSTransformer(data_source, geocoder)

        items = list(transformer.make_items())

        # Find event and hazard items
        event_items = [item for item in items if "event" in item.properties.get("roles", [])]
        hazard_items = [item for item in items if "hazard" in item.properties.get("roles", [])]

        # Check event item links and assets
        event_item = event_items[0]

        # Check assets
        self.assertIn("data", event_item.assets)
        self.assertIn("documentation", event_item.assets)

        data_asset = event_item.assets["data"]
        self.assertEqual(data_asset.media_type, "text/csv")
        self.assertIn("roles", data_asset.extra_fields)
        self.assertIn("data", data_asset.extra_fields["roles"])

        doc_asset = event_item.assets["documentation"]
        self.assertEqual(doc_asset.media_type, "text/html")
        self.assertIn("roles", doc_asset.extra_fields)
        self.assertIn("documentation", doc_asset.extra_fields["roles"])

        # Check links
        via_links = [link for link in event_item.links if link.rel == "via"]
        self.assertEqual(len(via_links), 1)

        # Check hazard item links and assets
        if hazard_items:
            hazard_item = hazard_items[0]

            # Check assets
            self.assertIn("data", hazard_item.assets)
            self.assertIn("documentation", hazard_item.assets)

            # Check links
            related_links = [link for link in hazard_item.links if link.rel == "related"]
            self.assertGreaterEqual(len(related_links), 1)

            # Check that related link points to event
            event_link = next((link for link in related_links if "event" in link.extra_fields.get("roles", [])), None)
            self.assertIsNotNone(event_link)
            self.assertIn("source", event_link.extra_fields.get("roles", []))

    def test_helper_methods(self) -> None:
        """Test helper methods in the IBTrACSTransformer class"""
        data_source = IBTrACSDataSource("test_url", SAMPLE_IBTRACS_CSV)
        transformer = IBTrACSTransformer(data_source, geocoder)

        # Test collection methods
        event_collection = transformer.get_event_collection()
        self.assertEqual(event_collection.id, "ibtracs-events")

        hazard_collection = transformer.get_hazard_collection()
        self.assertEqual(hazard_collection.id, "ibtracs-hazards")

        # Test country detection from track
        # Create a simple LineString for testing
        line = LineString([(-80.0, 25.0), (-75.0, 30.0), (-70.0, 35.0)])
        countries = transformer._get_countries_from_track(line)

        # Should at least include XYZ (international waters) as a fallback
        self.assertIn("XYZ", countries)
