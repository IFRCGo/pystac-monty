"""Tests for pystac.tests.extensions.monty USGS source"""

import json
import unittest
from os import makedirs

import pytest
import requests
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import WorldAdministrativeBoundariesGeocoder
from pystac_monty.sources.usgs import USGSDataSource, USGSTransformer
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator

CURRENT_SCHEMA_URI = "https://ifrcgo.github.io/monty/v0.1.0/schema.json"
CURRENT_SCHEMA_MAPURL = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/json-schema/schema.json"

geocoder = WorldAdministrativeBoundariesGeocoder(get_data_file("world-administrative-boundaries.fgb"), 0.1)


def load_scenarios(scenarios: list[tuple[str, str, str]]) -> list[USGSTransformer]:
    """Load test scenarios for USGS transformation testing.

    Args:
        scenarios: List of tuples containing (name, event_url, losses_url)

    Returns:
        List of USGSTransformer instances initialized with test data
    """
    transformers = []
    for scenario in scenarios:
        name, event_url, losses_url = scenario

        # Get event data
        event_response = requests.get(event_url)
        event_data = event_response.text

        # Get losses data if available
        losses_data = None
        if losses_url:
            losses_response = requests.get(losses_url)
            losses_data = json.dumps([losses_response.json()])

        # Create data source and transformer
        data_source = USGSDataSource(event_url, event_data, losses_data)
        transformers.append(USGSTransformer(data_source, geocoder))

    return transformers


# Test scenarios containing both event data and losses data
tibetan_plateau_eq = (
    "tibetan_plateau_eq",  # Scenario name
    "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/usgs/model/sources/USGS/us6000pi9w.geojson",  # Event URL
    "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/usgs/model/sources/USGS/us6000pi9w-losses.json",  # Losses URL
)


class USGSTest(unittest.TestCase):
    """Test suite for USGS transformation functionality"""

    scenarios = [tibetan_plateau_eq]

    def setUp(self) -> None:
        """Set up test environment"""
        super().setUp()
        self.validator = CustomValidator()
        # Create temporary folder for test outputs
        makedirs(get_data_file("temp/usgs"), exist_ok=True)

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_transformer(self, transformer: USGSTransformer) -> None:
        """Test USGS transformation to STAC items

        Args:
            transformer: USGSTransformer instance to test

        Tests:
            - Items are created
            - Items validate against schema
            - Source event, hazard and impact items are present
            - Items can be serialized to JSON
        """
        #items = transformer.make_items()
        #self.assertTrue(len(items) > 0)

        # Track items we find
        source_event_item = None
        source_hazard_item = None
        impact_items = []

        for item in transformer.get_stac_items():
            # Write pretty JSON in temporary folder for manual inspection
            item_path = get_data_file(f"temp/usgs/{item.id}.json")
            with open(item_path, "w", encoding="utf-8") as f:
                json.dump(item.to_dict(), f, indent=2, ensure_ascii=False)

            # Validate item against schema
            item.validate(validator=self.validator)

            # Check item type
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_item = item
            elif monty_item_ext.is_source_hazard():
                source_hazard_item = item
            elif monty_item_ext.is_source_impact():
                impact_items.append(item)

        # Verify required items were created
        self.assertIsNotNone(source_event_item)
        self.assertIsNotNone(source_hazard_item)

        # Since we provided losses data, we should have 3 impact items
        self.assertEqual(len(impact_items), 3)

        # Validate specific fields from earthquake extension
        self.assertIn("eq:magnitude", source_event_item.properties)
        self.assertIn("eq:depth", source_event_item.properties)
        self.assertIn("eq:magnitude_type", source_event_item.properties)

    def test_no_losses_data(self) -> None:
        """Test transformer behavior when no losses data is provided

        Tests that:
            - Only event and hazard items are created
            - No impact items are created
            - Items still validate
        """
        event_data = requests.get(tibetan_plateau_eq[1]).text
        data_source = USGSDataSource(tibetan_plateau_eq[1], event_data)
        transformer = USGSTransformer(data_source, geocoder)

        #items = transformer.make_items()

        # Should only have event and hazard items
        #self.assertEqual(len(items), 2)

        found_event = False
        found_hazard = False
        for item in transformer.get_stac_items():
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                found_event = True
            elif monty_item_ext.is_source_hazard():
                found_hazard = True
            else:
                self.fail("Unexpected item type found")

            item.validate(validator=self.validator)

        self.assertTrue(found_event)
        self.assertTrue(found_hazard)

    # def test_invalid_event_data(self) -> None:
    #     """Test handling of invalid event data

    #     Tests that appropriate errors are raised for:
    #         - Missing required fields
    #         - Invalid field types
    #         - Malformed JSON
    #     """
    #     # Test missing required fields
    #     invalid_data = "{}"
    #     data_source = USGSDataSource("test_url", invalid_data)
    #     transformer = USGSTransformer(data_source, geocoder)

    #     with self.assertRaises(KeyError):
    #         transformer.make_items()

    #     # Test invalid JSON
    #     invalid_json = "{"
    #     with self.assertRaises(json.JSONDecodeError):
    #         USGSDataSource("test_url", invalid_json)

    # def test_invalid_losses_data(self) -> None:
    #     """Test handling of invalid losses data

    #     Tests that:
    #         - Invalid losses data doesn't prevent event/hazard creation
    #         - Appropriate errors are raised
    #         - Valid items are still created
    #     """
    #     # Get valid event data
    #     event_data = requests.get(tibetan_plateau_eq[1]).text

    #     # Test with invalid losses JSON
    #     invalid_losses = "{"
    #     with self.assertRaises(json.JSONDecodeError):
    #         USGSDataSource("test_url", event_data, invalid_losses)

    #     # Test with empty losses data - should still create event and hazard
    #     empty_losses = "{}"
    #     data_source = USGSDataSource("test_url", event_data, empty_losses)
    #     transformer = USGSTransformer(data_source, geocoder)
    #     items = transformer.make_items()

    #     self.assertEqual(len(items), 2)  # Should still get event and hazard
