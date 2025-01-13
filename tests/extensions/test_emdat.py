"""Tests for pystac.tests.extensions.monty EM-DAT source"""

import json
import unittest
from os import makedirs

import pandas as pd
import pytest
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import GAULGeocoder, MockGeocoder
from pystac_monty.sources.emdat import EMDATDataSource, EMDATTransformer
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator

CURRENT_SCHEMA_URI = "https://ifrcgo.github.io/monty/v0.1.0/schema.json"
CURRENT_SCHEMA_MAPURL = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/json-schema/schema.json"


def load_scenarios(
    scenarios: list[tuple[str, str]],
) -> list[EMDATTransformer]:
    """Load test scenarios for EM-DAT transformation testing.

    Args:
        scenarios: List of tuples containing scenario name and Excel file path

    Returns:
        List of EMDATTransformer instances initialized with test data
    """
    transformers = []
    for scenario in scenarios:
        # Read Excel file using pandas
        df = pd.read_excel(scenario[1])
        emdat_data_source = EMDATDataSource(scenario[1], df)
        geocoder = MockGeocoder()
        transformers.append(EMDATTransformer(emdat_data_source, geocoder))
    return transformers


spain_flood = (
    "spain_flood",
    "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/EMDAT/model/sources/EM-DAT/public_emdat_custom_request_2025-01-13_4cf1ccf1-9f6e-41a3-9aec-0a19903febae.xlsx",
)


class EMDATTest(unittest.TestCase):
    """Test suite for EM-DAT transformation functionality"""

    scenarios = [spain_flood]

    def setUp(self) -> None:
        """Set up test environment"""
        super().setUp()
        self.validator = CustomValidator()
        # Create temporary folder for test outputs
        makedirs(get_data_file("temp/emdat"), exist_ok=True)

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_transformer(self, transformer: EMDATTransformer) -> None:
        """Test EM-DAT transformation to STAC items

        Args:
            transformer: EMDATTransformer instance to test

        Tests:
            - Items are created
            - Items validate against schema
            - Source event and hazard items are present
            - Items can be serialized to JSON
        """
        items = transformer.make_items()
        self.assertTrue(len(items) > 0)

        source_event_item = None
        source_hazard_item = None

        for item in items:
            # Write pretty JSON in temporary folder for manual inspection
            item_path = get_data_file(f"temp/emdat/{item.id}.json")
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

        # Verify required items were created
        self.assertIsNotNone(source_event_item)
        self.assertIsNotNone(source_hazard_item)

    def test_excel_loading(self) -> None:
        """Test Excel file loading functionality

        Tests:
            - Excel files can be loaded
            - Required columns are present
            - Data types are correct
        """
        for scenario in self.scenarios:
            df = pd.read_excel(scenario[1])

            # Check required columns exist
            required_columns = ["DisNo.", "ISO", "Start Year", "Disaster Type", "Admin Units"]
            for col in required_columns:
                self.assertIn(col, df.columns)

            # Check data types
            self.assertTrue(pd.api.types.is_integer_dtype(df["Start Year"]))
            self.assertTrue(pd.api.types.is_string_dtype(df["ISO"]))
