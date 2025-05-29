"""Tests for pystac.tests.extensions.monty EM-DAT source"""

import json
import tempfile
import unittest
from os import makedirs
from typing import Union

import pandas as pd
import pytest
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import MockGeocoder
from pystac_monty.sources.common import DataType, File, GenericDataSource, Memory
from pystac_monty.sources.emdat import EMDATDataSource, EMDATTransformer
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator

CURRENT_SCHEMA_URI = "https://ifrcgo.github.io/monty/v0.1.0/schema.json"
CURRENT_SCHEMA_MAPURL = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/json-schema/schema.json"

json_mock_data = {
    "data": {
        "api_version": "v1.1.1",
        "public_emdat": {
            "total_available": 595,
            "info": {
                "timestamp": "2025-02-27T08:03:24Z",
                "filters": {"from": 2024, "include_hist": True, "to": 2025},
                "cursor": {"offset": 590},
                "version": "2025-02-26",
            },
            "data": [
                {
                    "disno": "2025-0101-USA",
                    "classif_key": "tec-tra-air-air",
                    "group": "Technological",
                    "subgroup": "Transport",
                    "type": "Air",
                    "subtype": "Air",
                    "external_ids": None,
                    "name": None,
                    "iso": "USA",
                    "country": "United States of America",
                    "subregion": "Northern America",
                    "region": "Americas",
                    "location": "Alaska",
                    "origin": None,
                    "associated_types": None,
                    "ofda_response": False,
                    "appeal": False,
                    "declaration": False,
                    "aid_contribution": None,
                    "magnitude": None,
                    "magnitude_scale": None,
                    "latitude": None,
                    "longitude": None,
                    "river_basin": None,
                    "start_year": 2025,
                    "start_month": 2,
                    "start_day": 6,
                    "end_year": 2025,
                    "end_month": 2,
                    "end_day": 6,
                    "total_deaths": 10,
                    "no_injured": None,
                    "no_affected": None,
                    "no_homeless": None,
                    "total_affected": None,
                    "reconstr_dam": None,
                    "reconstr_dam_adj": None,
                    "insur_dam": None,
                    "insur_dam_adj": None,
                    "total_dam": None,
                    "total_dam_adj": None,
                    "cpi": None,
                    "admin_units": None,
                    "entry_date": "2025-02-17",
                    "last_update": "2025-02-19",
                },
                {
                    "disno": "2025-0102-NGA",
                    "classif_key": "tec-mis-fir-fir",
                    "group": "Technological",
                    "subgroup": "Miscellaneous accident",
                    "type": "Fire (Miscellaneous)",
                    "subtype": "Fire (Miscellaneous)",
                    "external_ids": None,
                    "name": "School dormitory",
                    "iso": "NGA",
                    "country": "Nigeria",
                    "subregion": "Sub-Saharan Africa",
                    "region": "Africa",
                    "location": "Kaura Namoda (Zamfara state)",
                    "origin": None,
                    "associated_types": None,
                    "ofda_response": False,
                    "appeal": False,
                    "declaration": False,
                    "aid_contribution": None,
                    "magnitude": None,
                    "magnitude_scale": None,
                    "latitude": None,
                    "longitude": None,
                    "river_basin": None,
                    "start_year": 2025,
                    "start_month": 2,
                    "start_day": 4,
                    "end_year": 2025,
                    "end_month": 2,
                    "end_day": 5,
                    "total_deaths": 17,
                    "no_injured": 17,
                    "no_affected": None,
                    "no_homeless": None,
                    "total_affected": 17,
                    "reconstr_dam": None,
                    "reconstr_dam_adj": None,
                    "insur_dam": None,
                    "insur_dam_adj": None,
                    "total_dam": None,
                    "total_dam_adj": None,
                    "cpi": None,
                    "admin_units": None,
                    "entry_date": "2025-02-17",
                    "last_update": "2025-02-19",
                },
                {
                    "disno": "2025-0103-KEN",
                    "classif_key": "tec-ind-col-col",
                    "group": "Technological",
                    "subgroup": "Industrial accident",
                    "type": "Collapse (Industrial)",
                    "subtype": "Collapse (Industrial)",
                    "external_ids": None,
                    "name": "Gold mine",
                    "iso": "KEN",
                    "country": "Kenya",
                    "subregion": "Sub-Saharan Africa",
                    "region": "Africa",
                    "location": "Kakamega county",
                    "origin": None,
                    "associated_types": None,
                    "ofda_response": False,
                    "appeal": False,
                    "declaration": False,
                    "aid_contribution": None,
                    "magnitude": None,
                    "magnitude_scale": "m3",
                    "latitude": None,
                    "longitude": None,
                    "river_basin": None,
                    "start_year": 2025,
                    "start_month": 2,
                    "start_day": 3,
                    "end_year": 2025,
                    "end_month": 2,
                    "end_day": 3,
                    "total_deaths": 12,
                    "no_injured": None,
                    "no_affected": 8,
                    "no_homeless": None,
                    "total_affected": 8,
                    "reconstr_dam": None,
                    "reconstr_dam_adj": None,
                    "insur_dam": None,
                    "insur_dam_adj": None,
                    "total_dam": None,
                    "total_dam_adj": None,
                    "cpi": None,
                    "admin_units": [{"adm1_code": 10, "adm1_name": "adm_name_1"}, {"adm2_code": 12, "adm2_name": "adm_name_2"}],
                    "entry_date": "2025-02-17",
                    "last_update": "2025-02-19",
                },
            ],
        },
    }
}


def save_data_to_tmp_file(data):
    tmpfile = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
    data = json.dumps(data).encode("utf-8")
    tmpfile.write(data)
    tmpfile.close()
    return tmpfile


DATA_FILE = save_data_to_tmp_file(json_mock_data)


def load_scenarios(
    scenarios: Union[list[tuple[str, str]], dict],
) -> list[EMDATTransformer]:
    """Load test scenarios for EM-DAT transformation testing.

    Args:
        scenarios: List of tuples containing scenario name and Excel file path

    Returns:
        List of EMDATTransformer instances initialized with test data
    """
    transformers = []
    if isinstance(scenarios, tempfile._TemporaryFileWrapper):
        emdat_data_source = EMDATDataSource(
            data=GenericDataSource(
                source_url="www.test.com",
                input_data=File(path=DATA_FILE.name, data_type=DataType.FILE),
            )
        )
        geocoder = MockGeocoder()
        transformers.append(EMDATTransformer(emdat_data_source, geocoder))

    elif isinstance(scenarios, dict):
        emdat_data_source = EMDATDataSource(
            data=GenericDataSource(
                source_url="www.test.com",
                input_data=Memory(content=scenarios, data_type=DataType.MEMORY),
            )
        )
        geocoder = MockGeocoder()
        transformers.append(EMDATTransformer(emdat_data_source, geocoder))
    else:
        for scenario in scenarios:
            # Read Excel file using pandas
            df = pd.read_excel(scenario[1])
            emdat_data_source = EMDATDataSource(
                data=GenericDataSource(
                    source_url=scenario[1],
                    input_data=Memory(content=df, data_type=DataType.MEMORY),
                )
            )
            geocoder = MockGeocoder()
            transformers.append(EMDATTransformer(emdat_data_source, geocoder))
    return transformers


spain_flood = (
    "spain_flood",
    (
        "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/EMDAT/model/sources/"
        "EM-DAT/public_emdat_custom_request_2025-01-13_4cf1ccf1-9f6e-41a3-9aec-0a19903febae.xlsx"
    ),
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

    @parameterized.expand(load_scenarios(json_mock_data))
    @pytest.mark.vcr()
    def test_transformer_with_json_data(self, transformer: EMDATTransformer) -> None:
        items = transformer.make_items()
        self.assertTrue(len(items) > 0)

        source_event_item = None
        source_hazard_item = None

        for item in items:
            # Write pretty JSON in temporary folder for manual inspection
            item_path = get_data_file(f"temp/emdat/{item.id}.json")
            with open(item_path, "w", encoding="utf-8") as f:
                json.dump(item.to_dict(), f, indent=2)

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

    @parameterized.expand(load_scenarios(DATA_FILE))
    @pytest.mark.vcr()
    def test_transformer_with_file_data(self, transformer: EMDATTransformer) -> None:
        items = transformer.make_items()
        self.assertTrue(len(items) > 0)

        source_event_item = None
        source_hazard_item = None

        for item in items:
            # Write pretty JSON in temporary folder for manual inspection
            item_path = get_data_file(f"temp/emdat/{item.id}.json")
            with open(item_path, "w", encoding="utf-8") as f:
                json.dump(item.to_dict(), f, indent=2)

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
