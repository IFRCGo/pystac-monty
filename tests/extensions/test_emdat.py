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
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import DataType, File, GenericDataSource, Memory
from pystac_monty.sources.emdat import EMDATDataSource, EMDATTransformer
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator
from tests.utils.test_hazard_taxonomy import assert_hazard_code_dict_valid
from tests.utils.test_utils import assert_processing_extension_fields, request_for_schema, validate_correlation_id

CURRENT_SCHEMA_URI = "https://ifrcgo.org/monty-stac-extension/v1.3.0/schema.json"
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
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

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
            assert_processing_extension_fields(item)

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
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

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
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

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
            assert_processing_extension_fields(item)

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
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

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
            assert_processing_extension_fields(item)

            # Check item type
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_item = item
            elif monty_item_ext.is_source_hazard():
                source_hazard_item = item

        # Verify required items were created
        self.assertIsNotNone(source_event_item)
        self.assertIsNotNone(source_hazard_item)

        # Verify Correlation ID
        hazard_profiles = MontyHazardProfiles()
        event_item_hazard_code = hazard_profiles.get_canonical_hazard_codes(source_event_item)[0].upper()
        validate_correlation_id(source_event_item.properties.get("monty:corr_id"), event_item_hazard_code)
        hazard_item_hazard_code = hazard_profiles.get_canonical_hazard_codes(source_hazard_item)[0].upper()
        validate_correlation_id(source_hazard_item.properties.get("monty:corr_id"), hazard_item_hazard_code)

    @parameterized.expand(load_scenarios(DATA_FILE))
    @pytest.mark.vcr()
    def test_transformer_item_links(self, transformer: EMDATTransformer) -> None:
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

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
            assert_processing_extension_fields(item)

            # Check item type
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_item = item
            elif monty_item_ext.is_source_hazard():
                source_hazard_item = item

        # Verify required items were created
        self.assertIsNotNone(source_event_item)
        self.assertIsNotNone(source_hazard_item)

        # Verify Related links exists
        event_item_related_items = source_event_item.get_links(rel="related")
        hazard_item_related_items = source_hazard_item.get_links(rel="related")
        event_item_self_link = source_event_item.self_href
        hazard_item_self_link = source_hazard_item.self_href

        self.assertTrue(len(event_item_related_items) > 0)
        self.assertTrue(len(hazard_item_related_items) > 0)
        assert all(link.href is not None and link.href != event_item_self_link for link in event_item_related_items)
        assert all(link.href is not None and link.href != hazard_item_self_link for link in hazard_item_related_items)
        assert event_item_self_link in [item.href for item in hazard_item_related_items]
        assert hazard_item_self_link in [item.href for item in event_item_related_items]

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_event_item_uses_all_codes(self, transformer: EMDATTransformer) -> None:
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

        for item in transformer.get_stac_items():
            # write pretty json in a temporary folder
            item_path = get_data_file(f"temp/emdat/{item.id}.json")
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            assert_processing_extension_fields(item)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event() and monty_item_ext.hazard_codes:
                # Should contain only the first code (UNDRR-ISC 2025)
                assert len(monty_item_ext.hazard_codes) == 3

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_hazard_item_uses_2025_code_only(self, transformer: EMDATTransformer) -> None:
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

        for item in transformer.get_stac_items():
            # write pretty json in a temporary folder
            item_path = get_data_file(f"temp/emdat/{item.id}.json")
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            assert_processing_extension_fields(item)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_hazard() and monty_item_ext.hazard_codes:
                # Should contain only the first code (UNDRR-ISC 2025)
                assert len(monty_item_ext.hazard_codes) == 1


class TestEmdatHazardCodeMappingDrift(unittest.TestCase):
    """Regression tests to counter hazard code mapping drifts."""

    def setUp(self) -> None:
        emdat_data_source = EMDATDataSource(
            data=GenericDataSource(
                source_url="www.test.com",
                input_data=Memory(content=pd.DataFrame(), data_type=DataType.MEMORY),
            )
        )
        self.transformer = EMDATTransformer(emdat_data_source, MockGeocoder())

    def test_volcanic_general_activity_maps_to_gh0201(self) -> None:
        """'nat-geo-vol-vol' (volcanic activity general) belongs on GH0201."""
        assert self.transformer.map_emdat_to_hazard_codes("nat-geo-vol-vol") == ["GH0201", "nat-geo-vol-vol", "VO"]

    def test_technological_codes_use_hip_2025_standard(self) -> None:
        """Technological hazards must use HIP 2025 TL codes, not the 2020-era ids."""
        expected_undrr_code = {
            "tec-ind-fir-fir": "TL0305",
            "tec-ind-rad-rad": "TL0601",
            "tec-mis-col-col": "TL0201",
            "tec-ind-ind-ind": "TL0309",
            "tec-ind-exp-exp": "TL0304",
            "tec-ind-che-che": "TL0301",
            "tec-tra-air-air": "TL0401",
            "tec-tra-wat-wat": "TL0403",
            "tec-tra-rai-rai": "TL0404",
            "tec-tra-roa-roa": "TL0405",
        }
        for classification_key, undrr_code in expected_undrr_code.items():
            codes = self.transformer.map_emdat_to_hazard_codes(classification_key)
            assert codes[0] == undrr_code, f"{classification_key} should map to {undrr_code}, got {codes}"

    def test_epidemic_disease_uses_ep_glide_code(self) -> None:
        """'nat-bio-epi-dis' (general infectious disease) must use GLIDE 'EP'."""
        assert self.transformer.map_emdat_to_hazard_codes("nat-bio-epi-dis") == ["BI0101", "nat-bio-epi-dis", "EP"]

    def test_passthrough_entries_resolve_full_triplets(self) -> None:
        expected = {
            "nat-cli-wil-for": ["EN0205", "nat-cli-wil-for", "WF"],
            "nat-cli-wil-lan": ["EN0205", "nat-cli-wil-lan", "WF"],
            # "nat-geo-env-coa": ["GH0405", "nat-geo-env-coa", "OT"],
            # "tec-mis-exp-exp": ["TL0304", "tec-mis-exp-exp", "AC"],
            "nat-hyd-mmw-lan": ["GH0304", "nat-hyd-mmw-lan", "LS"],
            "tec-mis-fir-fir": ["TL0305", "tec-mis-fir-fir", "FR"],
            # "tec-ind-col-col": ["TL0201", "tec-ind-col-col", "AC"],
            "nat-hyd-mmw-ava": ["MH0801", "nat-hyd-mmw-ava", "AV"],
        }
        for classification_key, codes in expected.items():
            assert self.transformer.map_emdat_to_hazard_codes(classification_key) == codes

    def test_mapped_codes_are_taxonomy_valid(self) -> None:
        """Every UNDRR/GLIDE/EM-DAT code produced by the mapping must exist in taxonomy.md."""
        classification_keys = [
            "nat-geo-vol-vol",
            "nat-bio-epi-dis",
            "tec-ind-fir-fir",
            "tec-ind-rad-rad",
            "tec-mis-col-col",
            "tec-ind-ind-ind",
            "tec-ind-exp-exp",
            "tec-ind-che-che",
            "tec-tra-air-air",
            "tec-tra-wat-wat",
            "tec-tra-rai-rai",
            "tec-tra-roa-roa",
            "nat-cli-wil-for",
            "nat-cli-wil-lan",
            "nat-geo-env-coa",
            "tec-mis-exp-exp",
            "nat-hyd-mmw-lan",
            "tec-mis-fir-fir",
            "tec-ind-col-col",
            "nat-hyd-mmw-ava",
        ]
        hazard_codes = {key: self.transformer.map_emdat_to_hazard_codes(key) for key in classification_keys}
        assert_hazard_code_dict_valid(hazard_codes, label="EMDAT_HAZARD_CODES")
