"""Tests for pystac.tests.extensions.monty"""

import json
import tempfile
from os import makedirs
from typing import List, Union
from unittest import TestCase

import pytest
import requests
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import MockGeocoder
from pystac_monty.sources.common import DataType, File, GenericDataSource, Memory
from pystac_monty.sources.ifrc_events import IFRCEventDataSource, IFRCEventTransformer
from pystac_monty.sources.utils import save_json_data_into_tmp_file
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator


def request_and_save_ifrc_tmp_file(url):
    response = requests.get(url)
    response.raise_for_status()  # Optional: raise error for bad responses
    results = json.loads(response.content)["results"]
    return save_json_data_into_tmp_file(results)


nepal_earthquake_data = ("karnali_earthquake", "https://goadmin-stage.ifrc.org/api/v2/event/?dtype=2&appeal_type=1&id=6732")

morocco_earthquake_data = ("morocco_earthquake", "https://goadmin-stage.ifrc.org/api/v2/event/?dtype=2&appeal_type=1&id=6646")

nepal_earthquake_data_2 = (
    "karnali_earthquake",
    request_and_save_ifrc_tmp_file("https://goadmin-stage.ifrc.org/api/v2/event/?dtype=2&appeal_type=1&id=6732"),
)

morocco_earthquake_data_2 = (
    "morocco_earthquake",
    request_and_save_ifrc_tmp_file("https://goadmin-stage.ifrc.org/api/v2/event/?dtype=2&appeal_type=1&id=6646"),
)


def load_scenarios(
    scenarios: Union[list[tuple[str, str]], tempfile._TemporaryFileWrapper],
) -> List[IFRCEventTransformer]:
    transformers: List[IFRCEventTransformer] = []

    for scenario in scenarios:
        geocoder = MockGeocoder()
        if isinstance(scenario[1], tempfile._TemporaryFileWrapper):
            data_source = IFRCEventDataSource(
                data=GenericDataSource(
                    source_url="www.test.com",
                    input_data=File(path=scenario[1].name, data_type=DataType.FILE),
                )
            )
        else:
            response = requests.get(scenario[1])
            data = json.loads(response.content)
            geocoder = MockGeocoder()
            data_source = IFRCEventDataSource(
                data=GenericDataSource(
                    source_url="www.test.com",
                    input_data=Memory(content=data["results"], data_type=DataType.MEMORY),
                )
            )
        transformer = IFRCEventTransformer(data_source, geocoder)
        transformers.append(transformer)

    return transformers


class IfrcEventsTest(TestCase):
    scenarios = [nepal_earthquake_data, morocco_earthquake_data]
    scenarios_2 = [nepal_earthquake_data_2, morocco_earthquake_data_2]

    def setUp(self) -> None:
        super().setUp()
        self.validator = CustomValidator()

        # Create temporary folder for test outputs
        makedirs(get_data_file("temp/ifrc_events"), exist_ok=True)

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_transformer_with_memory_data(self, transformer: IFRCEventTransformer) -> None:
        """Test IFRC transformation to STAC items

        Args:
            transformer: IfrcEventTransformer instance to test

        Tests:
            - Items are created
            - Items validate against schema
            - Source event and hazard items are present
            - Items can be serialized to JSON
        """
        items = transformer.make_items()
        self.assertTrue(len(items) > 0)

        source_event_item = None
        source_impact_item = None

        for item in items:
            # Write pretty JSON in temporary folder for manual inspection
            item_path = get_data_file(f"temp/ifrc_events/{item.id}.json")
            with open(item_path, "w", encoding="utf-8") as f:
                json.dump(item.to_dict(), f, indent=2, ensure_ascii=False)

            # Validate item against schema
            item.validate(validator=self.validator)

            # Check item type
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_item = item
            elif monty_item_ext.is_source_impact():
                source_impact_item = item

        # Verify required items were created
        self.assertIsNotNone(source_event_item)
        self.assertIsNotNone(source_impact_item)

    @parameterized.expand(load_scenarios(scenarios_2))
    @pytest.mark.vcr()
    def test_transformer_with_file_data(self, transformer: IFRCEventTransformer) -> None:
        """Test EM-DAT transformation to STAC items

        Args:
            transformer: IfrcEventTransformer instance to test

        Tests:
            - Items are created
            - Items validate against schema
            - Source event and hazard items are present
            - Items can be serialized to JSON
        """
        items = transformer.make_items()
        self.assertTrue(len(items) > 0)

        source_event_item = None
        source_impact_item = None

        for item in items:
            # Write pretty JSON in temporary folder for manual inspection
            item_path = get_data_file(f"temp/ifrc_events/{item.id}.json")
            with open(item_path, "w", encoding="utf-8") as f:
                json.dump(item.to_dict(), f, indent=2, ensure_ascii=False)

            # Validate item against schema
            item.validate(validator=self.validator)

            # Check item type
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_item = item
            elif monty_item_ext.is_source_impact():
                source_impact_item = item

        # Verify required items were created
        self.assertIsNotNone(source_event_item)
        self.assertIsNotNone(source_impact_item)

    @parameterized.expand(load_scenarios(scenarios))
    def test_no_old_2020_codes(self, transformer: IFRCEventTransformer):
        # Verify earthquake doesn't return old codes
        eq_codes = transformer.map_ifrc_to_hazard_codes("Earthquake")
        assert "GH0001" not in eq_codes
        assert "GH0002" not in eq_codes
        assert "GH0003" not in eq_codes

        # Verify cyclone doesn't return old codes
        cy_codes = transformer.map_ifrc_to_hazard_codes("Cyclone")
        assert "MH0030" not in cy_codes
        assert "MH0031" not in cy_codes

        # Verify tsunami uses new code
        ts_codes = transformer.map_ifrc_to_hazard_codes("Tsunami")
        assert "GH0006" not in ts_codes  # Old geological code
        assert "MH0705" in ts_codes  # New meteorological code

    @parameterized.expand(load_scenarios(scenarios))
    def test_ifrc_hazard_codes_2025(self, transformer: IFRCEventTransformer):
        # Test consolidated codes
        assert transformer.map_ifrc_to_hazard_codes("Earthquake") == ["GH0101", "nat-geo-ear-gro", "EQ"]
        assert transformer.map_ifrc_to_hazard_codes("Cyclone") == ["MH0306", "nat-met-sto-tro", "TC"]

        # Test reclassified tsunami
        assert transformer.map_ifrc_to_hazard_codes("Tsunami") == ["MH0705", "nat-geo-ear-tsu", "TS"]

        # Test other disaster types
        assert transformer.map_ifrc_to_hazard_codes("Flood") == ["MH0600", "nat-hyd-flo-flo", "FL"]
        assert transformer.map_ifrc_to_hazard_codes("Flash Flood") == ["MH0603", "nat-hyd-flo-fla", "FF"]
        assert transformer.map_ifrc_to_hazard_codes("Volcanic Eruption") == ["GH0201", "nat-geo-vol-vol", "VO"]
        assert transformer.map_ifrc_to_hazard_codes("Drought") == ["MH0401", "nat-cli-dro-dro", "DR"]
        assert transformer.map_ifrc_to_hazard_codes("Heat Wave") == ["MH0501", "nat-met-ext-hea", "HT"]
        assert transformer.map_ifrc_to_hazard_codes("Cold Wave") == ["MH0502", "nat-met-ext-col", "CW"]
        assert transformer.map_ifrc_to_hazard_codes("Landslide") == ["GH0300", "nat-geo-mmd-lan", "LS"]
        assert transformer.map_ifrc_to_hazard_codes("Storm Surge") == ["MH0703", "nat-met-sto-sur", "SS"]
        assert transformer.map_ifrc_to_hazard_codes("Fire") == ["TL0305", "tec-ind-fir-fir", "FR"]
        assert transformer.map_ifrc_to_hazard_codes("Epidemic") == ["BI0101", "nat-bio-epi-dis", "EP"]

    @parameterized.expand(load_scenarios(scenarios))
    def test_all_disaster_types_return_three_codes(self, transformer: IFRCEventTransformer):
        ifrc_disaster_types = [
            "Earthquake",
            "Cyclone",
            "Volcanic Eruption",
            "Tsunami",
            "Flood",
            "Cold Wave",
            "Fire",
            "Heat Wave",
            "Drought",
            "Storm Surge",
            "Landslide",
            "Flash Flood",
            "Epidemic",
        ]

        for disaster_type in ifrc_disaster_types:
            codes = transformer.map_ifrc_to_hazard_codes(disaster_type)
            assert len(codes) == 3, f"{disaster_type} should return exactly 3 codes"
            # First should be 2025 format
            assert codes[0].startswith(("MH", "GH", "BI", "TL", "EN"))

    @parameterized.expand(load_scenarios(scenarios))
    def test_ifrc_event_item_has_all_codes(self, transformer: IFRCEventTransformer):
        # Create mock IFRC event with earthquake
        items = transformer.make_items()
        event_item = items[0]

        monty = MontyExtension.ext(event_item)
        assert monty.hazard_codes == ["GH0101", "nat-geo-ear-gro", "EQ"]

    @parameterized.expand(load_scenarios([scenarios[0]]))
    def test_ifrc_event_item_keywords(self, transformer: IFRCEventTransformer):
        items = transformer.make_items()
        event_item = items[0]

        hazard_keywords = transformer.hazard_profiles.get_keywords(MontyExtension.ext(event_item).hazard_codes)
        country_keywords = ["Nepal"]

        expected_keywords = set(hazard_keywords + country_keywords)
        assert set(event_item.properties["keywords"]) == expected_keywords
