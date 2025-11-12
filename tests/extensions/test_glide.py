"""Tests for pystac.tests.extensions.monty"""

import json
import tempfile
import unittest
from os import makedirs
from typing import Union

import pytest
import requests
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import MockGeocoder
from pystac_monty.sources.common import DataType, File, GenericDataSource, Memory
from pystac_monty.sources.glide import GlideDataSource, GlideTransformer
from pystac_monty.sources.utils import save_json_data_into_tmp_file
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator

CURRENT_SCHEMA_URI = "https://ifrcgo.github.io/monty/v1.1.0/schema.json"
CURRENT_SCHEMA_MAPURL = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/json-schema/schema.json"

json_mock_data = {
    "glideset": [
        {
            "comments": "Glide test comments",
            "year": 2024,
            "docid": 23388,
            "latitude": 38.6013316868745,
            "homeless": 0,
            "source": "GDACS",
            "idsource": "",
            "killed": 0,
            "affected": 0,
            "duration": 8,
            "number": "2024-000199",
            "injured": 0,
            "month": 10,
            "geocode": "ESP",
            "location": "Spain",
            "magnitude": "0",
            "time": "",
            "id": "",
            "event": "FL",
            "day": 27,
            "status": "A",
            "longitude": -3.41102534556838,
        }
    ]
}

DATA_FILE = save_json_data_into_tmp_file(json_mock_data)


def load_scenarios(
    scenarios: Union[list[tuple[str, str]], tempfile._TemporaryFileWrapper],
) -> list[GlideTransformer]:
    transformers = []
    if isinstance(scenarios, tempfile._TemporaryFileWrapper):
        glide_data_source = GlideDataSource(
            data=GenericDataSource(
                source_url="https://www.glidenumber.net/glide/jsonglideset.jsp?level1=ESP&fromyear=2024&toyear=2024&events=FL&number=2024-000199",
                input_data=File(path=DATA_FILE.name, data_type=DataType.FILE),
            )
        )
        geocoder = MockGeocoder()
        transformers.append(GlideTransformer(glide_data_source, geocoder))
    else:
        for scenario in scenarios:
            response = requests.get(scenario[1])
            data = json.loads(response.content)
            glide_data_source = GlideDataSource(
                data=GenericDataSource(source_url=scenario[1], input_data=Memory(content=data, data_type=DataType.MEMORY))
            )
            geocoder = MockGeocoder()
            transformers.append(GlideTransformer(glide_data_source, geocoder))
    return transformers


spain_flood = (
    "spain_flood",
    "https://www.glidenumber.net/glide/jsonglideset.jsp?level1=ESP&fromyear=2024&toyear=2024&events=FL&number=2024-000199",
)


class GlideTest(unittest.TestCase):
    scenarios = [spain_flood]

    def setUp(self) -> None:
        super().setUp()
        self.validator = CustomValidator()
        # create temporary folder
        makedirs(get_data_file("temp/glide"), exist_ok=True)

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_transformer(self, transformer: GlideTransformer) -> None:
        items = transformer.make_items()
        self.assertTrue(len(items) > 0)
        source_event_item = None
        source_hazard_item = None
        for item in items:
            # write pretty json in a temporary folder for manual inspection
            item_path = get_data_file(f"temp/glide/{item.id}.json")
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_item = item
            elif monty_item_ext.is_source_hazard():
                source_hazard_item = item

        self.assertIsNotNone(source_event_item)
        self.assertIsNotNone(source_hazard_item)

    @parameterized.expand(load_scenarios(DATA_FILE))
    @pytest.mark.vcr()
    def test_transformer_with_file_data(self, transformer: GlideTransformer) -> None:
        items = transformer.make_items()
        self.assertTrue(len(items) > 0)
        source_event_item = None
        source_hazard_item = None
        for item in items:
            # write pretty json in a temporary folder for manual inspection
            item_path = get_data_file(f"temp/glide/{item.id}.json")
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_item = item
            elif monty_item_ext.is_source_hazard():
                source_hazard_item = item

        self.assertIsNotNone(source_event_item)
        self.assertIsNotNone(source_hazard_item)

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_event_item_uses_all_codes(self, transformer: GlideTransformer) -> None:
        for item in transformer.get_stac_items():
            # write pretty json in a temporary folder
            item_path = get_data_file(f"temp/glide/{item.id}.json")
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                # Should contain only the first code (UNDRR-ISC 2025)
                assert len(monty_item_ext.hazard_codes) == 3

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_hazard_item_uses_2025_code_only(self, transformer: GlideTransformer) -> None:
        for item in transformer.get_stac_items():
            # write pretty json in a temporary folder
            item_path = get_data_file(f"temp/glide/{item.id}.json")
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_hazard():
                # Should contain only the first code (UNDRR-ISC 2025)
                assert len(monty_item_ext.hazard_codes) == 1

    @parameterized.expand(load_scenarios(scenarios))
    def test_ifrc_hazard_codes_2025(self, transformer: GlideTransformer):
        # Test consolidated codes
        assert transformer.get_hazard_codes("EQ") == ["GH0101", "nat-geo-ear-gro", "EQ"]
        assert transformer.get_hazard_codes("TC") == ["MH0309", "nat-met-sto-tro", "TC"]

        # Test reclassified tsunami
        assert transformer.get_hazard_codes("TS") == ["MH0705", "nat-geo-ear-tsu", "TS"]

        # Test other disaster types
        assert transformer.get_hazard_codes("FL") == ["MH0600", "nat-hyd-flo-flo", "FL"]
        assert transformer.get_hazard_codes("FF") == ["MH0603", "nat-hyd-flo-fla", "FF"]
        assert transformer.get_hazard_codes("VO") == ["GH0205", "nat-geo-vol-vol", "VO"]
        assert transformer.get_hazard_codes("DR") == ["MH0401", "nat-cli-dro-dro", "DR"]
        assert transformer.get_hazard_codes("HT") == ["MH0501", "nat-met-ext-hea", "HT"]
        assert transformer.get_hazard_codes("CW") == ["MH0502", "nat-met-ext-col", "CW"]
        assert transformer.get_hazard_codes("LS") == ["GH0300", "nat-geo-mmd-lan", "LS"]
        assert transformer.get_hazard_codes("SS") == ["MH0703", "nat-met-sto-sur", "SS"]
        assert transformer.get_hazard_codes("FR") == ["TL0032", "tec-ind-fir-fir", "FR"]
        assert transformer.get_hazard_codes("EP") == ["BI0101", "nat-bio-epi-dis", "OT"]
