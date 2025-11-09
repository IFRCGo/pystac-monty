import json
import unittest
from os import makedirs
from typing import List

import pytest
import requests
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import MockGeocoder
from pystac_monty.sources.common import DataType, File, GenericDataSource, Memory
from pystac_monty.sources.idu import IDUDataSource, IDUTransformer
from pystac_monty.sources.utils import IDMCUtils, save_json_data_into_tmp_file
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator

CURRENT_SCHEMA_URI = "https://ifrcgo.github.io/monty/v0.1.0/schema.json"
CURRENT_SCHEMA_MAPURL = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/json-schema/schema.json"


def load_scenarios_from_file(scenarios: List[str], timeout: int = 30) -> List[IDUTransformer]:
    transformers = []
    for scenario in scenarios:
        response = requests.get(scenario[1], timeout=timeout)
        response_data = response.json()

        data_file = save_json_data_into_tmp_file(response_data)

        data = GenericDataSource(source_url=scenario[1], input_data=File(path=data_file.name, data_type=DataType.FILE))

        idu_data_source = IDUDataSource(data=data)
        geocoder = MockGeocoder()
        transformers.append(IDUTransformer(idu_data_source, geocoder))
    return transformers


def load_scenarios(scenarios: List[str], timeout: int = 30) -> List[IDUTransformer]:
    transformers = []
    for scenario in scenarios:
        response = requests.get(scenario[1], timeout=timeout)
        response_data = response.json()
        data = GenericDataSource(
            source_url=scenario[1], input_data=Memory(content=json.dumps(response_data), data_type=DataType.MEMORY)
        )
        idu_data_source = IDUDataSource(data=data)
        geocoder = MockGeocoder()
        transformers.append(IDUTransformer(idu_data_source, geocoder))
    return transformers


spain = [
    "Spain",
    "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/IDMC/model/sources/IDMC/idmc-idu-samples-ESP.json",  # noqa
]


class IDUTest(unittest.TestCase):
    scenarios = [spain]

    def setUp(self) -> None:
        super().setUp()
        self.validator = CustomValidator()
        # create temporary folder
        makedirs(get_data_file("temp/idu"), exist_ok=True)

    @parameterized.expand(load_scenarios_from_file(scenarios))
    @pytest.mark.vcr()
    def test_transformer_from_file(self, transformer: IDUTransformer) -> None:
        source_event_item = None
        source_impact_item = None

        for item in transformer.get_stac_items():
            item_path = get_data_file(f"temp/idu/{item.id}.json")
            with open(item_path, "w", encoding="utf-8") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_item = item
            elif monty_item_ext.is_source_impact():
                source_impact_item = item

        self.assertIsNotNone(source_event_item)
        self.assertIsNotNone(source_impact_item)

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_transformer(self, transformer: IDUTransformer) -> None:
        source_event_item = None
        source_impact_item = None

        for item in transformer.get_stac_items():
            item_path = get_data_file(f"temp/idu/{item.id}.json")
            with open(item_path, "w", encoding="utf-8") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_item = item
            elif monty_item_ext.is_source_impact():
                source_impact_item = item

        self.assertIsNotNone(source_event_item)
        self.assertIsNotNone(source_impact_item)

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_event_item_uses_all_codes(self, transformer: IDUTransformer) -> None:
        for item in transformer.get_stac_items():
            # write pretty json in a temporary folder
            item_path = get_data_file(f"temp/idu/{item.id}.json")
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                # Should contain only the first code (UNDRR-ISC 2025)
                assert len(monty_item_ext.hazard_codes) == 3

    @pytest.mark.vcr()
    def test_gdacs_hazard_codes_2025(self) -> None:
        assert IDMCUtils.hazard_codes_mapping(("weather related", "hydrological", "flood", "flood")) == [
            "MH0600",
            "nat-hyd-flo-flo",
            "FL",
        ]
        assert IDMCUtils.hazard_codes_mapping(("geophysical", "geophysical", "earthquake", "earthquake")) == [
            "GH0101",
            "nat-geo-ear-gro",
            "EQ",
        ]
        assert IDMCUtils.hazard_codes_mapping(("weather related", "meteorological", "storm", "typhoon/hurricane/cyclone")) == [
            "MH0309",
            "nat-met-sto-tro",
            "TC",
        ]
        assert IDMCUtils.hazard_codes_mapping(("weather related", "climatological", "drought", "drought")) == [
            "MH0401",
            "nat-cli-dro-dro",
            "DR",
        ]
        assert IDMCUtils.hazard_codes_mapping(("weather related", "climatological", "wildfire", "wildfire")) == [
            "EN0205",
            "nat-cli-wil-wil",
            "WF",
        ]
        assert IDMCUtils.hazard_codes_mapping(("geophysical", "geophysical", "volcanic activity", "volcanic activity")) == [
            "GH0205",
            "nat-geo-vol-vol",
            "VO",
        ]
