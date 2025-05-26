"""Tests for pystac.tests.extensions.monty"""

import json
import unittest
from os import makedirs
from typing import List

import pytest
import requests
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import MockGeocoder
from pystac_monty.sources.common import DataType, File, Memory
from pystac_monty.sources.gidd import GIDDDataSourceV2, GIDDTransformer
from pystac_monty.sources.utils import save_json_data_into_tmp_file
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator


def load_scenarios_from_file(scenarios: List[str], timeout: int = 30) -> List[GIDDTransformer]:
    transformers = []
    for scenario in scenarios:
        response = requests.get(scenario[1], timeout=timeout)
        response_data = response.json()

        data_file = save_json_data_into_tmp_file(response_data)

        data = {"source_url": scenario[1], "source_data": File(path=data_file.name, data_type=DataType.FILE)}

        geocoder = MockGeocoder()

        idu_data_source = GIDDDataSourceV2(data=data)
        transformers.append(GIDDTransformer(idu_data_source, geocoder))
    return transformers


def load_scenarios(scenarios: List[str], timeout: int = 30) -> List[GIDDTransformer]:
    transformers = []
    for scenario in scenarios:
        response = requests.get(scenario[1], timeout=timeout)
        response_data = response.json()

        data = {
            "source_url": scenario[1],
            "source_data": Memory(content=json.dumps(response_data["features"]), data_type=DataType.MEMORY),
        }

        geocoder = MockGeocoder()

        idu_data_source = GIDDDataSourceV2(data=data)
        transformers.append(GIDDTransformer(idu_data_source, geocoder))
    return transformers


test1 = [
    "Test1",
    "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/IDMC/model/sources/IDMC/IDMC_GIDD_Internal_Displacement_Disaggregated.geojson",  # noqa
]


class GIDDTest(unittest.TestCase):
    scenarios = [test1]

    def setUp(self) -> None:
        super().setUp()
        self.validator = CustomValidator()
        # create temporary folder
        makedirs(get_data_file("temp/gidd"), exist_ok=True)

    @parameterized.expand(load_scenarios_from_file(scenarios))
    @pytest.mark.vcr()
    def test_transformer_from_file(self, transformer: GIDDTransformer) -> None:
        source_event_item = None
        source_impact_item = None
        for item in transformer.get_stac_items():
            # write pretty json in a temporary folder for manual inspection
            item_path = get_data_file(f"temp/gidd/{item.id}.json")
            with open(item_path, "w") as f:
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
    def test_transformer(self, transformer: GIDDTransformer) -> None:
        source_event_item = None
        source_impact_item = None
        for item in transformer.get_stac_items():
            # write pretty json in a temporary folder for manual inspection
            item_path = get_data_file(f"temp/gidd/{item.id}.json")
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_item = item
            elif monty_item_ext.is_source_impact():
                source_impact_item = item

        self.assertIsNotNone(source_event_item)
        self.assertIsNotNone(source_impact_item)
