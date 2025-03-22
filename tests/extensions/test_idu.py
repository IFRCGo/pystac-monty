import json
import unittest
from os import makedirs
from typing import List

import pytest
import requests
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import MockGeocoder
from pystac_monty.sources.idu import IDUDataSource, IDUTransformer
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator

CURRENT_SCHEMA_URI = "https://ifrcgo.github.io/monty/v0.1.0/schema.json"
CURRENT_SCHEMA_MAPURL = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/json-schema/schema.json"


def load_scenarios(scenarios: List[str], timeout: int = 30) -> List[IDUTransformer]:
    transformers = []
    for scenario in scenarios:
        response = requests.get(scenario[1], timeout=timeout)
        response_data = response.json()

        idu_data_source = IDUDataSource(source_url=scenario[1], data=json.dumps(response_data))
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

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_transformer(self, transformer: IDUTransformer) -> None:
        items = transformer.make_items()
        self.assertTrue(len(items) > 0)
        source_event_item = None
        source_impact_item = None

        for item in items:
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
