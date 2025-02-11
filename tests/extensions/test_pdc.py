import json
import unittest
from os import makedirs
from typing import List

import pytest
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.sources.pdc import PDCDataSource, PDCTransformer
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator

CURRENT_SCHEMA_URI = "https://ifrcgo.github.io/monty/v0.1.0/schema.json"
CURRENT_SCHEMA_MAPURL = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/json-schema/schema.json"  # noqa


def load_scenarios(scenarios: List[dict]) -> List[PDCTransformer]:
    """Load Scenarios"""
    transformers = []
    for scenario in scenarios:
        # response = requests.get(scenario[1], timeout=timeout)
        # response_data = response.json()

        idu_data_source = PDCDataSource(source_url=scenario[0], data=json.dumps(scenario[1]))
        transformers.append(PDCTransformer(idu_data_source))
    return transformers


scenario = [
    "https://raw.githubusercontent.com/ranjan-stha/testrepo/refs/heads/main/testpdc.json",  # noqa
    {
        "hazards_file_path": "./tests/hazard_data.json",
        "exposure_detail_file_path": "./tests/exposure_detail.json",
        "uuid": "f2762e72-5169-487e-a3fa-30e21fc55893",
        "exposure_timestamp": "11234",
        "geojson_file_path": "",
    },
]


class PDCTest(unittest.TestCase):
    scenarios = [scenario]

    def setUp(self) -> None:
        super().setUp()
        self.validator = CustomValidator()
        # create temporary folder
        makedirs(get_data_file("temp/idu"), exist_ok=True)

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_transformer(self, transformer: PDCTransformer) -> None:
        items = transformer.make_items()
        self.assertTrue(len(items) > 0)
        source_event_item = None
        source_hazard_item = None
        source_impact_item = None

        for item in items:
            item_path = get_data_file(f"temp/idu/{item.id}.json")
            with open(item_path, "w", encoding="utf-8") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_item = item
            elif monty_item_ext.is_source_hazard():
                source_hazard_item = item
            elif monty_item_ext.is_source_impact():
                source_impact_item = item

        self.assertIsNotNone(source_event_item)
        self.assertIsNotNone(source_hazard_item)
        self.assertIsNotNone(source_impact_item)
