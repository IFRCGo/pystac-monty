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
    for scenario_item in scenarios:
        idu_data_source = PDCDataSource(source_url=scenario_item[0], data=json.dumps(scenario_item[1]))
        transformers.append(PDCTransformer(idu_data_source))
    return transformers


scenario = [
    "https://raw.githubusercontent.com/ranjan-stha/testrepo/refs/heads/main/testpdc.json",  # noqa
    {
        "hazards_file_path": "./tests/data/pdc/hazard_data.json",
        "exposure_detail_file_path": "./tests/data/pdc/exposure_detail.json",
        "uuid": "f2762e72-5169-487e-a3fa-30e21fc55893",
        "exposure_timestamp": "1738941728977",
        "geojson_file_path": "./tests/data/pdc/hazard_geo.geojson",
    },
]


class PDCTest(unittest.TestCase):
    scenarios = [scenario]

    def setUp(self) -> None:
        super().setUp()
        self.validator = CustomValidator()
        # create temporary folder
        makedirs(get_data_file("temp/pdc"), exist_ok=True)

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_transformer(self, transformer: PDCTransformer) -> None:
        items = transformer.make_items()
        self.assertTrue(len(items) > 0)
        source_event_item = None
        source_hazard_item = None
        source_impact_item = None

        for item in items:
            item_path = get_data_file(f"temp/pdc/{item.id}.json")
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
