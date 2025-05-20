"""Tests for pystac.tests.extensions.monty"""

import json
import tempfile
import unittest
from os import makedirs

import pytest
import requests
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import MockGeocoder
from pystac_monty.sources.glide import GlideDataSource, GlideTransformer
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator

CURRENT_SCHEMA_URI = "https://ifrcgo.github.io/monty/v0.1.0/schema.json"
CURRENT_SCHEMA_MAPURL = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/json-schema/schema.json"


def request_and_save_tmp_file(url):
    response = requests.get(url)
    tmpfile = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
    tmpfile.write(response.content)
    return tmpfile.name


def load_scenarios(
    scenarios: list[tuple[str, str]],
) -> list[GlideTransformer]:
    transformers = []
    for scenario in scenarios:
        data = request_and_save_tmp_file(scenario[1])
        glide_data_source = GlideDataSource(scenario[1], data)
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
