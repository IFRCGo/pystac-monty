import json
import tempfile
from os import makedirs
from typing import List, Tuple, TypedDict
from unittest import TestCase

import pytest
import requests
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import MockGeocoder
from pystac_monty.sources.desinventar import (
    DesinventarDataSource,
    DesinventarTransformer,
)
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator

geocoder = MockGeocoder()


class DesinventarData(TypedDict):
    zip_file_url: str
    country_code: str
    iso3: str


class DesinventarScenario(TypedDict):
    name: str
    data: DesinventarData


grenada_data: DesinventarScenario = {
    "name": "Grenada subset",
    "data": {
        "zip_file_url": "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/model/sources/DesInventar/DI_export_grd.zip",  # noqa: E501
        "country_code": "grd",
        "iso3": "GRD",
    },
}


def load_scenarios(
    scenarios: list[DesinventarScenario],
) -> List[Tuple[str, DesinventarTransformer]]:
    transformers: List[Tuple[str, DesinventarTransformer]] = []
    for scenario in scenarios:
        data = scenario["data"]
        # download zip file in temp folder
        response = requests.get(data["zip_file_url"])
        tmp_zip_file = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
        tmp_zip_file.write(response.content)
        data_source = DesinventarDataSource(tmp_zip_file, data["country_code"], data["iso3"], data["zip_file_url"])
        transformers.append((data["country_code"], DesinventarTransformer(data_source, geocoder)))
    return transformers


class DesinventarTest(TestCase):
    scenarios = [grenada_data]

    def setUp(self) -> None:
        """Set up test environment"""
        super().setUp()
        self.validator = CustomValidator()
        # Create temporary folder for test outputs
        makedirs(get_data_file("temp/desinventar"), exist_ok=True)

    @parameterized.expand(load_scenarios(scenarios))  # type: ignore[misc]
    @pytest.mark.vcr()
    def test_transformer(self, country_code: str, transformer: DesinventarTransformer) -> None:
        items = list(transformer.get_stac_items())
        print(items)
        self.assertTrue(len(items) > 0)

        source_event_items = []
        source_impact_items = []

        for item in items:
            # Write pretty JSON in temporary folder for manual inspection
            item_path = get_data_file(f"temp/desinventar/{item.id}.json")
            with open(item_path, "w", encoding="utf-8") as f:
                json.dump(item.to_dict(), f, indent=2, ensure_ascii=False)

            # Validate item against schema
            item.validate(validator=self.validator)

            # Check item type
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_items.append(item)
            elif monty_item_ext.is_source_impact():
                source_impact_items.append(item)

        # Verify required items were created
        # source_event_items contains items
        self.assertTrue(len(source_event_items) > 0)
        # source_impact_items contains items
        self.assertTrue(len(source_impact_items) > 0)
