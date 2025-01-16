import json
from os import makedirs
from typing import List, Tuple, TypedDict
from unittest import TestCase

from parameterized import parameterized

from pystac_monty.sources.desinventar import (
    DesinventarDataSource,
    DesinventarTransformer,
)
from tests.conftest import get_data_file


class DesinventarData(TypedDict):
    zip_file_url: str
    country_code: str
    iso3: str


class DesinventarScenario(TypedDict):
    name: str
    data: DesinventarData


nepal_data: DesinventarScenario = {
    "name": "Nepal data",
    "data": {
        "zip_file_url": "https://www.desinventar.net/DesInventar/download/DI_export_npl.zip",
        "country_code": "npl",
        "iso3": "npl",
    },
}

chile_data: DesinventarScenario = {
    "name": "Chile",
    "data": {
        "zip_file_url": "https://www.desinventar.net/DesInventar/download/DI_export_chl.zip",
        "country_code": "chl",
        "iso3": "chl",
    },
}


def load_scenarios(
    scenarios: list[DesinventarScenario],
) -> List[Tuple[str, DesinventarTransformer]]:
    transformers: List[Tuple[str, DesinventarTransformer]] = []

    for scenario in scenarios:
        data = scenario["data"]
        data_source = DesinventarDataSource.from_url(data["zip_file_url"], data["country_code"], data["iso3"])

        transformer = DesinventarTransformer(data_source)
        transformers.append((data["country_code"], transformer))

    return transformers


class DesinventarTest(TestCase):
    scenarios = [chile_data, nepal_data]

    def setUp(self) -> None:
        super().setUp()

        # Create temporary folder for test outputs
        makedirs(get_data_file("temp/desinventar"), exist_ok=True)

    @parameterized.expand(load_scenarios(scenarios))
    def test_transformer(self, country_code: str, transformer: DesinventarTransformer) -> None:
        items = transformer.get_items()
        self.assertTrue(len(items) > 0)

        makedirs(get_data_file(f"temp/desinventar/{country_code}"), exist_ok=True)

        for item in items:
            item_path = get_data_file(f"temp/desinventar/{country_code}/{item.id}.json")
            with open(item_path, "w", encoding="utf-8") as f:
                json.dump(item.to_dict(), f, indent=2, ensure_ascii=False)

        # TODO: validate items
