"""Tests for pystac.tests.extensions.monty"""

import json
import unittest
from os import makedirs
from typing import Any, Optional

import pytest
import requests
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.sources.gdacs import (
    GDACSDataSource,
    GDACSDataSourceType,
    GDACSTransformer,
)
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator

CURRENT_SCHEMA_URI = "https://ifrcgo.github.io/monty/v0.1.0/schema.json"
CURRENT_SCHEMA_MAPURL = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/json-schema/schema.json"


def load_scenarios(
    scenarios: list[tuple[str, dict[GDACSDataSourceType, str]]],
) -> list[GDACSTransformer]:
    transformers = []
    for scenario in scenarios:
        items = scenario[1].items()
        gdacs_data_sources = []
        for item in items:
            data = requests.get(item[1]).text
            gdacs_data_sources.append(GDACSDataSource(item[1], data, item[0]))
        transformers.append(GDACSTransformer(gdacs_data_sources))
    return transformers


spain_flood_episode1 = (
    "spain_flood_episode1",
    {
        GDACSDataSourceType.EVENT: "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/model/sources/GDACS/1102983-1-geteventdata-source.json",
        GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=FL&eventid=1102983&episodeid=1",
    },
)
spain_flood_episode2 = (
    "spain_flood_episode2",
    {
        GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/geteventdata?eventtype=FL&eventid=1102983&episodeid=2",
        GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=FL&eventid=1102983&episodeid=2",
    },
)


class GDACSTest(unittest.TestCase):
    scenarios = [spain_flood_episode1, spain_flood_episode2]

    def setUp(self) -> None:
        super().setUp()
        self.validator = CustomValidator()
        # create temporary folder
        makedirs(get_data_file("temp/gdacs"), exist_ok=True)

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_transformer(self, transformer: GDACSTransformer) -> None:
        items = transformer.make_items()
        self.assertTrue(len(items) > 0)
        source_event_item = None
        source_hazard_item = None
        for item in items:
            # write pretty json in a temporary folder
            item_path = get_data_file(f"temp/gdacs/{item.id}.json")
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
