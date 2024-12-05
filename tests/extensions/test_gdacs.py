"""Tests for pystac.tests.extensions.monty"""

import json
import unittest
from os import makedirs

import pystac.validation
import pytest
import requests

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


@pytest.fixture
def enable_monty_extension() -> None:
    MontyExtension.enable_extension()


class GDACSTest(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.validator = CustomValidator()
        # create temporary folder
        makedirs(get_data_file("temp/gdacs"), exist_ok=True)

    @pytest.mark.vcr()
    def test_transformer(self) -> None:
        event_data = requests.get(
            "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/model/sources/GDACS/1102983-1-geteventdata-source.json"
        ).text
        geometry_data = requests.get(
            "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/model/sources/GDACS/1102983-1-getgeometry-source.json"
        ).text
        transformer = GDACSTransformer(
            [
                GDACSDataSource(
                    "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/model/sources/GDACS/1102983-1-geteventdata-source.json",
                    event_data,
                    GDACSDataSourceType.EVENT,
                ),
                GDACSDataSource(
                    "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/model/sources/GDACS/1102983-1-getgeometry-source.json",
                    geometry_data,
                    GDACSDataSourceType.GEOMETRY,
                ),
            ]
        )
        items = transformer.make_items()
        self.assertTrue(len(items) > 0)
        source_event_item = None
        source_hazard_item = None
        for item in items:
            item.validate(validator=self.validator)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_item = item
            elif monty_item_ext.is_source_hazard():
                source_hazard_item = item
            # write pretty json in a temporary folder
            item_path = get_data_file(f"temp/gdacs/{item.id}.json")
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)

        self.assertIsNotNone(source_event_item)
        self.assertIsNotNone(source_hazard_item)
