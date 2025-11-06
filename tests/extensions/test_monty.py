"""Tests for pystac.tests.extensions.monty"""

import json
import unittest
from datetime import datetime
from typing import Any

import pytest
import requests
from pystac import Asset, Item
from pystac.validation import JsonSchemaSTACValidator

from pystac_monty.extension import MontyExtension

CURRENT_SCHEMA_URI = "https://ifrcgo.org/monty-stac-extension/v1.1.0/schema.json"
CURRENT_SCHEMA_MAPURL = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/json-schema/schema.json"


@pytest.fixture
def enable_monty_extension() -> None:
    MontyExtension.enable_extension()


def make_item() -> Item:
    item = Item(
        id="test-item",
        geometry=[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)],
        bbox=[0.0, 0.0, 1.0, 1.0],
        datetime=datetime(2021, 1, 1),
        properties={},
    )
    item.add_asset(
        "data",
        Asset(
            href="https://example.com/data.tif",
            media_type="image/tiff; application=geotiff",
        ),
    )
    MontyExtension.add_to(item)

    return item


class CustomValidator(JsonSchemaSTACValidator):
    _schema_cache = None

    def _get_schema(self, schema_uri: str) -> dict[str, Any]:
        if schema_uri == CURRENT_SCHEMA_URI:
            if self._schema_cache is None:
                self.__class__._schema_cache = json.loads(requests.get(CURRENT_SCHEMA_MAPURL).text)
            return self._schema_cache
        return super()._get_schema(schema_uri)


class MontyTest(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.item = make_item()
        self.validator = CustomValidator()

    def test_stac_extensions(self) -> None:
        self.assertTrue(MontyExtension.has_extension(self.item))

    def test_item_repr(self) -> None:
        monty_item_ext = MontyExtension.ext(self.item)
        self.assertEqual(f"<ItemMontyExtension Item id={self.item.id}>", monty_item_ext.__repr__())

    @pytest.mark.vcr()
    def test_validates_reference_event(self) -> None:
        event_item = Item.from_dict(
            json.loads(
                requests.get(
                    "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/examples/reference-events/20241027T150000-ESP-HM-FLOOD-001-GCDB.json"  ## noqa E501
                ).text
            )
        )
        event_item.validate(validator=self.validator)
