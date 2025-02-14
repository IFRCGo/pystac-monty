"""Tests for pystac.tests.extensions.monty"""

import json
from os import makedirs
from typing import List
from unittest import TestCase

import pytest
import requests
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import MockGeocoder
from pystac_monty.sources.ifrc_events import IFRCEventDataSource, IFRCEventTransformer
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator

nepal_earthquake_data = ("karnali_earthquake", "https://goadmin-stage.ifrc.org/api/v2/event/?dtype=2&appeal_type=1&id=6732")

morocco_earthquake_data = ("morocco_earthquake", "https://goadmin-stage.ifrc.org/api/v2/event/?dtype=2&appeal_type=1&id=6646")


def load_scenarios(
    scenarios: list[tuple[str, str]],
) -> List[IFRCEventTransformer]:
    transformers: List[IFRCEventTransformer] = []

    for scenario in scenarios:
        event_data = requests.get(scenario[1]).text
        event_data = json.loads(event_data)["results"]
        geocoder = MockGeocoder()
        data_source = IFRCEventDataSource(scenario[1], json.dumps(event_data))

        transformer = IFRCEventTransformer(data_source, geocoder)
        transformers.append(transformer)

    return transformers


class IfrcEventsTest(TestCase):
    scenarios = [nepal_earthquake_data, morocco_earthquake_data]

    def setUp(self) -> None:
        super().setUp()
        self.validator = CustomValidator()

        # Create temporary folder for test outputs
        makedirs(get_data_file("temp/ifrc_events"), exist_ok=True)

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_transformer(self, transformer: IFRCEventTransformer) -> None:
        """Test EM-DAT transformation to STAC items

        Args:
            transformer: IfrcEventTransformer instance to test

        Tests:
            - Items are created
            - Items validate against schema
            - Source event and hazard items are present
            - Items can be serialized to JSON
        """
        items = transformer.make_items()
        self.assertTrue(len(items) > 0)

        source_event_item = None
        source_impact_item = None

        for item in items:
            # Write pretty JSON in temporary folder for manual inspection
            item_path = get_data_file(f"temp/ifrc_events/{item.id}.json")
            with open(item_path, "w", encoding="utf-8") as f:
                json.dump(item.to_dict(), f, indent=2, ensure_ascii=False)

            # Validate item against schema
            item.validate(validator=self.validator)

            # Check item type
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_item = item
            elif monty_item_ext.is_source_impact():
                source_impact_item = item

        # Verify required items were created
        self.assertIsNotNone(source_event_item)
        self.assertIsNotNone(source_impact_item)
