"""Tests for pystac.tests.extensions.monty"""

import json
from os import makedirs
from typing import List,  TypedDict
from unittest import TestCase

from parameterized import parameterized
from pystac_monty.geocoding import MockGeocoder

from pystac_monty.sources.ifrc_events import (
    IfrcEventsDataSource,
    IfrcEventsTransformer
)
from tests.conftest import get_data_file


class IfrcEventsScenario(TypedDict):
    name: str
    event_url: str


hurricane_beryl_data: IfrcEventsScenario = {
    "name": "Hurricane Beryl - Saint Vincent and the Grenadines, Grenada, Barbados and Jamaica",
    "event_url": "https://alpha-1-api.ifrc-go.dev.datafriendlyspace.org/api/v2/event/7046/?format=json"
}

food_security_data: IfrcEventsScenario = {
    "name": "Food Security",
    "event_url": "https://alpha-1-api.ifrc-go.dev.datafriendlyspace.org/api/v2/event/2139/?format=json"
}


def load_scenarios(
    scenarios: list[IfrcEventsScenario],
) -> List[IfrcEventsTransformer]:
    transformers: List[IfrcEventsTransformer] = []

    for scenario in scenarios:
        event_url = scenario.get('event_url', None)
        geocoder = MockGeocoder()

        if event_url is None:
            continue

        data_source = IfrcEventsDataSource(event_url)

        transformer = IfrcEventsTransformer(data_source, geocoder)
        transformers.append(transformer)

    return transformers


class IfrcEventsTest(TestCase):
    scenarios = [hurricane_beryl_data, food_security_data]

    def setUp(self) -> None:
        super().setUp()

        # Create temporary folder for test outputs
        makedirs(get_data_file("temp/ifrc_events"), exist_ok=True)

    @parameterized.expand(load_scenarios(scenarios))
    def test_transformer(self, transformer: IfrcEventsTransformer) -> None:
        items = transformer.make_items()
        self.assertTrue(len(items) > 0)

        makedirs(get_data_file("temp/ifrc_events"), exist_ok=True)

        for item in items:
            item_path = get_data_file(f"temp/ifrc_events/{item.id}.json")
            with open(item_path, "w", encoding="utf-8") as f:
                json.dump(item.to_dict(), f, indent=2, ensure_ascii=False)

        # TODO: validate items
