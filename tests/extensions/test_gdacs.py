"""Tests for pystac.tests.extensions.monty"""

import json
import unittest
from os import makedirs

import pytest
import requests
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import MockGeocoder
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
    scenarios: list[dict],
) -> list[GDACSTransformer]:
    transformers = []
    for scenario in scenarios:
        event_data_url = scenario.get(GDACSDataSourceType.EVENT)
        event_data = requests.get(event_data_url).json()
        episodes_data = []
        for episode in scenario.get("episodes"):
            episode_event_url = episode.get(GDACSDataSourceType.EVENT)
            episode_geometry_url = episode.get(GDACSDataSourceType.GEOMETRY)
            episode_event = requests.get(episode_event_url).json()
            episode_geometry = requests.get(episode_geometry_url).json()
            episode_data = {
                GDACSDataSourceType.EVENT: (episode_event_url, episode_event),
            }
            if episode_geometry_url is not None:
                episode_data[GDACSDataSourceType.GEOMETRY] = (episode_geometry_url, episode_geometry)
            episodes_data.append(episode_data)
        gdacs_data_sources = GDACSDataSource(source_url=event_data_url, data=event_data, episodes=episodes_data)
        geocoder = MockGeocoder()
        transformers.append(GDACSTransformer(gdacs_data_sources, geocoder))
    return transformers


spain_flood = {
    GDACSDataSourceType.EVENT: "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/model/sources/GDACS/1102983-1-geteventdata-source.json",  ## noqa E501
    "episodes": [
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=FL&eventid=1102983&episodeid=1",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=FL&eventid=1102983&episodeid=1",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=FL&eventid=1102983&episodeid=2",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=FL&eventid=1102983&episodeid=2",
        },
    ],
}

drought_latam = {
    GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/geteventdata?eventtype=DR&eventid=1016449",
    # 50 episodes
    "episodes": [
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=1",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=1",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=2",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=2",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=3",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=3",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=4",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=4",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=5",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=5",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=6",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=6",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=7",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=7",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=8",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=8",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=9",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=9",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=10",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=10",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=11",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=11",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=12",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=12",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=13",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=13",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=14",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=14",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=15",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=15",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=16",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=16",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=17",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=17",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=18",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=18",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=19",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=19",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=20",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=20",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=21",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=21",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=22",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=22",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=23",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=23",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=24",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=24",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=25",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=25",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=26",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=26",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=27",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=27",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=28",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=28",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=29",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=29",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=30",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=30",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=31",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=31",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=32",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=32",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=33",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=33",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=34",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=34",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=35",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=35",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=36",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=36",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=37",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=37",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=38",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=38",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=39",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=39",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=40",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=40",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=41",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=41",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=42",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=42",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=43",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=43",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=44",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=44",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=45",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=45",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=46",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=46",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=47",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=47",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=48",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=48",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=49",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=49",
        },
        {
            GDACSDataSourceType.EVENT: "https://www.gdacs.org/gdacsapi/api/events/getepisodedata?eventtype=DR&eventid=1016449&episodeid=50",
            GDACSDataSourceType.GEOMETRY: "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=DR&eventid=1016449&episodeid=50",
        },
    ],
}


class GDACSTest(unittest.TestCase):
    scenarios = [spain_flood, drought_latam]

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
        source_impact_item = None
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
            elif monty_item_ext.is_source_impact():
                source_impact_item = item

        self.assertIsNotNone(source_event_item)
        self.assertIsNotNone(source_hazard_item)
        # self.assertIsNotNone(source_impact_item)
