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
from pystac_monty.sources.common import DataType, File, GdacsDataSourceType, GdacsEpisodes, GenericDataSource
from pystac_monty.sources.gdacs import (
    GDACSDataSourceType,
    GDACSDataSourceV3,
    GDACSTransformer,
)
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator

CURRENT_SCHEMA_URI = "https://ifrcgo.org/monty-stac-extension/v1.0.0/schema.json"
CURRENT_SCHEMA_MAPURL = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/json-schema/schema.json"


def request_and_save_tmp_file(url):
    response = requests.get(url)
    tmpfile = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
    tmpfile.write(response.content)
    return tmpfile.name


def load_scenarios(
    scenarios: list[dict],
) -> list[GDACSTransformer]:
    transformers = []
    for scenario in scenarios:
        event_data_url = scenario.get(GDACSDataSourceType.EVENT)
        event_data_file = request_and_save_tmp_file(event_data_url)

        episodes_data = []
        for episode in scenario.get("episodes"):
            episode_event_url = episode.get(GDACSDataSourceType.EVENT)
            episode_geometry_url = episode.get(GDACSDataSourceType.GEOMETRY)
            episode_event_file = request_and_save_tmp_file(episode_event_url)
            episode_geometry_file = request_and_save_tmp_file(episode_geometry_url)
            event_episode_data = GdacsEpisodes(
                type=GDACSDataSourceType.EVENT,
                data=GenericDataSource(
                    source_url=episode_event_url, data_source=File(path=episode_event_file, data_type=DataType.FILE)
                ),
            )

            if episode_geometry_url is not None:
                geometry_episode_data = GdacsEpisodes(
                    type=GDACSDataSourceType.GEOMETRY,
                    data=GenericDataSource(
                        source_url=episode_geometry_url, data_source=File(path=episode_geometry_file, data_type=DataType.FILE)
                    ),
                )

            episode_data_tuple = (event_episode_data, geometry_episode_data)
            episodes_data.append(episode_data_tuple)
        gdacs_data_sources = GDACSDataSourceV3(
            data=GdacsDataSourceType(
                source_url=event_data_url, event_data=File(path=event_data_file, data_type=DataType.FILE), episodes=episodes_data
            )
        )
        geocoder = MockGeocoder()
        transformers.append(GDACSTransformer(gdacs_data_sources, geocoder))
    return transformers


spain_flood = {
    GDACSDataSourceType.EVENT: "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/docs/model/sources/GDACS/1102983-1-geteventdata-source.json",  ## noqa E501
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
    # 7 episodes
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
        source_event_item = None
        source_hazard_item = None
        source_impact_item = None
        sendai_data_available = False
        for episode in transformer.data_source.episodes:
            episode_data_file = episode[0].data.data_source.path
            with open(episode_data_file, "r", encoding="utf-8") as f:
                episode_data = json.loads(f.read())

            if "sendai" in episode_data["properties"] and len(episode_data["properties"]["sendai"]) > 0:
                sendai_data_available = True
                break

        for item in transformer.get_stac_items():
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
        if sendai_data_available:
            self.assertIsNotNone(source_impact_item)
