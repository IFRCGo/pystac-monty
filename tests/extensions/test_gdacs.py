"""Tests for pystac.tests.extensions.monty"""

import json
import tempfile
import typing
import unittest
from os import makedirs
from pathlib import Path

import pytest
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import MockGeocoder
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import DataType, File, GdacsDataSourceType, GdacsEpisodes, GenericDataSource, Memory
from pystac_monty.sources.gdacs import (
    GDACSDataSource,
    GDACSDataSourceType,
    GDACSTransformer,
)
from pystac_monty.sources.utils import save_json_data_into_tmp_file
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator
from tests.utils.test_utils import assert_processing_extension_fields, request_for_schema, validate_correlation_id

CURRENT_SCHEMA_URI = "https://ifrcgo.org/monty-stac-extension/v1.3.0/schema.json"
CURRENT_SCHEMA_MAPURL = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/json-schema/schema.json"
GDACS_FIXTURES = Path(__file__).resolve().parent.parent / "data-files" / "gdacs"


def fixture_tmp_file(name: str) -> tempfile._TemporaryFileWrapper:
    """Load committed GDACS JSON; no network at import/collection time."""
    data = json.loads((GDACS_FIXTURES / name).read_text(encoding="utf-8"))
    return save_json_data_into_tmp_file(data)


def fixture_memory(name: str) -> dict:
    """Load committed GDACS JSON for in-memory scenarios; no network at import/collection time."""
    return json.loads((GDACS_FIXTURES / name).read_text(encoding="utf-8"))


def load_scenarios(
    scenarios: typing.Iterable[typing.Tuple[typing.Dict, str]],
) -> list[GDACSTransformer]:
    transformers = []
    for scenario, hazard_type in scenarios:
        if isinstance(scenario.get(GDACSDataSourceType.EVENT), tempfile._TemporaryFileWrapper):
            event_data_file = scenario.get(GDACSDataSourceType.EVENT)

            episodes_data = []
            for episode in scenario.get("episodes"):
                episode_event_file = episode.get(GDACSDataSourceType.EVENT)
                episode_geometry_file = episode.get(GDACSDataSourceType.GEOMETRY)
                episode_impact_file = episode.get(GDACSDataSourceType.IMPACT)
                event_episode_data = GdacsEpisodes(
                    type=GDACSDataSourceType.EVENT,
                    data=GenericDataSource(
                        source_url="https://www.test.com", input_data=File(path=episode_event_file.name, data_type=DataType.FILE)
                    ),
                    hazard_type=hazard_type,
                )

                if episode_geometry_file is not None:
                    geometry_episode_data = GdacsEpisodes(
                        type=GDACSDataSourceType.GEOMETRY,
                        data=GenericDataSource(
                            source_url="https://www.test.com",
                            input_data=File(path=episode_geometry_file.name, data_type=DataType.FILE),
                        ),
                        hazard_type=hazard_type,
                    )
                if episode_impact_file:
                    impact_episode_data = GdacsEpisodes(
                        type=GDACSDataSourceType.IMPACT,
                        data=GenericDataSource(
                            source_url="https://www.test.com",
                            input_data=File(path=episode_impact_file.name, data_type=DataType.FILE),
                        ),
                        hazard_type=hazard_type,
                    )
                else:
                    impact_episode_data = None

                episode_data_tuple = (event_episode_data, geometry_episode_data, impact_episode_data)
                episodes_data.append(episode_data_tuple)
            gdacs_data_sources = GDACSDataSource(
                data=GdacsDataSourceType(
                    source_url="https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/docs/model/sources/GDACS/1102983-1-geteventdata-source.json",
                    event_data=File(path=event_data_file.name, data_type=DataType.FILE),
                    episodes=episodes_data,
                )
            )
            geocoder = MockGeocoder()
            transformers.append(GDACSTransformer(gdacs_data_sources, geocoder))
        elif isinstance(scenario.get(GDACSDataSourceType.EVENT), dict):
            event_data = scenario.get(GDACSDataSourceType.EVENT)
            episodes_data = []
            for episode in scenario.get("episodes"):
                episode_event = episode.get(GDACSDataSourceType.EVENT)
                episode_geometry = episode.get(GDACSDataSourceType.GEOMETRY)
                episode_impact = episode.get(GDACSDataSourceType.IMPACT)
                event_episode_data = GdacsEpisodes(
                    type=GDACSDataSourceType.EVENT,
                    data=GenericDataSource(
                        source_url="https://www.test.com",
                        input_data=Memory(content=episode_event, data_type=DataType.MEMORY),
                    ),
                    hazard_type=hazard_type,
                )

                geometry_episode_data = None
                if episode_geometry is not None:
                    geometry_episode_data = GdacsEpisodes(
                        type=GDACSDataSourceType.GEOMETRY,
                        data=GenericDataSource(
                            source_url="https://www.test.com",
                            input_data=Memory(content=episode_geometry, data_type=DataType.MEMORY),
                        ),
                        hazard_type=hazard_type,
                    )
                if episode_impact:
                    impact_episode_data = GdacsEpisodes(
                        type=GDACSDataSourceType.IMPACT,
                        data=GenericDataSource(
                            source_url="https://www.test.com",
                            input_data=Memory(content=episode_impact, data_type=DataType.MEMORY),
                        ),
                        hazard_type=hazard_type,
                    )
                else:
                    impact_episode_data = None
                episode_data_tuple = (event_episode_data, geometry_episode_data, impact_episode_data)
                episodes_data.append(episode_data_tuple)

            gdacs_data_sources = GDACSDataSource(
                data=GdacsDataSourceType(
                    source_url="https://www.test.com",
                    event_data=Memory(content=event_data, data_type=DataType.MEMORY),
                    episodes=episodes_data,
                )
            )
            geocoder = MockGeocoder()
            transformers.append(GDACSTransformer(gdacs_data_sources, geocoder))
        else:
            raise TypeError(
                "Scenario event data must be a committed fixture dict or temporary file; "
                "live GDACS URLs are not supported at collection time."
            )

    return transformers


spain_flood = {
    GDACSDataSourceType.EVENT: fixture_memory("1102983-1-geteventdata-source.json"),
    "episodes": [
        {
            GDACSDataSourceType.EVENT: fixture_memory("fl-1102983-ep1-event.json"),
            GDACSDataSourceType.GEOMETRY: fixture_memory("fl-1102983-ep1-geometry.json"),
        },
        {
            GDACSDataSourceType.EVENT: fixture_memory("fl-1102983-ep2-event.json"),
            GDACSDataSourceType.GEOMETRY: fixture_memory("fl-1102983-ep2-geometry.json"),
        },
    ],
}

drought_latam = {
    GDACSDataSourceType.EVENT: fixture_memory("dr-1016449-event.json"),
    "episodes": [
        {
            GDACSDataSourceType.EVENT: fixture_memory("dr-1016449-ep1-event.json"),
            GDACSDataSourceType.GEOMETRY: fixture_memory("dr-1016449-ep1-geometry.json"),
        },
        {
            GDACSDataSourceType.EVENT: fixture_memory("dr-1016449-ep2-event.json"),
            GDACSDataSourceType.GEOMETRY: fixture_memory("dr-1016449-ep2-geometry.json"),
        },
        {
            GDACSDataSourceType.EVENT: fixture_memory("dr-1016449-ep3-event.json"),
            GDACSDataSourceType.GEOMETRY: fixture_memory("dr-1016449-ep3-geometry.json"),
        },
        {
            GDACSDataSourceType.EVENT: fixture_memory("dr-1016449-ep4-event.json"),
            GDACSDataSourceType.GEOMETRY: fixture_memory("dr-1016449-ep4-geometry.json"),
        },
        {
            GDACSDataSourceType.EVENT: fixture_memory("dr-1016449-ep5-event.json"),
            GDACSDataSourceType.GEOMETRY: fixture_memory("dr-1016449-ep5-geometry.json"),
        },
        {
            GDACSDataSourceType.EVENT: fixture_memory("dr-1016449-ep6-event.json"),
            GDACSDataSourceType.GEOMETRY: fixture_memory("dr-1016449-ep6-geometry.json"),
        },
        {
            GDACSDataSourceType.EVENT: fixture_memory("dr-1016449-ep7-event.json"),
            GDACSDataSourceType.GEOMETRY: fixture_memory("dr-1016449-ep7-geometry.json"),
        },
    ],
}

spain_flood_2 = {
    GDACSDataSourceType.EVENT: fixture_tmp_file("1102983-1-geteventdata-source.json"),
    "episodes": [
        {
            GDACSDataSourceType.EVENT: fixture_tmp_file("fl-1102983-ep1-event.json"),
            GDACSDataSourceType.GEOMETRY: fixture_tmp_file("fl-1102983-ep1-geometry.json"),
        },
        {
            GDACSDataSourceType.EVENT: fixture_tmp_file("fl-1102983-ep2-event.json"),
            GDACSDataSourceType.GEOMETRY: fixture_tmp_file("fl-1102983-ep2-geometry.json"),
        },
    ],
}

spain_tropical_cyclone_3 = {
    GDACSDataSourceType.EVENT: fixture_tmp_file("tc-1001253-event.json"),
    "episodes": [
        {
            GDACSDataSourceType.EVENT: fixture_tmp_file("tc-1001253-ep1-event.json"),
            GDACSDataSourceType.GEOMETRY: fixture_tmp_file("tc-1001253-ep1-geometry.json"),
            GDACSDataSourceType.IMPACT: fixture_tmp_file("tc-1001253-ep1-impact.json"),
        }
    ],
}

venezuela_wildfire_1 = {
    GDACSDataSourceType.EVENT: fixture_tmp_file("wf-1028649-event.json"),
    "episodes": [
        {
            GDACSDataSourceType.EVENT: fixture_tmp_file("wf-1028649-ep1-event.json"),
            GDACSDataSourceType.GEOMETRY: fixture_tmp_file("wf-1028649-ep1-geometry.json"),
            GDACSDataSourceType.IMPACT: fixture_tmp_file("wf-1028649-ep1-impact.json"),
        }
    ],
}


class GDACSTest(unittest.TestCase):
    scenarios = zip([spain_flood, drought_latam], ["FL", "DR"])
    scenarios_2 = zip(
        [
            spain_flood_2,
        ],
        ["FL"],
    )
    scenarios_3 = zip([spain_tropical_cyclone_3], ["TC"])
    scenarios_4 = zip([venezuela_wildfire_1], ["WF"])

    def setUp(self) -> None:
        super().setUp()
        self.validator = CustomValidator()
        # create temporary folder
        makedirs(get_data_file("temp/gdacs"), exist_ok=True)

    # Test for memory data
    @parameterized.expand(load_scenarios(scenarios), skip_on_empty=True)
    @pytest.mark.vcr()
    def test_transformer_with_memory_data(self, transformer: GDACSTransformer) -> None:
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

        source_event_item = None
        source_hazard_item = None
        source_impact_item = None
        sendai_data_available = False
        for episode in transformer.data_source.episodes:
            episode_data = episode[0].data.input_data.content

            if "sendai" in episode_data["properties"] and len(episode_data["properties"]["sendai"]) > 0:
                sendai_data_available = True
                break

        for item in transformer.get_stac_items():
            # write pretty json in a temporary folder
            item_path = get_data_file(f"temp/gdacs/{item.id}.json")
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            assert_processing_extension_fields(item)
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

    # Test for file data
    @parameterized.expand(load_scenarios(scenarios_2), skip_on_empty=True)
    @pytest.mark.vcr()
    def test_transformer_with_file_data(self, transformer: GDACSTransformer) -> None:
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

        source_event_item = None
        source_hazard_item = None
        source_impact_item = None
        sendai_data_available = False
        for episode in transformer.data_source.episodes:
            episode_data_file = episode[0].data.input_data.path
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
            assert_processing_extension_fields(item)
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

        # Verify Correlation ID
        hazard_profiles = MontyHazardProfiles()
        event_item_hazard_code = hazard_profiles.get_canonical_hazard_codes(source_event_item)[0].upper()
        validate_correlation_id(source_event_item.properties.get("monty:corr_id"), event_item_hazard_code)
        hazard_item_hazard_code = hazard_profiles.get_canonical_hazard_codes(source_hazard_item)[0].upper()
        validate_correlation_id(source_hazard_item.properties.get("monty:corr_id"), hazard_item_hazard_code)
        if sendai_data_available:
            impact_item_hazard_code = hazard_profiles.get_canonical_hazard_codes(source_impact_item)[0].upper()
            validate_correlation_id(source_impact_item.properties.get("monty:corr_id"), impact_item_hazard_code)

    @parameterized.expand(load_scenarios(scenarios_2), skip_on_empty=True)
    @pytest.mark.vcr()
    def test_transformer_item_links(self, transformer: GDACSTransformer) -> None:
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

        source_event_item = None
        source_hazard_item = None
        source_impact_item = None
        sendai_data_available = False
        for episode in transformer.data_source.episodes:
            episode_data_file = episode[0].data.input_data.path
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
            assert_processing_extension_fields(item)
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

        # Verify Related links exists
        event_item_related_items = source_event_item.get_links(rel="related")
        hazard_item_related_items = source_hazard_item.get_links(rel="related")
        event_item_self_link = source_event_item.self_href
        hazard_item_self_link = source_hazard_item.self_href

        self.assertTrue(len(event_item_related_items) > 0)
        self.assertTrue(len(hazard_item_related_items) > 0)
        assert all(link.href is not None and link.href != event_item_self_link for link in event_item_related_items)
        assert all(link.href is not None and link.href != hazard_item_self_link for link in hazard_item_related_items)

        if sendai_data_available:
            impact_item_related_items = source_impact_item.get_links(rel="related")
            self.assertTrue(len(impact_item_related_items) > 0)
            impact_item_self_link = source_impact_item.self_href
            assert all(link.href is not None and link.href != impact_item_self_link for link in impact_item_related_items)

    @parameterized.expand([], skip_on_empty=True)
    @pytest.mark.vcr()
    def test_gdacs_hazard_codes_2025(self, transformer: GDACSTransformer) -> None:
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

        assert transformer.get_hazard_codes("FL") == ["MH0600", "nat-hyd-flo-flo", "FL"]
        assert transformer.get_hazard_codes("EQ") == ["GH0101", "nat-geo-ear-gro", "EQ"]
        assert transformer.get_hazard_codes("TC") == ["MH0309", "nat-met-sto-tro", "TC"]
        assert transformer.get_hazard_codes("DR") == ["MH0401", "nat-cli-dro-dro", "DR"]
        assert transformer.get_hazard_codes("WF") == ["EN0205", "nat-cli-wil-wil", "WF"]
        assert transformer.get_hazard_codes("VO") == ["GH0205", "nat-geo-vol-vol", "VO"]

    @parameterized.expand(load_scenarios(scenarios_2), skip_on_empty=True)
    @pytest.mark.vcr()
    def test_hazard_item_uses_2025_code_only(self, transformer: GDACSTransformer) -> None:
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

        for item in transformer.get_stac_items():
            # write pretty json in a temporary folder
            item_path = get_data_file(f"temp/gdacs/{item.id}.json")
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            assert_processing_extension_fields(item)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_hazard():
                # Should contain only the first code (UNDRR-ISC 2025)
                assert len(monty_item_ext.hazard_codes) == 1

    @parameterized.expand(load_scenarios(scenarios_2), skip_on_empty=True)
    @pytest.mark.vcr()
    def test_event_item_uses_all_codes(self, transformer: GDACSTransformer) -> None:
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

        for item in transformer.get_stac_items():
            # write pretty json in a temporary folder
            item_path = get_data_file(f"temp/gdacs/{item.id}.json")
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            assert_processing_extension_fields(item)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                # Should contain only the first code (UNDRR-ISC 2025)
                assert len(monty_item_ext.hazard_codes) == 3

    @parameterized.expand(load_scenarios(scenarios_3), skip_on_empty=True)
    @pytest.mark.vcr()
    def test_transformer_with_file_data_tc(self, transformer: GDACSTransformer) -> None:
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

        source_event_item = None
        source_hazard_item = None
        source_impact_items = []

        for item in transformer.get_stac_items():
            # write pretty json in a temporary folder
            item_path = get_data_file(f"temp/gdacs/{item.id}.json")
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            assert_processing_extension_fields(item)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_item = item
            elif monty_item_ext.is_source_hazard():
                source_hazard_item = item
            elif monty_item_ext.is_source_impact():
                source_impact_items.append(item)

        ids = [source_event_item.id, source_hazard_item.id] + [item.id for item in source_impact_items]
        self.assertTrue(len(ids) == len(set(ids)))

        self.assertIsNotNone(source_event_item)
        self.assertIsNotNone(source_hazard_item)
        self.assertTrue(len(source_impact_items) >= 0)  # can be 0 as well (when affected pop = 0)

    @parameterized.expand(load_scenarios(scenarios_4), skip_on_empty=True)
    @pytest.mark.vcr()
    def test_transformer_with_file_data_wf(self, transformer: GDACSTransformer) -> None:
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

        source_event_item = None
        source_hazard_item = None
        source_impact_items = []

        for item in transformer.get_stac_items():
            # write pretty json in a temporary folder
            item_path = get_data_file(f"temp/gdacs/{item.id}.json")
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            assert_processing_extension_fields(item)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_item = item
            elif monty_item_ext.is_source_hazard():
                source_hazard_item = item
            elif monty_item_ext.is_source_impact():
                source_impact_items.append(item)

        ids = [source_event_item.id, source_hazard_item.id] + [item.id for item in source_impact_items]
        self.assertTrue(len(ids) == len(set(ids)))

        self.assertIsNotNone(source_event_item)
        self.assertIsNotNone(source_hazard_item)
        self.assertTrue(len(source_impact_items) >= 0)


# Cyclone NOUL-26, episodes 9 and 13: the reference case of the GDACS impact mapping
# (monty-stac-extension docs/model/sources/GDACS, "Tropical cyclone advisory timeline").
NOUL_26_EPISODES = (9, 13)


def noul_26_timeline_url(episode: int) -> str:
    return f"https://www.gdacs.org/gdacsapi/api/export/gettimeline?episode={episode}"


def make_noul_26_transformer() -> GDACSTransformer:
    def episode_source(data_type: GDACSDataSourceType, name: str, url: str) -> GdacsEpisodes:
        return GdacsEpisodes(
            type=data_type,
            data=GenericDataSource(source_url=url, input_data=Memory(content=fixture_memory(name), data_type=DataType.MEMORY)),
            hazard_type="TC",
        )

    episodes = [
        (
            episode_source(GDACSDataSourceType.EVENT, f"tc-1001294-ep{ep}-event.json", "https://www.test.com"),
            episode_source(GDACSDataSourceType.GEOMETRY, f"tc-1001294-ep{ep}-geometry.json", "https://www.test.com"),
            episode_source(GDACSDataSourceType.IMPACT, f"tc-1001294-ep{ep}-timeline.json", noul_26_timeline_url(ep)),
        )
        for ep in NOUL_26_EPISODES
    ]
    data_source = GDACSDataSource(
        data=GdacsDataSourceType(
            source_url="https://www.test.com",
            event_data=Memory(content=fixture_memory("tc-1001294-ep13-event.json"), data_type=DataType.MEMORY),
            episodes=episodes,
        )
    )
    return GDACSTransformer(data_source, MockGeocoder())


class GDACSImpactMappingTest(unittest.TestCase):
    """GDACS `impacts[]` items follow the documented mapping (pystac-monty#188)."""

    def setUp(self) -> None:
        super().setUp()
        self.validator = CustomValidator()

    def get_impact_items_by_episode(self, transformer: GDACSTransformer) -> dict[int, dict[str, typing.Any]]:
        items_by_episode: dict[int, dict[str, typing.Any]] = {ep: {} for ep in NOUL_26_EPISODES}
        for item in transformer.get_stac_items():
            item.validate(validator=self.validator)
            monty = MontyExtension.ext(item)
            if monty.is_source_impact():
                self.assertNotIn(item.id, items_by_episode[monty.episode_number])
                items_by_episode[monty.episode_number][item.id] = item
        return items_by_episode

    @pytest.mark.vcr()
    def test_tc_impact_items(self) -> None:
        request_for_schema(url=CURRENT_SCHEMA_URI)
        items_by_episode = self.get_impact_items_by_episode(make_noul_26_transformer())

        for episode, items in items_by_episode.items():
            self.assertTrue(items)
            for item_id, item in items.items():
                # The episode is not part of the id: gdacs-impact-{eventid}-{timeline id}-{band}
                self.assertRegex(item_id, r"^gdacs-impact-1001294-\d+-(pop39|pop74|surge)$")
                detail = MontyExtension.ext(item).impact_detail
                self.assertEqual(detail.category, "people")
                self.assertEqual(detail.type, "potentially_affected")
                self.assertEqual(detail.estimate_type, "modelled")
                self.assertEqual(detail.unit, "count")
                self.assertGreater(detail.value, 0)
                self.assertTrue(detail.description)
                self.assertIn("forecasted", item.properties)
                self.assertIn("advisory_number", item.properties)
                self.assertEqual(item.geometry["type"], "Point")
                self.assertEqual([link.href for link in item.get_links("via")], [noul_26_timeline_url(episode)])

        ep9, ep13 = items_by_episode[9], items_by_episode[13]

        # Advisory 9 (observed): `pop` is 0, but 75 million people are in the 39 kt wind field.
        self.assertEqual(MontyExtension.ext(ep13["gdacs-impact-1001294-271236-pop39"]).impact_detail.value, 75490475)
        self.assertEqual(MontyExtension.ext(ep13["gdacs-impact-1001294-271236-pop74"]).impact_detail.value, 614941)
        self.assertFalse(ep13["gdacs-impact-1001294-271236-pop39"].properties["forecasted"])
        self.assertEqual(
            ep13["gdacs-impact-1001294-271236-pop39"].properties["title"],
            "NOUL-26 advisory 9, population in 39 kt wind field",
        )

        # Last forecast entry of episode 13: every band is 0, so no item (its `pop` of 19.8 million is not mapped).
        self.assertFalse([item_id for item_id in ep13 if "-271403-" in item_id])
        # No storm surge exposure in this storm: no surge item.
        self.assertFalse([item_id for item_id in ep13 if item_id.endswith("-surge")])

        # Observed entries repeat in each episode: they collapse onto the same ids.
        observed_ep9 = {item_id for item_id, item in ep9.items() if not item.properties["forecasted"]}
        self.assertTrue(observed_ep9)
        self.assertTrue(observed_ep9 <= set(ep13))
        # Superseded forecasts of episode 9 keep their own ids.
        forecast_ep9 = {item_id for item_id, item in ep9.items() if item.properties["forecasted"]}
        self.assertTrue(forecast_ep9)
        self.assertFalse(forecast_ep9 & set(ep13))

    @parameterized.expand(load_scenarios(zip([venezuela_wildfire_1], ["WF"])), skip_on_empty=True)
    @pytest.mark.vcr()
    def test_wf_impact_item(self, transformer: GDACSTransformer) -> None:
        request_for_schema(url=CURRENT_SCHEMA_URI)

        impact_items = []
        event_item = None
        for item in transformer.get_stac_items():
            item.validate(validator=self.validator)
            monty = MontyExtension.ext(item)
            if monty.is_source_impact():
                impact_items.append(item)
            elif monty.is_source_event():
                event_item = item

        self.assertEqual(len(impact_items), 1)
        item = impact_items[0]
        monty = MontyExtension.ext(item)
        self.assertEqual(item.id, f"gdacs-impact-{monty.src_event_id}-{monty.episode_number}-popaffected")

        # The geometry is the burnt area of the `getimpact` document, not the event footprint.
        self.assertEqual(item.geometry["type"], "MultiPolygon")
        self.assertNotEqual(item.geometry, event_item.geometry)

        detail = monty.impact_detail
        self.assertEqual(detail.category, "people")
        self.assertEqual(detail.type, "potentially_affected")
        self.assertEqual(detail.estimate_type, "modelled")
        self.assertEqual(detail.unit, "count")
        self.assertEqual(detail.value, 11070)
        self.assertEqual(detail.description, "Population in the burnt area")
