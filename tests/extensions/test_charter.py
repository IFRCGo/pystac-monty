"""Tests for Charter STAC source transformer"""

import datetime
import unittest
from copy import deepcopy
from pathlib import Path

import pytest

from pystac_monty.extension import MontyExtension
from pystac_monty.sources.charter import CharterDataSource, CharterTransformer, iter_charter_stac_items
from pystac_monty.sources.common import DataType, File, GenericDataSource, Memory, sanitize_stac_item_id
from pystac_monty.sources.utils import save_json_data_into_tmp_file
from tests.extensions.test_monty import CustomValidator


@pytest.fixture(scope="module")
def vcr_config():
    return {"cassette_library_dir": "tests/extensions/cassettes/test_charter"}


JSON_MOCK_DATA = {
    "id": "act-test",
    "type": "Feature",
    "geometry": {"type": "Point", "coordinates": [-43.202, -21.547]},
    "bbox": [-43.202, -21.547, -43.202, -21.547],
    "properties": {
        "disaster:activation_id": 1019,
        "disaster:call_ids": [1166],
        "disaster:type": ["flood", "landslide"],
        "disaster:country": "BRA",
        "cpe:activation_status": "archived",
        "datetime": "2024-01-01T00:00:00Z",
        "title": "Test Multi-Hazard Activation",
        "description": "Test activation for multi-hazard scenario",
    },
    "links": [],
    "areas": [
        {
            "id": "area-test",
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-43.3, -21.6], [-43.1, -21.6], [-43.1, -21.4], [-43.3, -21.4], [-43.3, -21.6]]],
            },
            "bbox": [-43.3, -21.6, -43.1, -21.4],
            "properties": {
                "title": "Test Area",
                "description": "Radius (km): 10.0\nPriority: 1",
                "cpe:status": {"stage": "notificationNew"},
            },
        }
    ],
}


# Modeled on monty-stac-extension/docs/model/sources/Charter/act-1000-vap-1144-1.json.
# The trailing duplicated coordinate exercises polygon normalization.
VAP_MOCK = {
    "type": "Feature",
    "id": "act-test-vap-1144-1",
    "geometry": {
        "type": "Polygon",
        "coordinates": [
            [
                [67.69, 36.559],
                [67.799, 36.57],
                [67.751, 36.727],
                [67.642, 36.718],
                [67.69, 36.559],
                [67.69, 36.559],
            ]
        ],
    },
    "bbox": [67.642, 36.559, 67.799, 36.727],
    "properties": {
        "disaster:class": "ValueAddedProduct",
        "cpe:cos2_id": "act-test-vap-1144-1",
        "datetime": "2024-01-02T00:00:00Z",
        "title": "Preliminary satellite-derived damage assessment, Test Area",
        "additional_information": "Damaged buildings observed.",
        "copyright": "Includes Pleiades material \u00a9 CNES (2024), Distribution Airbus DS.",
    },
}


def _activation_with_vaps() -> dict:
    """Multi-hazard activation with a web page link and one VAP sidecar."""
    data = deepcopy(JSON_MOCK_DATA)
    data["links"] = [{"rel": "about", "href": "https://disasterscharter.org/activations/test-1000", "type": "text/html"}]
    data["vaps"] = [deepcopy(VAP_MOCK)]
    return data


def _responses(items):
    return [item for item in items if MontyExtension.ext(item).is_source_response()]


def _memory_transformer(data: dict | None = None) -> CharterTransformer:
    source = CharterDataSource(
        data=GenericDataSource(
            source_url="https://supervisor.disasterscharter.org/api/activations/act-test",
            input_data=Memory(content=data or JSON_MOCK_DATA, data_type=DataType.MEMORY),
        )
    )
    return CharterTransformer(source, None)


def _partition(items):
    event, hazards = None, []
    for item in items:
        monty = MontyExtension.ext(item)
        if monty.is_source_event():
            event = item
        elif monty.is_source_hazard():
            hazards.append(item)
    return event, hazards


class CharterTest(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.validator = CustomValidator()

    @pytest.mark.vcr()
    def test_transformer_with_mock_data(self) -> None:
        """Multi-hazard mock activation: schema-valid items, links, shared corr_id."""
        items = list(_memory_transformer().get_stac_items())
        for item in items:
            item.validate(validator=self.validator)

        event, hazards = _partition(items)
        self.assertIsNotNone(event)
        self.assertEqual(len(hazards), 2)
        self.assertEqual(event.collection_id, "charter-events")
        for hazard in hazards:
            self.assertEqual(hazard.collection_id, "charter-hazards")

        monty_event = MontyExtension.ext(event)
        self.assertEqual(monty_event.country_codes, ["BRA"])
        self.assertIsNotNone(monty_event.correlation_id)
        self.assertGreater(len(monty_event.hazard_codes or []), 0)
        self.assertTrue(any(link.rel == "via" for link in event.links))

        corr = monty_event.correlation_id
        for hazard in hazards:
            monty_hazard = MontyExtension.ext(hazard)
            self.assertEqual(monty_hazard.correlation_id, corr)
            self.assertLessEqual(len(monty_hazard.hazard_codes or []), 3)
            self.assertIsNotNone(monty_hazard.hazard_detail)
            self.assertTrue(any(link.rel == "related" for link in hazard.links))
            derived = [link for link in hazard.links if link.rel == "derived_from"]
            self.assertEqual(len(derived), 1)
            self.assertEqual(derived[0].media_type, "application/json")

    def test_sanitize_stac_item_id(self) -> None:
        self.assertEqual(sanitize_stac_item_id("abc/def:g[h]"), "abc-def-g-h")
        self.assertEqual(sanitize_stac_item_id("aoi_1_epi_0"), "aoi_1_epi_0")
        self.assertEqual(sanitize_stac_item_id(""), "x")

    def test_area_description_parsing(self) -> None:
        transformer = _memory_transformer()
        radius, priority, surface_area = transformer.parse_area_description("Radius (km): 8.0\nPriority: 1")
        self.assertEqual((radius, priority, surface_area), (8.0, 1, None))

        radius, priority, surface_area = transformer.parse_area_description("Some text")
        self.assertEqual((radius, priority, surface_area), (None, None, None))

        radius, priority, surface_area = transformer.parse_area_description(
            "Call-1144 AoI ID: 1, Priority: 1, SurfaceArea: 101, Comment:"
        )
        self.assertEqual((radius, priority, surface_area), (None, 1, 101.0))

    def test_transformer_with_file_data(self) -> None:
        tmpfile = save_json_data_into_tmp_file(JSON_MOCK_DATA)
        source = CharterDataSource(
            data=GenericDataSource(source_url="test", input_data=File(path=tmpfile.name, data_type=DataType.FILE))
        )
        items = list(CharterTransformer(source, None).get_stac_items())
        self.assertEqual(len(items), 3)

    def test_missing_activation_id(self) -> None:
        bad_data = deepcopy(JSON_MOCK_DATA)
        del bad_data["properties"]["disaster:activation_id"]
        self.assertEqual(list(_memory_transformer(bad_data).get_stac_items()), [])

    def test_single_hazard_activation(self) -> None:
        data = deepcopy(JSON_MOCK_DATA)
        data["properties"]["disaster:type"] = ["earthquake"]
        items = list(_memory_transformer(data).get_stac_items())
        hazards = [item for item in items if MontyExtension.ext(item).is_source_hazard()]
        self.assertEqual(len(hazards), 1)

    def test_multiple_areas(self) -> None:
        data = deepcopy(JSON_MOCK_DATA)
        data["areas"] = [
            data["areas"][0],
            {
                "id": "area-test-2",
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-43.5, -21.8], [-43.0, -21.8], [-43.0, -21.2], [-43.5, -21.2], [-43.5, -21.8]]],
                },
                "bbox": [-43.5, -21.8, -43.0, -21.2],
                "properties": {
                    "title": "Test Area 2",
                    "description": "Radius (km): 15.0\nPriority: 2",
                    "cpe:status": {"stage": "readyToDeliver"},
                },
            },
        ]
        items = list(_memory_transformer(data).get_stac_items())
        hazards = [item for item in items if MontyExtension.ext(item).is_source_hazard()]
        self.assertEqual(len(hazards), 4)

    def test_validator_integration(self) -> None:
        from pystac_monty.validators.charter import CharterActivation

        validated = CharterActivation(**JSON_MOCK_DATA)
        self.assertEqual(validated.properties.disaster_activation_id, 1019)
        self.assertEqual(len(validated.areas), 1)

    def test_no_vaps_yields_no_response_items(self) -> None:
        responses = _responses(_memory_transformer().get_stac_items())
        self.assertEqual(responses, [])

    def test_response_items_from_vap(self) -> None:
        """A VAP sidecar produces a response item with response_detail and bidirectional links."""
        items = list(_memory_transformer(_activation_with_vaps()).get_stac_items())
        for item in items:
            item.validate(validator=self.validator)

        event, hazards = _partition(items)
        responses = _responses(items)

        self.assertEqual(len(responses), 1)
        response = responses[0]
        self.assertEqual(response.id, "charter-response-1019-1144-1")
        self.assertEqual(response.collection_id, "charter-response")
        self.assertEqual(response.properties["roles"], ["response", "source"])
        self.assertEqual(response.properties["disaster:class"], "vap")
        self.assertEqual(response.properties["disaster:resolution_class"], "VHR")

        detail = response.properties["monty:response_detail"]
        self.assertEqual(detail["type"], "eo-gra")
        self.assertEqual(detail["source_id"], "1144-1")
        self.assertEqual(detail["methodology"], "human_interpreted")
        self.assertEqual(detail["sendai_targets"], ["C", "D"])
        self.assertEqual(detail["producer"], "Airbus")

        monty_response = MontyExtension.ext(response)
        self.assertEqual(monty_response.country_codes, ["BRA"])
        self.assertEqual(monty_response.correlation_id, MontyExtension.ext(event).correlation_id)
        self.assertGreater(len(monty_response.hazard_codes or []), 0)

        # Trailing duplicate coordinate is normalized away (6 -> 5 points, still closed).
        ring = response.geometry["coordinates"][0]
        self.assertEqual(len(ring), 5)
        self.assertEqual(ring[0], ring[-1])

        related_roles = [link.extra_fields.get("roles") for link in response.links if link.rel == "related"]
        self.assertIn(["event"], related_roles)
        self.assertEqual(related_roles.count(["hazard"]), len(hazards))
        derived = [link for link in response.links if link.rel == "derived_from"]
        self.assertEqual(len(derived), 1)
        self.assertEqual(derived[0].media_type, "text/html")

    def test_response_skipped_without_source_id(self) -> None:
        data = _activation_with_vaps()
        data["vaps"][0]["properties"].pop("cpe:cos2_id")
        data["vaps"][0]["id"] = "no-identifiable-id"
        self.assertEqual(_responses(_memory_transformer(data).get_stac_items()), [])

    def test_vap_source_id(self) -> None:
        self.assertEqual(CharterTransformer._vap_source_id({"properties": {"cpe:cos2_id": "act-1-vap-9-2"}}), "9-2")
        self.assertEqual(CharterTransformer._vap_source_id({"id": "act-1-vap-7-3"}), "7-3")
        self.assertIsNone(CharterTransformer._vap_source_id({"id": "no-match"}))

    def test_infer_response_type(self) -> None:
        self.assertEqual(CharterTransformer._infer_response_type("Damage assessment", ""), "eo-gra")
        self.assertEqual(CharterTransformer._infer_response_type("Flood extent map", "affected area"), "eo-del")
        self.assertEqual(CharterTransformer._infer_response_type("Population exposure", ""), "eo-pop")
        self.assertEqual(CharterTransformer._infer_response_type("Reference map", "general overview"), "eo-vap")

    def test_sendai_defaults_match_response_taxonomy(self) -> None:
        # Defaults must match monty-stac-extension response-taxonomy.md section 4.5.
        from pystac_monty.sources.charter import RESPONSE_TYPE_SENDAI

        self.assertEqual(RESPONSE_TYPE_SENDAI["eo-gra"], ["C", "D"])
        self.assertEqual(RESPONSE_TYPE_SENDAI["eo-del"], ["D", "G"])
        self.assertEqual(RESPONSE_TYPE_SENDAI["eo-pop"], ["B"])
        self.assertEqual(RESPONSE_TYPE_SENDAI["eo-vap"], ["D", "G"])

    def test_response_type_and_sendai_targets_per_vap(self) -> None:
        cases = [
            ("Affected area extent", "eo-del", ["D", "G"]),
            ("Population exposure analysis", "eo-pop", ["B"]),
            ("Reference overview map", "eo-vap", ["D", "G"]),
        ]
        for title, expected_type, expected_sendai in cases:
            data = _activation_with_vaps()
            data["vaps"][0]["properties"]["title"] = title
            data["vaps"][0]["properties"]["additional_information"] = ""
            response = _responses(_memory_transformer(data).get_stac_items())[0]
            detail = response.properties["monty:response_detail"]
            self.assertEqual(detail["type"], expected_type, title)
            self.assertEqual(detail["sendai_targets"], expected_sendai, title)

    def test_infer_producer_and_resolution(self) -> None:
        self.assertEqual(CharterTransformer._infer_producer("Distribution Airbus DS"), "Airbus")
        self.assertIsNone(CharterTransformer._infer_producer("Maxar"))
        self.assertIsNone(CharterTransformer._infer_producer(""))
        self.assertEqual(CharterTransformer._infer_resolution_class("Pleiades / CNES"), "VHR")
        self.assertIsNone(CharterTransformer._infer_resolution_class("Sentinel-2"))

    def test_normalize_polygon_geometry(self) -> None:
        norm = CharterTransformer._normalize_polygon_geometry
        poly = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0], [0, 0]]]}
        self.assertEqual(norm(poly)["coordinates"][0], [[0, 0], [1, 0], [1, 1], [0, 0]])
        point = {"type": "Point", "coordinates": [0, 0]}
        self.assertEqual(norm(point), point)
        self.assertIsNone(norm(None))

    def test_make_event_item_guards(self) -> None:
        transformer = _memory_transformer()
        point = {"type": "Point", "coordinates": [0, 0]}
        self.assertIsNone(transformer.make_event_item({"properties": {}}))
        self.assertIsNone(
            transformer.make_event_item({"properties": {"disaster:activation_id": "x", "disaster:type": ["other"]}})
        )
        self.assertIsNone(
            transformer.make_event_item({"properties": {"disaster:activation_id": "x", "disaster:type": ["flood"]}})
        )
        self.assertIsNone(
            transformer.make_event_item(
                {"properties": {"disaster:activation_id": "x", "disaster:type": ["flood"]}, "geometry": point}
            )
        )

    def test_make_event_item_accepts_datetime_object(self) -> None:
        activation = {
            "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
            "bbox": [0.0, 0.0, 0.0, 0.0],
            "properties": {
                "disaster:activation_id": "dtobj",
                "disaster:type": ["flood"],
                "disaster:country": "BRA",
                "datetime": datetime.datetime(2024, 1, 1),
            },
        }
        item = _memory_transformer().make_event_item(activation)
        self.assertIsNotNone(item)
        self.assertEqual(item.id, "charter-event-dtobj")

    def test_make_items_delegates_to_get_stac_items(self) -> None:
        items = _memory_transformer().make_items()
        self.assertTrue(any(MontyExtension.ext(item).is_source_event() for item in items))

    def test_iter_charter_stac_items_from_submodule(self) -> None:
        charter_dir = Path(__file__).resolve().parents[2] / "monty-stac-extension" / "docs" / "model" / "sources" / "Charter"
        if not list(charter_dir.glob("act-*-activation.json")):
            self.skipTest("monty-stac-extension submodule not initialized")
        roles = [item.properties.get("roles", []) for item in iter_charter_stac_items(charter_dir)]
        self.assertTrue(any("event" in role for role in roles))
        self.assertTrue(any("hazard" in role for role in roles))
        self.assertTrue(any("response" in role for role in roles))
