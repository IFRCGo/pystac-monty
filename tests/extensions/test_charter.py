"""Tests for Charter STAC source transformer"""

import json
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path

from pystac_monty.extension import MontyExtension
from pystac_monty.sources.charter import (
    CharterDataSource,
    CharterTransformer,
    convert_charter_activations,
    iter_charter_stac_items,
)
from pystac_monty.sources.common import DataType, File, GenericDataSource, Memory, sanitize_stac_item_id
from pystac_monty.sources.utils import save_json_data_into_tmp_file
from tests.extensions.test_monty import CustomValidator

JSON_MOCK_DATA = {
    "id": "act-test",
    "type": "Feature",
    "geometry": {"type": "Point", "coordinates": [-43.202, -21.547]},
    "bbox": [-43.202, -21.547, -43.202, -21.547],
    "properties": {
        "disaster:activation_id": "test",
        "disaster:type": ["flood", "landslide"],
        "disaster:country": "BRA",
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

    def test_iter_charter_stac_items_includes_nested_response_examples(self) -> None:
        source_dir = Path(__file__).resolve().parents[2] / "monty-stac-extension" / "docs" / "model" / "sources" / "Charter"
        items = list(iter_charter_stac_items(source_dir))
        response_ids = {item.id for item in items if MontyExtension.ext(item).is_source_response()}

        self.assertIn("charter-response-1000-1144-1", response_ids)
        self.assertIn("charter-response-1019-1166-19", response_ids)
        self.assertIn("charter-response-1019-1166-22", response_ids)
        self.assertIn("charter-response-1166-phr1a-0907-00777", response_ids)
        self.assertNotIn("charter-response-1144-phr1a-0907-00777", response_ids)

        event_1019 = next(item for item in items if item.id == "charter-event-1019")
        event_response_hrefs = {link.get_href() for link in event_1019.links if link.extra_fields.get("roles") == ["response"]}
        self.assertIn("../charter-response/charter-response-1019-1166-19.json", event_response_hrefs)

    def test_convert_charter_activations_uses_normal_static_export(self) -> None:
        source_dir = Path(__file__).resolve().parents[2] / "monty-stac-extension" / "docs" / "model" / "sources" / "Charter"
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            convert_charter_activations(source_dir, output_dir)

            item_doc = json.loads((output_dir / "charter-events" / "charter-event-1000.json").read_text(encoding="utf-8"))
            collection_doc = json.loads((output_dir / "charter-response" / "charter-response.json").read_text(encoding="utf-8"))

        self.assertEqual(next(link for link in item_doc["links"] if link["rel"] == "self")["href"], "./charter-event-1000.json")
        self.assertNotIn("https://ifrcgo.org/monty-stac-extension/examples", json.dumps(item_doc))
        self.assertEqual(len(collection_doc["extent"]["spatial"]["bbox"]), 1)

    def test_calibrated_dataset_response_detail(self) -> None:
        source_dir = Path(__file__).resolve().parents[2] / "monty-stac-extension" / "docs" / "model" / "sources" / "Charter"
        item = next(item for item in iter_charter_stac_items(source_dir) if item.id == "charter-response-1166-phr1a-0907-00777")

        detail = item.properties["monty:response_detail"]
        self.assertEqual(detail["type"], "eo-dat")
        self.assertEqual(detail["source_id"], "DS_PHR1A_202603021304008_FR1_PX_W044S22_0907_00777-calibrated")
        self.assertEqual(detail["producer"], "Airbus")
        self.assertEqual(detail["sendai_targets"], ["D", "G"])
