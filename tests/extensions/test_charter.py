"""Tests for Charter STAC source transformer"""

import json
import shutil
import unittest
from pathlib import Path

import pytest
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.sources.charter import (
    CharterDataSource,
    CharterTransformer,
    convert_charter_activations,
    iter_charter_stac_items,
)
from pystac_monty.sources.common import DataType, GenericDataSource, Memory, sanitize_stac_item_id
from pystac_monty.sources.utils import save_json_data_into_tmp_file
from tests.extensions.test_monty import CustomValidator


# Configure VCR cassette directory for this test module
@pytest.fixture(scope="module")
def vcr_config():
    return {"cassette_library_dir": "tests/extensions/cassettes/test_charter"}


# Mock Charter activation with areas (multi-hazard example)
json_mock_data = {
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

DATA_FILE = save_json_data_into_tmp_file(json_mock_data)


def load_scenarios(scenarios):
    """Load Charter test scenarios"""
    transformers = []
    for scenario_name, data_source_type in scenarios:
        if data_source_type == "file":
            charter_source = CharterDataSource(
                data=GenericDataSource(
                    source_url="https://supervisor.disasterscharter.org/api/activations/act-test",
                    input_data=Memory(content=json_mock_data, data_type=DataType.MEMORY),
                )
            )
        else:
            charter_source = CharterDataSource(
                data=GenericDataSource(
                    source_url="https://supervisor.disasterscharter.org/api/activations/act-test",
                    input_data=Memory(content=json_mock_data, data_type=DataType.MEMORY),
                )
            )
        transformers.append(CharterTransformer(charter_source, None))
    return transformers


class CharterTest(unittest.TestCase):
    scenarios = [("multi_hazard_test", "memory")]

    def setUp(self) -> None:
        super().setUp()
        self.validator = CustomValidator()
        # Use tests/data/charter for outputs instead of data-files/temp
        from pathlib import Path

        output_dir = Path(__file__).parent.parent / "data" / "charter" / "test-output"
        output_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir = output_dir

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_transformer_with_mock_data(self, transformer: CharterTransformer) -> None:
        """Test Charter transformer generates correct STAC items"""
        event_item = None
        hazard_items = []

        for item in transformer.get_stac_items():
            # Save for inspection
            item_path = self.output_dir / f"{item.id}.json"
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)

            # Validate against schema
            item.validate(validator=self.validator)

            # Categorize items
            monty_ext = MontyExtension.ext(item)
            if monty_ext.is_source_event():
                event_item = item
            elif monty_ext.is_source_hazard():
                hazard_items.append(item)

        # Assertions
        self.assertIsNotNone(event_item, "Event item should be created")
        self.assertEqual(len(hazard_items), 2, "Should have 2 hazard items (flood + landslide)")
        self.assertEqual(event_item.collection_id, "charter-events")
        for h in hazard_items:
            self.assertEqual(h.collection_id, "charter-hazards")

        # Verify event properties
        monty_event = MontyExtension.ext(event_item)
        self.assertEqual(monty_event.country_codes, ["BRA"])
        self.assertIsNotNone(monty_event.correlation_id)
        self.assertGreater(len(monty_event.hazard_codes), 0)

        # Verify hazard items
        for hazard_item in hazard_items:
            monty_hazard = MontyExtension.ext(hazard_item)
            # Each hazard should have same correlation ID as event
            self.assertEqual(monty_hazard.correlation_id, monty_event.correlation_id)
            # Each hazard should have max 3 codes
            self.assertLessEqual(len(monty_hazard.hazard_codes), 3)
            # Should have hazard detail
            self.assertIsNotNone(monty_hazard.hazard_detail)

    def test_sanitize_stac_item_id(self) -> None:
        """Forbidden STAC API id characters are replaced; underscores are kept."""
        self.assertEqual(sanitize_stac_item_id("abc/def:g[h]"), "abc-def-g-h")
        self.assertEqual(sanitize_stac_item_id("aoi_1_epi_0"), "aoi_1_epi_0")
        self.assertEqual(sanitize_stac_item_id(""), "x")

    def test_hazard_code_mapping(self) -> None:
        """Test Charter disaster types map to correct hazard codes"""
        from pystac_monty.sources.charter import CHARTER_HAZARD_CODES

        # Test current types
        self.assertIn("flood", CHARTER_HAZARD_CODES)
        self.assertEqual(CHARTER_HAZARD_CODES["flood"][0], "MH0600")  # UNDRR-ISC 2025

        self.assertIn("earthquake", CHARTER_HAZARD_CODES)
        self.assertEqual(CHARTER_HAZARD_CODES["earthquake"][0], "GH0101")

        # Test deprecated types still mapped
        self.assertIn("flood_flash", CHARTER_HAZARD_CODES)
        self.assertEqual(CHARTER_HAZARD_CODES["flood_flash"][0], "MH0603")

    def test_cpe_status_mapping(self) -> None:
        """Test CPE status maps to valid estimate types"""
        from pystac_monty.sources.charter import CPE_STATUS_MAPPING

        self.assertEqual(CPE_STATUS_MAPPING["notificationNew"], "primary")
        self.assertEqual(CPE_STATUS_MAPPING["readyToDeliver"], "secondary")

    def test_area_description_parsing(self) -> None:
        """Test parsing radius and priority from area description"""
        charter_source = CharterDataSource(
            data=GenericDataSource(source_url="test", input_data=Memory(content=json_mock_data, data_type=DataType.MEMORY))
        )
        transformer = CharterTransformer(charter_source, None)

        # Test radius parsing
        radius, priority = transformer.parse_area_description("Radius (km): 8.0\nPriority: 1")
        self.assertEqual(radius, 8.0)
        self.assertEqual(priority, 1)

        # Test missing values
        radius, priority = transformer.parse_area_description("Some text")
        self.assertIsNone(radius)
        self.assertIsNone(priority)

    def test_transformer_with_file_data(self) -> None:
        """Test Charter transformer with file-based data"""
        from pystac_monty.sources.common import File
        from pystac_monty.sources.utils import save_json_data_into_tmp_file

        tmpfile = save_json_data_into_tmp_file(json_mock_data)
        charter_source = CharterDataSource(
            data=GenericDataSource(source_url="test", input_data=File(path=tmpfile.name, data_type=DataType.FILE))
        )
        transformer = CharterTransformer(charter_source, None)

        items = list(transformer.get_stac_items())
        self.assertEqual(len(items), 3)  # 1 event + 2 hazards

    def test_item_links_validation(self) -> None:
        """Test that items have proper related links"""
        charter_source = CharterDataSource(
            data=GenericDataSource(source_url="test", input_data=Memory(content=json_mock_data, data_type=DataType.MEMORY))
        )
        transformer = CharterTransformer(charter_source, None)

        event_item = None
        hazard_items = []

        for item in transformer.get_stac_items():
            monty = MontyExtension.ext(item)
            if monty.is_source_event():
                event_item = item
            elif monty.is_source_hazard():
                hazard_items.append(item)

        # Event should have "via" link to Charter API
        via_links = [link for link in event_item.links if link.rel == "via"]
        self.assertGreater(len(via_links), 0)

        # Hazard items should link to event
        for hazard_item in hazard_items:
            related_links = [link for link in hazard_item.links if link.rel == "related"]
            self.assertGreater(len(related_links), 0)

            # Hazard items should have derived_from link to event
            derived_from_links = [link for link in hazard_item.links if link.rel == "derived_from"]
            self.assertGreater(len(derived_from_links), 0, "Hazard item should have derived_from link")
            self.assertEqual(derived_from_links[0].media_type, "application/json")

    def test_missing_activation_id(self) -> None:
        """Test handling of missing activation ID"""
        bad_data = json_mock_data.copy()
        bad_data["properties"] = bad_data["properties"].copy()
        del bad_data["properties"]["disaster:activation_id"]

        charter_source = CharterDataSource(
            data=GenericDataSource(source_url="test", input_data=Memory(content=bad_data, data_type=DataType.MEMORY))
        )
        transformer = CharterTransformer(charter_source, None)

        items = list(transformer.get_stac_items())
        self.assertEqual(len(items), 0)  # Should skip invalid data

    def test_single_hazard_activation(self) -> None:
        """Test activation with single disaster type"""
        single_hazard_data = json_mock_data.copy()
        single_hazard_data["properties"] = single_hazard_data["properties"].copy()
        single_hazard_data["properties"]["disaster:type"] = ["earthquake"]

        charter_source = CharterDataSource(
            data=GenericDataSource(source_url="test", input_data=Memory(content=single_hazard_data, data_type=DataType.MEMORY))
        )
        transformer = CharterTransformer(charter_source, None)

        items = list(transformer.get_stac_items())
        hazard_items = [item for item in items if MontyExtension.ext(item).is_source_hazard()]

        self.assertEqual(len(hazard_items), 1)  # Only 1 hazard item

    def test_multiple_areas(self) -> None:
        """Test activation with multiple areas"""
        multi_area_data = json_mock_data.copy()
        multi_area_data["areas"] = [
            multi_area_data["areas"][0],
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

        charter_source = CharterDataSource(
            data=GenericDataSource(source_url="test", input_data=Memory(content=multi_area_data, data_type=DataType.MEMORY))
        )
        transformer = CharterTransformer(charter_source, None)

        items = list(transformer.get_stac_items())
        hazard_items = [item for item in items if MontyExtension.ext(item).is_source_hazard()]

        # 2 disaster types × 2 areas = 4 hazard items
        self.assertEqual(len(hazard_items), 4)

    def test_correlation_id_consistency(self) -> None:
        """Test that all items from same activation share correlation ID"""
        charter_source = CharterDataSource(
            data=GenericDataSource(source_url="test", input_data=Memory(content=json_mock_data, data_type=DataType.MEMORY))
        )
        transformer = CharterTransformer(charter_source, None)

        items = list(transformer.get_stac_items())
        correlation_ids = [MontyExtension.ext(item).correlation_id for item in items]

        # All items should have same correlation ID
        self.assertEqual(len(set(correlation_ids)), 1)
        self.assertIsNotNone(correlation_ids[0])

    def test_validator_integration(self) -> None:
        """Test Charter validator accepts valid data"""
        from pystac_monty.validators.charter import CharterActivation

        # Should not raise validation error
        validated = CharterActivation(**json_mock_data)
        self.assertEqual(validated.properties.disaster_activation_id, "test")
        self.assertEqual(len(validated.areas), 1)


REPO_ROOT = Path(__file__).resolve().parents[2]
CHARTER_MODEL_DIR = REPO_ROOT / "monty-stac-extension" / "docs" / "model" / "sources" / "Charter"
CHARTER_EXAMPLE_EVENT = REPO_ROOT / "monty-stac-extension" / "examples" / "charter-events" / "charter-event-1019.json"


def test_charter_act_1019_matches_extension_example(tmp_path: Path) -> None:
    """Pipeline on submodule Charter docs vs hand-authored example (ids, roles, corr_id, links)."""
    act = CHARTER_MODEL_DIR / "act-1019-activation.json"
    area = CHARTER_MODEL_DIR / "act-1019-area-juiz-de-fora.json"
    if not act.is_file() or not CHARTER_EXAMPLE_EVENT.is_file():
        pytest.skip("monty-stac-extension submodule examples/model not present")

    shutil.copy(act, tmp_path / act.name)
    shutil.copy(area, tmp_path / area.name)

    items = list(iter_charter_stac_items(tmp_path))
    event = next(i for i in items if MontyExtension.ext(i).is_source_event())
    hazards = [i for i in items if MontyExtension.ext(i).is_source_hazard()]

    example = json.loads(CHARTER_EXAMPLE_EVENT.read_text(encoding="utf-8"))

    assert event.id == example["id"]
    assert event.collection_id == example["collection"]
    assert event.properties["roles"] == example["properties"]["roles"]
    assert MontyExtension.ext(event).correlation_id == example["properties"]["monty:corr_id"]
    assert set(MontyExtension.ext(event).hazard_codes or ()) == set(example["properties"]["monty:hazard_codes"])
    assert set(event.properties.get("keywords", [])) == set(example["properties"].get("keywords", []))

    assert any(link.rel == "via" for link in event.links)
    assert any(link.rel == "collection" for link in event.links)

    assert len(hazards) == 2
    assert {h.collection_id for h in hazards} == {"charter-hazards"}
    corr = MontyExtension.ext(event).correlation_id
    assert all(MontyExtension.ext(h).correlation_id == corr for h in hazards)
    assert all(any(link.rel == "derived_from" for link in h.links) for h in hazards)


def test_convert_charter_writes_empty_response_subcatalog(tmp_path: Path) -> None:
    """Batch export uses charter-response (not charter-impacts) and emits an empty response collection."""
    charter_in = tmp_path / "in"
    charter_out = tmp_path / "out"
    charter_in.mkdir()
    shutil.copy(CHARTER_MODEL_DIR / "act-1019-activation.json", charter_in / "act-1019-activation.json")
    shutil.copy(CHARTER_MODEL_DIR / "act-1019-area-juiz-de-fora.json", charter_in / "act-1019-area-juiz-de-fora.json")
    if not (charter_in / "act-1019-activation.json").is_file():
        pytest.skip("monty-stac-extension Charter model not present")

    convert_charter_activations(charter_in, charter_out)

    assert (charter_out / "charter-events").is_dir()
    assert (charter_out / "charter-hazards").is_dir()
    assert (charter_out / "charter-response" / "charter-response.json").is_file()
    assert not (charter_out / "charter-impacts").exists()

    rsp = json.loads((charter_out / "charter-response" / "charter-response.json").read_text(encoding="utf-8"))
    assert rsp["id"] == "charter-response"
    assert rsp.get("stac_extensions") == []
