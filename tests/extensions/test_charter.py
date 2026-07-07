"""Tests for Charter STAC source transformer"""

import datetime
import json
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path

import pytest

from pystac_monty.exporter import MONTY_STAC_EXAMPLES_BASE_URL
from pystac_monty.extension import MontyExtension
from pystac_monty.sources.batch_export import run_batch
from pystac_monty.sources.charter import (
    CharterDataSource,
    CharterTransformer,
    convert_charter_activations,
    iter_charter_stac_items,
)
from pystac_monty.sources.common import DataType, File, GenericDataSource, Memory, sanitize_stac_item_id
from pystac_monty.sources.utils import save_json_data_into_tmp_file
from tests.extensions.test_monty import CustomValidator
from tests.utils.test_utils import validate_correlation_id


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
        "title": "Multi-Hazard Activation",
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


def _synthetic_vap(number: int) -> dict:
    vap = deepcopy(VAP_MOCK)
    vap["id"] = f"act-test-vap-1166-{number}"
    vap["properties"].pop("cpe:cos2_id", None)
    vap["properties"]["cpe:cos2_xml"] = f"<product><identifier>1166-{number}</identifier></product>"
    vap["properties"]["title"] = f"Reference map {number}"
    vap["properties"]["additional_information"] = "General overview map."
    vap["properties"]["copyright"] = ""
    return vap


def _synthetic_dataset(number: int) -> dict:
    return {
        "type": "Feature",
        "id": f"DS_PHR1A_202603021304008_FR1_PX_W044S22_0907_{number:05d}-calibrated",
        "geometry": {"type": "Point", "coordinates": [-43.2, -21.5]},
        "bbox": [-43.2, -21.5, -43.2, -21.5],
        "properties": {
            "datetime": "2024-01-02T00:00:00Z",
            "title": f"Calibrated acquisition {number}",
            "disaster:call_ids": [1166],
            "disaster:type": ["flood", "landslide"],
            "providers": [{"name": "Airbus"}],
        },
        "assets": {},
    }


def _activation_with_full_local_response_bodies() -> dict:
    data = deepcopy(JSON_MOCK_DATA)
    data["areas"] = [
        {
            **deepcopy(JSON_MOCK_DATA["areas"][0]),
            "id": f"area-test-{idx}",
            "properties": {
                **deepcopy(JSON_MOCK_DATA["areas"][0]["properties"]),
                "title": f"Test Area {idx}",
                "description": "Radius (km): 10.0\nPriority: 1",
            },
        }
        for idx in range(4)
    ]
    data["vaps"] = [_synthetic_vap(number) for number in range(1, 13)]
    data["calibrated_datasets"] = [_synthetic_dataset(number) for number in range(1, 160)]
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


def _charter_model_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "monty-stac-extension" / "docs" / "model" / "sources" / "Charter"


def _charter_examples_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "monty-stac-extension" / "examples"


def _role_counts(items):
    counts = {"event": 0, "hazard": 0, "response": 0}
    for item in items:
        monty = MontyExtension.ext(item)
        if monty.is_source_event():
            counts["event"] += 1
        elif monty.is_source_hazard():
            counts["hazard"] += 1
        elif monty.is_source_response():
            counts["response"] += 1
    return counts


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

    def test_full_local_bundle_emits_specified_act1019_counts(self) -> None:
        items = list(_memory_transformer(_activation_with_full_local_response_bodies()).get_stac_items())
        counts = _role_counts(items)
        responses = _responses(items)

        self.assertEqual(counts, {"event": 1, "hazard": 8, "response": 171})
        self.assertEqual(len([item for item in responses if item.id.startswith("charter-response-1019-1166-")]), 12)
        self.assertEqual(len([item for item in responses if item.id.startswith("charter-response-1166-")]), 159)

    def test_api_area_disastertype_limits_hazard_split(self) -> None:
        data = deepcopy(JSON_MOCK_DATA)
        data["areas"][0]["properties"]["disastertype"] = "landslide"

        hazards = [item for item in _memory_transformer(data).get_stac_items() if MontyExtension.ext(item).is_source_hazard()]

        self.assertEqual(len(hazards), 1)
        self.assertEqual(MontyExtension.ext(hazards[0]).hazard_codes, ["MH0901", "LS", "nat-geo-mmd-lan"])

    def test_api_area_search_url_uses_clean_aoi_slug(self) -> None:
        data = deepcopy(JSON_MOCK_DATA)
        data["areas"][0]["id"] = (
            "https://catalog.disasterscharter.org:443//charter/cat/[activationid1019,charterarea]/"
            "search?format=json&uid=Juiz_de_Fora-QVwJEJDB0IZNzAO3SVGtOw__-area-act-1019"
        )

        hazards = [item for item in _memory_transformer(data).get_stac_items() if MontyExtension.ext(item).is_source_hazard()]

        self.assertEqual(
            {hazard.id for hazard in hazards},
            {
                "charter-hazard-1019-juiz_de_fora-qvwjejdb0iznzao3svgtow__-flood",
                "charter-hazard-1019-juiz_de_fora-qvwjejdb0iznzao3svgtow__-landslide",
            },
        )

    def test_hazard_detail_sets_estimate_type_without_severity(self) -> None:
        data = deepcopy(JSON_MOCK_DATA)
        data["properties"]["disaster:type"] = ["flood"]
        data["areas"][0]["properties"]["description"] = "Priority: 1, Comment:"

        hazard = next(item for item in _memory_transformer(data).get_stac_items() if MontyExtension.ext(item).is_source_hazard())
        detail = MontyExtension.ext(hazard).hazard_detail

        self.assertIsNotNone(detail)
        self.assertEqual(detail.estimate_type, "primary")
        self.assertNotIn("severity_value", detail.properties)

    def test_unknown_area_stage_maps_to_primary_estimate(self) -> None:
        data = deepcopy(JSON_MOCK_DATA)
        data["properties"]["disaster:type"] = ["flood"]
        data["areas"][0]["properties"]["cpe:status"] = {"stage": "readyToDeliver"}

        hazard = next(item for item in _memory_transformer(data).get_stac_items() if MontyExtension.ext(item).is_source_hazard())

        self.assertEqual(MontyExtension.ext(hazard).hazard_detail.estimate_type, "primary")

    def test_iter_charter_stac_items_includes_api_sidecars(self) -> None:
        charter_dir = _charter_model_dir()
        if not list(charter_dir.glob("act-*-activation.json")):
            self.skipTest("monty-stac-extension submodule not initialized")

        items = list(iter_charter_stac_items(charter_dir))
        response_ids = {item.id for item in items if MontyExtension.ext(item).is_source_response()}
        counts = _role_counts(items)

        self.assertIn("charter-response-1000-1144-1", response_ids)
        self.assertIn("charter-response-1019-1166-19", response_ids)
        self.assertIn("charter-response-1166-phr1a-0907-00777", response_ids)
        self.assertIn("charter-response-1166-s2b_msil2a_20251228t130249_n0511_r095_t23kqs_20251228t162706", response_ids)
        self.assertIn("charter-response-1166-lc08_l1gt_098169_20260226_20260226_02_rt", response_ids)
        self.assertIn("charter-response-1166-tsx1_sar__eec_re___sl_s_sra_20260228t082140_20260228t082141", response_ids)
        self.assertNotIn("charter-response-1144-phr1a-0907-00777", response_ids)
        self.assertEqual(counts, {"event": 2, "hazard": 9, "response": 7})

        event_1019 = next(item for item in items if item.id == "charter-event-1019")
        event_response_hrefs = {link.get_href() for link in event_1019.links if link.extra_fields.get("roles") == ["response"]}
        self.assertIn("../charter-response/charter-response-1019-1166-19.json", event_response_hrefs)

    def test_charter_listing_counts_are_fixture_bounded(self) -> None:
        charter_dir = _charter_model_dir()
        if not list(charter_dir.glob("act-*-activation.json")):
            self.skipTest("monty-stac-extension submodule not initialized")

        api_dir = charter_dir / "api-files"
        vaps_listing = json.loads((api_dir / "act-1019-vaps.json").read_text(encoding="utf-8"))
        datasets_listing = json.loads((api_dir / "call-1166-calibratedDatasets.json").read_text(encoding="utf-8"))
        areas_listing = json.loads((api_dir / "activation-1019-areas.json").read_text(encoding="utf-8"))

        self.assertEqual(len([link for link in vaps_listing["links"] if link["rel"] == "item"]), 12)
        self.assertEqual(len([link for link in datasets_listing["links"] if link["rel"] == "item"]), 159)
        self.assertEqual(len(areas_listing["features"]), 4)

        items = list(iter_charter_stac_items(charter_dir))
        act1019_hazards = [item for item in items if item.id.startswith("charter-hazard-1019")]
        act1019_responses = [
            item for item in items if item.id.startswith("charter-response-1019") or item.id.startswith("charter-response-1166")
        ]

        self.assertEqual(len(act1019_hazards), 8)
        self.assertEqual(len(act1019_responses), 6)

    def test_convert_charter_activations_uses_static_exporter(self) -> None:
        charter_dir = _charter_model_dir()
        if not list(charter_dir.glob("act-*-activation.json")):
            self.skipTest("monty-stac-extension submodule not initialized")

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            convert_charter_activations(charter_dir, output_dir)

            item_doc = json.loads((output_dir / "charter-events" / "charter-event-1000.json").read_text(encoding="utf-8"))
            collection_doc = json.loads((output_dir / "charter-response" / "charter-response.json").read_text(encoding="utf-8"))

        self.assertEqual(next(link for link in item_doc["links"] if link["rel"] == "self")["href"], "./charter-event-1000.json")
        self.assertEqual(len(collection_doc["extent"]["spatial"]["bbox"]), 1)

    def test_charter_batch_export_dispatches_through_registry(self) -> None:
        charter_dir = _charter_model_dir()
        if not list(charter_dir.glob("act-*-activation.json")):
            self.skipTest("monty-stac-extension submodule not initialized")

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            run_batch("charter", charter_dir, output_dir)

            self.assertTrue((output_dir / "charter-events" / "charter-event-1000.json").is_file())
            self.assertTrue((output_dir / "charter-response" / "charter-response.json").is_file())

    def test_convert_charter_activations_matches_regenerated_examples(self) -> None:
        charter_dir = _charter_model_dir()
        examples_dir = _charter_examples_dir()
        if not list(charter_dir.glob("act-*-activation.json")):
            self.skipTest("monty-stac-extension submodule not initialized")

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            convert_charter_activations(charter_dir, output_dir, public_href_base=MONTY_STAC_EXAMPLES_BASE_URL)

            for collection in ("charter-events", "charter-hazards", "charter-response"):
                expected_files = sorted((examples_dir / collection).glob("*.json"))
                generated_files = sorted((output_dir / collection).glob("*.json"))
                self.assertEqual([path.name for path in generated_files], [path.name for path in expected_files])
                for expected_path in expected_files:
                    generated_path = output_dir / collection / expected_path.name
                    self.assertEqual(
                        json.loads(generated_path.read_text(encoding="utf-8")),
                        json.loads(expected_path.read_text(encoding="utf-8")),
                        expected_path.name,
                    )

    def test_all_fixture_response_items_validate(self) -> None:
        charter_dir = _charter_model_dir()
        if not list(charter_dir.glob("act-*-activation.json")):
            self.skipTest("monty-stac-extension submodule not initialized")

        for item in iter_charter_stac_items(charter_dir):
            if MontyExtension.ext(item).is_source_response():
                item.validate(validator=self.validator)

    def test_calibrated_dataset_response_detail(self) -> None:
        charter_dir = _charter_model_dir()
        if not list(charter_dir.glob("act-*-activation.json")):
            self.skipTest("monty-stac-extension submodule not initialized")

        item = next(item for item in iter_charter_stac_items(charter_dir) if item.id == "charter-response-1166-phr1a-0907-00777")

        detail = item.properties["monty:response_detail"]
        self.assertEqual(detail["type"], "eo-dat")
        self.assertEqual(detail["source_id"], "DS_PHR1A_202603021304008_FR1_PX_W044S22_0907_00777-calibrated")
        self.assertEqual(detail["producer"], "Airbus")
        self.assertEqual(detail["sendai_targets"], ["D", "G"])

    def test_act1019_hazard_codes_and_corr_id_contract(self) -> None:
        charter_dir = _charter_model_dir()
        if not list(charter_dir.glob("act-*-activation.json")):
            self.skipTest("monty-stac-extension submodule not initialized")

        items = list(iter_charter_stac_items(charter_dir))
        event = next(item for item in items if item.id == "charter-event-1019")
        hazards = [item for item in items if item.id.startswith("charter-hazard-1019")]
        responses = [
            item for item in items if item.id.startswith("charter-response-1019") or item.id.startswith("charter-response-1166")
        ]
        event_monty = MontyExtension.ext(event)

        self.assertIn("charter-hazard-1019-juiz_de_fora-qvwjejdb0iznzao3svgtow__-flood", {item.id for item in hazards})
        self.assertNotIn(
            "charter-hazard-1019-search-format-json-uid-juiz_de_fora-qvwjejdb0iznzao3svgtow__-area-act-1019-flood",
            {item.id for item in hazards},
        )
        self.assertEqual(
            event_monty.hazard_codes,
            ["MH0600", "FL", "nat-hyd-flo-flo", "MH0901", "LS", "nat-geo-mmd-lan"],
        )
        validate_correlation_id(event_monty.correlation_id, "MH0600")
        for item in hazards + responses:
            monty = MontyExtension.ext(item)
            self.assertEqual(monty.correlation_id, event_monty.correlation_id)
        for hazard in hazards:
            undrr_codes = [code for code in MontyExtension.ext(hazard).hazard_codes or [] if code.startswith(("MH", "GH", "TH"))]
            self.assertEqual(len(undrr_codes), 1)

    def test_vap_eodat_sibling_links_are_bidirectional(self) -> None:
        charter_dir = _charter_model_dir()
        if not list(charter_dir.glob("act-*-activation.json")):
            self.skipTest("monty-stac-extension submodule not initialized")

        items = {item.id: item for item in iter_charter_stac_items(charter_dir)}
        vap = items["charter-response-1019-1166-19"]
        eodat = items["charter-response-1166-phr1a-0907-00777"]

        vap_response_links = [link.get_href() for link in vap.links if link.extra_fields.get("roles") == ["response"]]
        eodat_response_links = [link.get_href() for link in eodat.links if link.extra_fields.get("roles") == ["response"]]

        self.assertIn("./charter-response-1166-phr1a-0907-00777.json", vap_response_links)
        self.assertIn("./charter-response-1019-1166-19.json", eodat_response_links)
        derived = [link for link in vap.links if link.rel == "derived_from"]
        self.assertEqual(derived[0].media_type, "text/html")

    def test_calibrated_dataset_uses_matching_call_id_and_omits_null_producer(self) -> None:
        data = deepcopy(JSON_MOCK_DATA)
        data["properties"]["disaster:call_ids"] = [111, 222]
        data["calibrated_datasets"] = [
            {
                "type": "Feature",
                "id": "DS_PHR1A_202603021304008_FR1_PX_W044S22_0907_00777-calibrated",
                "geometry": {"type": "Point", "coordinates": [-43.2, -21.5]},
                "bbox": [-43.2, -21.5, -43.2, -21.5],
                "properties": {
                    "datetime": "2024-01-02T00:00:00Z",
                    "title": "Calibrated acquisition",
                    "disaster:call_ids": [222],
                    "disaster:type": ["flood"],
                    "providers": [],
                },
                "assets": {},
            }
        ]

        response = _responses(_memory_transformer(data).get_stac_items())[0]
        detail = response.properties["monty:response_detail"]

        self.assertEqual(response.id, "charter-response-222-phr1a-0907-00777")
        self.assertNotIn("producer", detail)

    def test_vap_call_id_is_derived_from_source_identifier(self) -> None:
        data = _activation_with_vaps()
        data["properties"]["disaster:call_ids"] = [111, 222]
        data["vaps"][0]["id"] = "act-test-vap-222-5"
        data["vaps"][0]["properties"].pop("cpe:cos2_id", None)
        data["vaps"][0]["properties"]["cpe:cos2_xml"] = "<product><identifier>222-5</identifier></product>"

        response = _responses(_memory_transformer(data).get_stac_items())[0]

        self.assertEqual(response.id, "charter-response-1019-222-5")
        self.assertEqual(response.properties["disaster:call_ids"], [222])
        self.assertEqual(response.properties["monty:response_detail"]["source_id"], "222-5")

    def test_no_vaps_yields_no_response_items(self) -> None:
        responses = _responses(_memory_transformer().get_stac_items())
        self.assertEqual(responses, [])

    @pytest.mark.vcr()
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
        self.assertEqual(related_roles.count(["hazard"]), 1)
        derived = [link for link in response.links if link.rel == "derived_from"]
        self.assertEqual(len(derived), 1)
        self.assertEqual(derived[0].media_type, "text/html")

    def test_response_skipped_without_source_id(self) -> None:
        data = _activation_with_vaps()
        data["vaps"][0]["properties"].pop("cpe:cos2_id")
        data["vaps"][0]["id"] = "no-identifiable-id"
        self.assertEqual(_responses(_memory_transformer(data).get_stac_items()), [])

    def test_vap_source_id(self) -> None:
        self.assertEqual(
            CharterTransformer._vap_source_id(
                {"properties": {"cpe:cos2_xml": "<product><identifier>1166-19</identifier></product>"}}
            ),
            "1166-19",
        )
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
        self.assertEqual(RESPONSE_TYPE_SENDAI["eo-ref"], ["G"])
        self.assertEqual(RESPONSE_TYPE_SENDAI["eo-fep"], ["D", "G"])
        self.assertEqual(RESPONSE_TYPE_SENDAI["eo-mon"], ["D", "G"])
        self.assertEqual(RESPONSE_TYPE_SENDAI["eo-pop"], ["B"])
        self.assertEqual(RESPONSE_TYPE_SENDAI["eo-vap"], ["D", "G"])
        self.assertEqual(RESPONSE_TYPE_SENDAI["eo-sr"], ["G"])

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
        self.assertEqual(CharterTransformer._infer_producer("Maxar WorldView"), "Maxar")
        self.assertEqual(CharterTransformer._infer_producer("Sentinel-2 Copernicus"), "ESA/EC (Copernicus)")
        self.assertEqual(CharterTransformer._infer_producer("Landsat USGS/NASA"), "USGS/NASA")
        self.assertEqual(CharterTransformer._infer_producer("TerraSAR-X DLR"), "DLR")
        self.assertIsNone(CharterTransformer._infer_producer(""))
        self.assertEqual(CharterTransformer._infer_resolution_class("Pleiades / CNES"), "VHR")
        self.assertEqual(CharterTransformer._infer_resolution_class("SPOT"), "HR")
        self.assertEqual(CharterTransformer._infer_resolution_class("Sentinel-2"), "MR")

    def test_other_disaster_type_requires_manual_review(self) -> None:
        self.assertEqual(CharterTransformer._manual_review_disaster_types(["other"]), ["other"])
        self.assertIsNone(
            _memory_transformer().make_event_item(
                {
                    "properties": {
                        "disaster:activation_id": "manual",
                        "disaster:type": ["other"],
                    }
                }
            )
        )

    def test_string_disaster_type_is_normalized(self) -> None:
        data = deepcopy(JSON_MOCK_DATA)
        data["properties"]["disaster:type"] = "flood"
        items = list(_memory_transformer(data).get_stac_items())
        hazards = [item for item in items if MontyExtension.ext(item).is_source_hazard()]
        self.assertEqual(len(hazards), 1)

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

        self.assertIsNone(
            transformer.make_event_item(
                {
                    "properties": {
                        "disaster:activation_id": "x",
                        "disaster:type": ["flood"],
                        "disaster:country": "BRA",
                        "datetime": "2024-01-01T00:00:00Z",
                        "title": "Test activation for internal QA",
                    },
                    "geometry": point,
                    "bbox": [0, 0, 0, 0],
                }
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
        charter_dir = _charter_model_dir()
        if not list(charter_dir.glob("act-*-activation.json")):
            self.skipTest("monty-stac-extension submodule not initialized")
        roles = [item.properties.get("roles", []) for item in iter_charter_stac_items(charter_dir)]
        self.assertTrue(any("event" in role for role in roles))
        self.assertTrue(any("hazard" in role for role in roles))
        self.assertTrue(any("response" in role for role in roles))
