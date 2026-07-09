"""Tests for CEMS STAC source transformer."""

from __future__ import annotations

import json
import re
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path

import pytest

from pystac_monty.exporter import MONTY_SOURCE_DATETIME_PROPERTY
from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import MontyGeoCoder, NoopMontyGeocoder
from pystac_monty.sources.batch_export import run_batch
from pystac_monty.sources.cems import (
    CURATED_CEMS_EXAMPLE_IDS,
    CEMSDataSource,
    CEMSTransformer,
    _country_codes,
    _iso3_from_country_name,
    default_cems_export_geocoder,
    iter_cems_stac_items,
    regenerate_cems_examples,
    resolve_gdacs_current_episode,
)
from pystac_monty.sources.common import DataType, GenericDataSource, Memory
from tests.extensions.test_monty import CustomValidator
from tests.utils.test_utils import validate_correlation_id

RFC3339_UTC_PATTERN = re.compile(r"(\+00:00|Z)$")


@pytest.fixture(scope="module")
def vcr_config():
    return {"cassette_library_dir": "tests/extensions/cassettes/test_cems"}


MINIMAL_ACTIVATION = {
    "code": "EMSR999",
    "name": "Test flood in Exampleland",
    "reason": "River flooding after heavy rainfall.",
    "category": "Flood",
    "subCategory": "Riverine flood",
    "eventTime": "2026-01-15T10:00:00",
    "centroid": "POINT (12.5 41.9)",
    "extent": "POLYGON ((12.0 41.5, 13.0 41.5, 13.0 42.5, 12.0 42.5, 12.0 41.5))",
    "countries": [{"name": "Italy"}],
    "reportLink": "https://storymaps.arcgis.com/stories/example",
    "gdacsId": None,
    "charterNumber": None,
    "relatedevents": [],
    "aois": [
        {
            "name": "Example AOI",
            "number": 1,
            "extent": "POLYGON ((12.1 41.6, 12.9 41.6, 12.9 42.4, 12.1 42.4, 12.1 41.6))",
            "products": [
                {
                    "type": "DEL",
                    "monitoring": False,
                    "monitoringNumber": 0,
                    "extent": "POLYGON ((12.2 41.7, 12.8 41.7, 12.8 42.3, 12.2 42.3, 12.2 41.7))",
                    "downloadPath": "https://example.test/EMSR999/AOI01/DEL_PRODUCT.zip",
                    "layers": [
                        {
                            "name": "EMSR999/AOI01/DEL_PRODUCT/example_cog.tif",
                            "format": "cog",
                        }
                    ],
                    "version": {
                        "statusCode": "F",
                        "deliveryTime": "2026-01-16T12:00:00",
                        "reason": "",
                    },
                },
                {
                    "type": "GRA",
                    "monitoring": False,
                    "monitoringNumber": 0,
                    "extent": "POLYGON ((12.2 41.7, 12.8 41.7, 12.8 42.3, 12.2 42.3, 12.2 41.7))",
                    "downloadPath": "https://example.test/EMSR999/AOI01/GRA_PRODUCT.zip",
                    "layers": [
                        {
                            "name": "EMSR999/AOI01/GRA_PRODUCT/example_cog.tif",
                            "format": "cog",
                        }
                    ],
                    "stats": {
                        "Estimated population": {"None": {"total": 1200}},
                        "Flooded area": {"None": {"affected": 42.0, "unit": "ha"}},
                        "Maximum of all extents**": {"None": {"affected": 42.0}},
                    },
                    "version": {
                        "statusCode": "F",
                        "deliveryTime": "2026-01-17T12:00:00",
                        "reason": "",
                    },
                },
            ],
        }
    ],
}


def _memory_transformer(data: dict | None = None) -> CEMSTransformer:
    payload = {"results": [data or MINIMAL_ACTIVATION]}
    source = CEMSDataSource(
        data=GenericDataSource(
            source_url="https://rapidmapping.emergency.copernicus.eu/backend/dashboard-api/public-activations/?code=EMSR999",
            input_data=Memory(content=payload, data_type=DataType.MEMORY),
        )
    )
    return CEMSTransformer(source, None)


def _partition(items):
    event, hazards, responses, impacts = None, [], [], []
    for item in items:
        monty = MontyExtension.ext(item)
        if monty.is_source_event():
            event = item
        elif monty.is_source_hazard():
            hazards.append(item)
        elif monty.is_source_response():
            responses.append(item)
        elif monty.is_source_impact():
            impacts.append(item)
    return event, hazards, responses, impacts


def _cems_fixture_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "monty-stac-extension" / "docs" / "model" / "sources" / "CEMS" / "api-files"


def _cems_examples_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "monty-stac-extension" / "examples"


def _collection_for_item_id(item_id: str) -> str:
    if item_id.startswith("cems-event-"):
        return "cems-events"
    if item_id.startswith("cems-hazard-"):
        return "cems-hazards"
    if item_id.startswith("cems-response-"):
        return "cems-response"
    return "cems-impacts"


def _assert_rfc3339_utc_datetime(value: str) -> None:
    assert RFC3339_UTC_PATTERN.search(value), value


class CEMSTest(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.validator = CustomValidator()

    def test_transformer_with_mock_data(self) -> None:
        items = list(_memory_transformer().get_stac_items())
        for item in items:
            item.validate(validator=self.validator)

        event, hazards, responses, impacts = _partition(items)
        self.assertIsNotNone(event)
        self.assertEqual(event.collection_id, "cems-events")
        self.assertEqual(len(hazards), 2)
        self.assertGreaterEqual(len(responses), 3)
        self.assertEqual(len(impacts), 1)

        monty_event = MontyExtension.ext(event)
        self.assertEqual(monty_event.country_codes, ["ITA"])
        self.assertIn("MH0604", monty_event.hazard_codes or [])
        validate_correlation_id(monty_event.correlation_id, "MH0604")

        corr = monty_event.correlation_id
        for item in hazards + responses + impacts:
            self.assertEqual(MontyExtension.ext(item).correlation_id, corr)

        for hazard in hazards:
            event_links = [
                link
                for link in hazard.links
                if link.rel in {"derived_from", "related"} and "cems-events/cems-event-EMSR999.json" in (link.get_href() or "")
            ]
            self.assertEqual(len(event_links), 1)
            self.assertEqual(event_links[0].rel, "related")
            self.assertEqual(event_links[0].extra_fields.get("roles"), ["event"])

        gra = next(item for item in responses if item.id.endswith("-gra"))
        self.assertEqual(gra.properties["monty:response_detail"]["type"], "eo-gra")
        self.assertEqual(gra.properties["monty:response_detail"]["status"], "published")
        self.assertTrue(gra.assets)
        activation_links = [
            link for link in gra.links if (link.get_href() or "") == "https://rapidmapping.emergency.copernicus.eu/EMSR999"
        ]
        self.assertEqual(len(activation_links), 1)
        self.assertEqual(activation_links[0].rel, "derived_from")

        impact = impacts[0]
        self.assertEqual(impact.properties["monty:impact_detail"]["category"], "people")
        self.assertEqual(impact.properties["monty:impact_detail"]["value"], 1200.0)
        derived = [link for link in impact.links if link.rel == "derived_from"]
        self.assertEqual(len(derived), 1)
        self.assertEqual(derived[0].extra_fields.get("roles"), ["response"])

    def test_delivery_datetime_normalized(self) -> None:
        data = deepcopy(MINIMAL_ACTIVATION)
        data["aois"][0]["products"][1]["version"]["deliveryTime"] = "2025-11-10T23:16:39.188570"
        responses = [
            item
            for item in _memory_transformer(data).get_stac_items()
            if MontyExtension.ext(item).is_source_response() and item.id.endswith("-gra")
        ]
        gra = responses[0]
        exported_datetime = gra.properties.get(MONTY_SOURCE_DATETIME_PROPERTY) or gra.properties.get("datetime")
        self.assertIsInstance(exported_datetime, str)
        _assert_rfc3339_utc_datetime(exported_datetime)

    def test_response_and_impact_use_aoi_local_country_codes(self) -> None:
        data = deepcopy(MINIMAL_ACTIVATION)
        data["centroid"] = "POINT (-3.0 40.0)"
        data["extent"] = "POLYGON ((-8.0 37.0, 2.0 37.0, 2.0 43.0, -8.0 43.0, -8.0 37.0))"
        data["aois"][0]["extent"] = "POLYGON ((-7.5 37.5, 1.5 37.5, 1.5 42.5, -7.5 42.5, -7.5 37.5))"
        for product in data["aois"][0]["products"]:
            product["extent"] = data["aois"][0]["extent"]
        data["countries"] = [{"name": "Haiti"}, {"name": "Cuba"}, {"name": "Spain"}, {"name": "Jamaica"}]
        items = list(_memory_transformer(data).get_stac_items())
        _, hazards, responses, impacts = _partition(items)
        hazard = next(item for item in hazards if item.id.endswith("-flood"))
        gra = next(item for item in responses if item.id.endswith("-gra"))
        impact = impacts[0]

        self.assertEqual(MontyExtension.ext(hazard).country_codes[0], "ESP")
        self.assertEqual(MontyExtension.ext(gra).country_codes[0], "ESP")
        self.assertEqual(MontyExtension.ext(impact).country_codes[0], "ESP")
        event_corr = MontyExtension.ext(next(item for item in items if MontyExtension.ext(item).is_source_event())).correlation_id
        self.assertEqual(MontyExtension.ext(gra).correlation_id, event_corr)
        self.assertEqual(MontyExtension.ext(impact).correlation_id, event_corr)

    def test_country_name_resolution_uses_pycountry(self) -> None:
        geocoder = NoopMontyGeocoder()
        self.assertEqual(_iso3_from_country_name("Jamaica", geocoder), "JAM")
        self.assertEqual(_iso3_from_country_name("Italy", geocoder), "ITA")
        self.assertEqual(_iso3_from_country_name("United States of America", geocoder), "USA")
        self.assertEqual(
            _country_codes([{"name": "Haiti"}, {"name": "Cuba"}, {"name": "Jamaica"}], geocoder),
            ["HTI", "CUB", "JAM"],
        )

    def test_country_name_resolution_treats_geocoder_unk_as_unresolved(self) -> None:
        class UnkGeocoder(MontyGeoCoder):
            def get_geometry_from_admin_units(self, admin_units: str, simplified: bool) -> dict | None:
                return None

            def get_geometry_by_country_name(self, country_name: str, simplified: bool = False) -> dict | None:
                return {"iso3": "UNK"}

            def get_iso3_from_point(self, point) -> str | None:
                return "UNK"

            def get_iso3_from_geometry(self, geometry: dict) -> str | None:
                return "UNK"

            def get_geometry_from_iso3(self, iso3: str, simplified: bool = False) -> dict | None:
                return None

        self.assertIsNone(_iso3_from_country_name("Atlantis", UnkGeocoder()))

    def test_fixture_emsm847_primary_country_and_corr_id(self) -> None:
        fixture = _cems_fixture_dir() / "EMSR847-storm-detail.json"
        if not fixture.is_file():
            self.skipTest("monty-stac-extension submodule not initialized")

        geocoder = default_cems_export_geocoder()
        items = list(iter_cems_stac_items(fixture, geocoder=geocoder))
        event = next(item for item in items if item.id == "cems-event-EMSR847")
        hazard = next(item for item in items if item.id == "cems-hazard-EMSR847-aoi01-storm")
        impact = next(item for item in items if item.id == "cems-impact-EMSR847-aoi01-gra-population")

        event_monty = MontyExtension.ext(event)
        hazard_monty = MontyExtension.ext(hazard)
        impact_monty = MontyExtension.ext(impact)

        self.assertEqual(event_monty.country_codes[0], "JAM")
        self.assertEqual(hazard_monty.country_codes[0], "JAM")
        self.assertEqual(impact_monty.country_codes[0], "JAM")
        self.assertTrue(event_monty.correlation_id.startswith("20251026-JAM-"))
        self.assertEqual(hazard_monty.correlation_id, event_monty.correlation_id)
        self.assertEqual(impact_monty.correlation_id, event_monty.correlation_id)

    def test_status_no_impact_mapping(self) -> None:
        data = deepcopy(MINIMAL_ACTIVATION)
        data["aois"][0]["products"].append(
            {
                "type": "DEL",
                "monitoring": True,
                "monitoringNumber": 1,
                "extent": data["aois"][0]["extent"],
                "version": {
                    "statusCode": "N",
                    "deliveryTime": "2026-01-18T12:00:00",
                    "reason": "Because of no change of situation detected",
                },
            }
        )
        responses = [item for item in _memory_transformer(data).get_stac_items() if MontyExtension.ext(item).is_source_response()]
        monitoring = next(item for item in responses if item.id.endswith("-del-m1"))
        self.assertEqual(monitoring.properties["monty:response_detail"]["status"], "no-impact")

    def test_gdacs_episode_parser_uses_feature_properties(self) -> None:
        class FakeResponse:
            def raise_for_status(self) -> None:
                pass

            def json(self) -> dict:
                return {"type": "Feature", "properties": {"eventtype": "TC", "eventid": 1001230, "episodeid": 41}}

        with unittest.mock.patch("pystac_monty.sources.cems.requests.get", return_value=FakeResponse()):
            self.assertEqual(resolve_gdacs_current_episode("TC1001230"), 41)

    def test_footprint_class_adds_secondary_hazard_detail(self) -> None:
        data = deepcopy(MINIMAL_ACTIVATION)
        data["category"] = "Storm"
        data["subCategory"] = "Tropical cyclone, hurricane, typhoon"
        data["aois"][0]["products"][1]["stats"] = {
            "Landslide": {"None": {"affected": 3, "unit": ""}},
            "Estimated population": {"None": {"total": 1200}},
        }

        items = list(_memory_transformer(data).get_stac_items())
        _, hazards, _, _ = _partition(items)
        landslide = next(item for item in hazards if item.id.endswith("-landslide"))
        detail = MontyExtension.ext(landslide).hazard_detail
        self.assertEqual(detail.severity_value, 3)
        self.assertEqual(detail.severity_unit, "count")
        self.assertEqual(detail.severity_label, "Landslide")

    def test_impact_stats_emit_one_item_per_thematic_class(self) -> None:
        data = deepcopy(MINIMAL_ACTIVATION)
        data["aois"][0]["products"][1]["stats"]["Facilities"] = {
            "School": {"affected": 2, "unit": "count"},
            "Hospital": {"affected": 1, "unit": "count"},
        }

        items = list(_memory_transformer(data).get_stac_items())
        _, _, _, impacts = _partition(items)
        facilities = [item for item in impacts if item.id.endswith("-facilities")]
        self.assertEqual(len(facilities), 1)
        self.assertEqual(MontyExtension.ext(facilities[0]).impact_detail.value, 3)

    def test_monitoring_prev_links(self) -> None:
        data = deepcopy(MINIMAL_ACTIVATION)
        data["aois"][0]["products"].extend(
            [
                {
                    "type": "DEL",
                    "monitoring": True,
                    "monitoringNumber": 1,
                    "extent": data["aois"][0]["extent"],
                    "version": {
                        "statusCode": "F",
                        "deliveryTime": "2026-01-18T12:00:00",
                        "reason": "",
                    },
                },
                {
                    "type": "DEL",
                    "monitoring": True,
                    "monitoringNumber": 2,
                    "extent": data["aois"][0]["extent"],
                    "version": {
                        "statusCode": "F",
                        "deliveryTime": "2026-01-19T12:00:00",
                        "reason": "",
                    },
                },
            ]
        )
        responses = [item for item in _memory_transformer(data).get_stac_items() if MontyExtension.ext(item).is_source_response()]
        mon2 = next(item for item in responses if item.id.endswith("-del-m2"))
        prev_links = [link for link in mon2.links if link.rel == "prev"]
        self.assertEqual(len(prev_links), 1)
        self.assertTrue(prev_links[0].target.id.endswith("-del-m1"))

    def test_fixture_emsm871_exports_roles(self) -> None:
        fixture = _cems_fixture_dir() / "EMSR871-flood-detail.json"
        if not fixture.is_file():
            self.skipTest("monty-stac-extension submodule not initialized")

        items = list(iter_cems_stac_items(fixture))
        event, hazards, responses, impacts = _partition(items)
        self.assertIsNotNone(event)
        self.assertGreater(len(hazards), 0)
        self.assertGreater(len(responses), 0)
        self.assertGreater(len(impacts), 0)

        monitoring_del = [item for item in responses if item.id.endswith("-del-m2")]
        self.assertTrue(monitoring_del)

    def test_fixture_emsm842_minimal_activation(self) -> None:
        fixture = _cems_fixture_dir() / "EMSR842-wildfire-detail.json"
        if not fixture.is_file():
            self.skipTest("monty-stac-extension submodule not initialized")

        items = list(iter_cems_stac_items(fixture))
        event, hazards, responses, impacts = _partition(items)
        self.assertIsNotNone(event)
        self.assertEqual(event.id, "cems-event-EMSR842")
        self.assertGreater(len(responses), 0)
        self.assertGreater(len(impacts), 0)
        self.assertIn("MH1301", MontyExtension.ext(event).hazard_codes or [])

    @pytest.mark.vcr()
    def test_fixture_emsm847_cross_source_links(self) -> None:
        fixture = _cems_fixture_dir() / "EMSR847-storm-detail.json"
        if not fixture.is_file():
            self.skipTest("monty-stac-extension submodule not initialized")

        event = next(item for item in iter_cems_stac_items(fixture) if MontyExtension.ext(item).is_source_event())
        related_hrefs = [link.get_href() for link in event.links if link.rel == "related"]
        self.assertTrue(any("gdacs-events/1001230-" in href for href in related_hrefs))
        self.assertTrue(any("charter-events/charter-event-996.json" in href for href in related_hrefs))
        self.assertTrue(any(link.rel == "via" for link in event.links))

    def test_cems_batch_export_dispatches_through_registry(self) -> None:
        fixture_dir = _cems_fixture_dir()
        if not list(fixture_dir.glob("*-detail.json")):
            self.skipTest("monty-stac-extension submodule not initialized")

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            run_batch("cems", fixture_dir / "EMSR842-wildfire-detail.json", output_dir)
            self.assertTrue((output_dir / "cems-events" / "cems-event-EMSR842.json").is_file())
            self.assertTrue((output_dir / "cems-response" / "cems-response.json").is_file())

    def test_convert_cems_matches_regenerated_examples(self) -> None:
        fixture = _cems_fixture_dir() / "EMSR847-storm-detail.json"
        examples_dir = _cems_examples_dir()
        if not fixture.is_file():
            self.skipTest("monty-stac-extension submodule not initialized")

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            regenerate_cems_examples(fixture, output_dir)

            for item_id in CURATED_CEMS_EXAMPLE_IDS:
                collection = _collection_for_item_id(item_id)
                expected_path = examples_dir / collection / f"{item_id}.json"
                generated_path = output_dir / collection / f"{item_id}.json"
                self.assertTrue(generated_path.is_file(), item_id)
                self.assertTrue(expected_path.is_file(), item_id)
                self.assertEqual(
                    json.loads(generated_path.read_text(encoding="utf-8")),
                    json.loads(expected_path.read_text(encoding="utf-8")),
                    item_id,
                )

    def test_curated_examples_collection_links_match_files(self) -> None:
        examples_dir = _cems_examples_dir()
        if not (examples_dir / "cems-events" / "cems-events.json").is_file():
            self.skipTest("monty-stac-extension submodule not initialized")

        for collection in ("cems-events", "cems-hazards", "cems-response", "cems-impacts"):
            collection_path = examples_dir / collection / f"{collection}.json"
            collection_doc = json.loads(collection_path.read_text(encoding="utf-8"))
            item_hrefs = [link["href"] for link in collection_doc["links"] if link["rel"] == "item"]
            item_files = sorted(
                path.name for path in (examples_dir / collection).glob("*.json") if path.name != f"{collection}.json"
            )
            linked_files = sorted(Path(href).name for href in item_hrefs)
            self.assertEqual(linked_files, item_files, collection)
            expected_count = 2 if collection == "cems-hazards" else 1
            self.assertEqual(len(item_files), expected_count, collection)

    def test_curated_event_related_links_are_bounded(self) -> None:
        examples_dir = _cems_examples_dir()
        event_path = examples_dir / "cems-events" / "cems-event-EMSR847.json"
        if not event_path.is_file():
            self.skipTest("monty-stac-extension submodule not initialized")

        event_doc = json.loads(event_path.read_text(encoding="utf-8"))
        related_links = [link for link in event_doc["links"] if link["rel"] == "related"]
        self.assertLessEqual(len(related_links), 6)
        related_hrefs = [link["href"] for link in related_links]
        self.assertTrue(any("gdacs-events/" in href for href in related_hrefs))
        self.assertTrue(any("charter-events/" in href for href in related_hrefs))
        self.assertTrue(any("cems-hazard-EMSR847-aoi01-storm.json" in href for href in related_hrefs))
        self.assertTrue(any("cems-hazard-EMSR847-aoi01-landslide.json" in href for href in related_hrefs))
        self.assertTrue(any("cems-response-EMSR847-aoi01-gra.json" in href for href in related_hrefs))
        self.assertTrue(any("cems-impact-EMSR847-aoi01-gra-population.json" in href for href in related_hrefs))

    def test_curated_response_datetime_is_valid(self) -> None:
        examples_dir = _cems_examples_dir()
        response_path = examples_dir / "cems-response" / "cems-response-EMSR847-aoi01-gra.json"
        if not response_path.is_file():
            self.skipTest("monty-stac-extension submodule not initialized")

        response_doc = json.loads(response_path.read_text(encoding="utf-8"))
        _assert_rfc3339_utc_datetime(response_doc["properties"]["datetime"])

    @pytest.mark.vcr()
    def test_resolve_gdacs_current_episode(self) -> None:
        episode = resolve_gdacs_current_episode("TC1001230")
        self.assertIsNotNone(episode)
        self.assertGreater(episode, 0)
