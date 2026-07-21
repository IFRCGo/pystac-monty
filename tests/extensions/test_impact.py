"""Tests for the Response->Impact pairing"""

import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pystac
import pytest
from pystac.validation import JsonSchemaSTACValidator

from pystac_monty.extension import (
    SCHEMA_URI,
    MontyEstimateType,
    MontyExtension,
    MontyImpactExposureCategory,
    MontyImpactType,
)
from pystac_monty.impact import build_impact_from_response, build_impacts_from_response, link_derived_from_response
from pystac_monty.response import build_response_item
from tests.utils.test_cases import ARBITRARY_BBOX, ARBITRARY_GEOM

CURRENT_SCHEMA_URI = SCHEMA_URI
SUBMODULE_ROOT = Path(__file__).resolve().parents[2] / "monty-stac-extension"
SUBMODULE_SCHEMA_PATH = SUBMODULE_ROOT / "json-schema" / "schema.json"

PAIRING_FIXTURE_ROOT = SUBMODULE_ROOT / "examples" / "_response-impact-pairing"
RESPONSE_FIXTURE_PATH = PAIRING_FIXTURE_ROOT / "response-EMSR-DEMO-001-GRA.json"
IMPACT_FIXTURE_PATH = PAIRING_FIXTURE_ROOT / "impact-EMSR-DEMO-001-buildings-destroyed.json"


def _load_submodule_schema() -> dict[str, Any]:
    with SUBMODULE_SCHEMA_PATH.open(encoding="utf-8") as f:
        return json.load(f)


class CustomValidator(JsonSchemaSTACValidator):
    _schema_cache = None

    def _get_schema(self, schema_uri: str) -> dict[str, Any]:
        if schema_uri == CURRENT_SCHEMA_URI:
            if self._schema_cache is None:
                self.__class__._schema_cache = _load_submodule_schema()
            return self._schema_cache
        return super()._get_schema(schema_uri)


def make_response_item(**overrides: Any):
    kwargs = dict(
        id="test-response",
        geometry=ARBITRARY_GEOM,
        bbox=ARBITRARY_BBOX,
        datetime=datetime(2026, 6, 15, tzinfo=timezone.utc),
        correlation_id="20260615T000000Z-ESP-FL-001-GCDB",
        country_codes=["ESP"],
        hazard_codes=["FL"],
        type="eo-del",
        source_id="EMSR744",
        producer="JRC",
    )
    kwargs.update(overrides)
    item = build_response_item(**kwargs)
    item.set_self_href(f"./{item.id}.json")
    return item


class BuildImpactFromResponseTest(unittest.TestCase):
    def test_sets_core_properties(self) -> None:
        response_item = make_response_item()
        impact_item = build_impact_from_response(
            response_item,
            MontyImpactExposureCategory.ALL_PEOPLE,
            MontyImpactType.TOTAL_AFFECTED,
            1200,
            unit="people",
            estimate_type=MontyEstimateType.PRIMARY,
        )

        self.assertEqual(impact_item.properties["roles"], ["impact"])
        self.assertEqual(impact_item.properties["monty:corr_id"], response_item.properties["monty:corr_id"])
        self.assertEqual(impact_item.properties["monty:country_codes"], ["ESP"])
        self.assertEqual(impact_item.properties["monty:hazard_codes"], ["FL"])
        self.assertEqual(
            impact_item.properties["monty:impact_detail"],
            {
                "category": "people",
                "type": "affected_total",
                "value": 1200,
                "unit": "people",
                "estimate_type": "primary",
            },
        )
        self.assertEqual(impact_item.geometry, response_item.geometry)
        self.assertEqual(impact_item.bbox, response_item.bbox)
        self.assertEqual(impact_item.datetime, response_item.datetime)

    def test_adds_derived_from_link_to_response(self) -> None:
        response_item = make_response_item()
        impact_item = build_impact_from_response(response_item, MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.DEATH, 12)

        derived_links = [link for link in impact_item.links if link.rel == "derived_from"]
        self.assertEqual(len(derived_links), 1)
        self.assertIs(derived_links[0].target, response_item)
        self.assertEqual(derived_links[0].extra_fields["roles"], ["response"])

    def test_adds_related_link_to_response(self) -> None:
        response_item = make_response_item()
        impact_item = build_impact_from_response(response_item, MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.DEATH, 12)

        related_links = [link for link in response_item.links if link.rel == "related"]
        self.assertEqual(len(related_links), 1)
        self.assertIs(related_links[0].target, impact_item)
        self.assertEqual(related_links[0].extra_fields["roles"], ["impact"])

    def test_related_link_is_not_duplicated_on_repeated_calls(self) -> None:
        """Test related_links with same parameters on response_item."""
        response_item = make_response_item()
        build_impact_from_response(
            response_item, MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.DEATH, 12, id="impact-fixed-id"
        )
        build_impact_from_response(
            response_item, MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.DEATH, 12, id="impact-fixed-id"
        )

        related_links = [link for link in response_item.links if link.rel == "related"]
        self.assertEqual(len(related_links), 1)

    def test_does_not_set_self_href(self) -> None:
        """Setting the self href is left to the caller/exporter, not the builder."""
        response_item = make_response_item()
        impact_item = build_impact_from_response(response_item, MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.DEATH, 12)
        self.assertIsNone(impact_item.get_self_href())

    def test_link_derived_from_response_helper(self) -> None:
        response_item = make_response_item()
        impact_item = build_impact_from_response(response_item, MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.DEATH, 12)
        impact_item.links = [link for link in impact_item.links if link.rel != "derived_from"]

        link_derived_from_response(impact_item, response_item)

        self.assertEqual([link.rel for link in impact_item.links if link.rel != "self"], ["derived_from"])

    def test_id_defaults_from_response_source_id_category_and_type(self) -> None:
        """Matches the item naming as in examples/_response-impact-pairing examples."""
        response_item = make_response_item(id="response-EMSR-DEMO-001-GRA", source_id="EMSR-DEMO-001")
        impact_item = build_impact_from_response(
            response_item, MontyImpactExposureCategory.BUILDINGS, MontyImpactType.DESTROYED, 87
        )
        self.assertEqual(impact_item.id, "impact-EMSR-DEMO-001-buildings-destroyed")

    def test_id_falls_back_to_response_id_without_source_id(self) -> None:
        response_item = make_response_item(id="response-charter-1000-1144-GRA", source_id=None)
        impact_item = build_impact_from_response(
            response_item, MontyImpactExposureCategory.BUILDINGS, MontyImpactType.DAMAGED, 42
        )
        self.assertEqual(impact_item.id, "impact-charter-1000-1144-buildings-damaged")

    def test_id_override(self) -> None:
        """Use the id if it is available."""
        response_item = make_response_item()
        impact_item = build_impact_from_response(
            response_item, MontyImpactExposureCategory.BUILDINGS, MontyImpactType.DAMAGED, 42, id="custom-impact-id"
        )
        self.assertEqual(impact_item.id, "custom-impact-id")

    def test_id_fallback_handles_short_response_id_without_double_hyphen(self) -> None:
        """A response id with fewer than 3 segments has nothing to strip; fall back to the
        full response id rather than an empty base (which would yield a double hyphen)."""
        response_item = make_response_item(id="test-response", source_id=None)
        impact_item = build_impact_from_response(
            response_item, MontyImpactExposureCategory.BUILDINGS, MontyImpactType.DESTROYED, 1
        )
        self.assertEqual(impact_item.id, "impact-test-response-buildings-destroyed")

    def test_ids_collide_when_responses_share_source_id(self) -> None:
        """Documents a known limitation: source_id is not guaranteed unique per Response
        (e.g. a shared CEMS activation code), so the derived id can collide. Callers in
        that situation must pass an explicit id= per Response."""
        response_1 = make_response_item(id="response-EMSR847-aoi01-gra", source_id="EMSR847")
        response_2 = make_response_item(id="response-EMSR847-aoi02-gra", source_id="EMSR847")

        impact_1 = build_impact_from_response(response_1, MontyImpactExposureCategory.BUILDINGS, MontyImpactType.DESTROYED, 1)
        impact_2 = build_impact_from_response(response_2, MontyImpactExposureCategory.BUILDINGS, MontyImpactType.DESTROYED, 2)
        self.assertEqual(impact_1.id, impact_2.id)

        impact_1 = build_impact_from_response(
            response_1,
            MontyImpactExposureCategory.BUILDINGS,
            MontyImpactType.DESTROYED,
            1,
            id="impact-EMSR847-aoi01-buildings-destroyed",
        )
        impact_2 = build_impact_from_response(
            response_2,
            MontyImpactExposureCategory.BUILDINGS,
            MontyImpactType.DESTROYED,
            2,
            id="impact-EMSR847-aoi02-buildings-destroyed",
        )
        self.assertNotEqual(impact_1.id, impact_2.id)


class BuildImpactsFromResponseTest(unittest.TestCase):
    """Pattern P4: one Impact item per thematic category from a {thematic: value} mapping."""

    def test_returns_one_impact_item_per_category(self) -> None:
        response_item = make_response_item()
        impact_items = build_impacts_from_response(
            response_item,
            {
                MontyImpactExposureCategory.ALL_PEOPLE: 1200,
                MontyImpactExposureCategory.BUILDINGS: 42,
                MontyImpactExposureCategory.CROPS: 7,
            },
            MontyImpactType.TOTAL_AFFECTED,
            unit="count",
        )

        self.assertEqual(len(impact_items), 3)
        categories = {MontyExtension.ext(item).impact_detail.category for item in impact_items}
        self.assertEqual(categories, {"people", "buildings", "crops"})
        for item in impact_items:
            self.assertEqual(item.properties["monty:corr_id"], response_item.properties["monty:corr_id"])
            derived_links = [link for link in item.links if link.rel == "derived_from"]
            self.assertEqual(len(derived_links), 1)
            self.assertIs(derived_links[0].target, response_item)


class ImpactFromResponseSchemaValidationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.validator = CustomValidator()

    def test_paired_impact_item_validates(self) -> None:
        response_item = make_response_item()
        impact_item = build_impact_from_response(
            response_item,
            MontyImpactExposureCategory.ALL_PEOPLE,
            MontyImpactType.TOTAL_AFFECTED,
            1200,
            unit="people",
        )
        impact_item.set_self_href(f"./{impact_item.id}.json")
        impact_item.validate(validator=self.validator)

    def test_multi_thematic_impact_items_validate(self) -> None:
        response_item = make_response_item()
        impact_items = build_impacts_from_response(
            response_item,
            {MontyImpactExposureCategory.ALL_PEOPLE: 1200, MontyImpactExposureCategory.BUILDINGS: 42},
            MontyImpactType.DAMAGED,
        )
        for impact_item in impact_items:
            impact_item.set_self_href(f"./{impact_item.id}.json")
            impact_item.validate(validator=self.validator)


class PairingFixtureRoundTripTest(unittest.TestCase):
    """Round-trips the synthetic Response+Impact fixture under
    examples/_response-impact-pairing building an Impact item
    from the Response item."""

    def setUp(self) -> None:
        self.validator = CustomValidator()

    def test_builds_matching_impact_fixture(self) -> None:
        with RESPONSE_FIXTURE_PATH.open(encoding="utf-8") as f:
            response_source = json.load(f)
        with IMPACT_FIXTURE_PATH.open(encoding="utf-8") as f:
            impact_source = json.load(f)

        response_item = pystac.Item.from_dict(response_source)
        response_item.set_self_href(str(RESPONSE_FIXTURE_PATH))

        expected_detail = impact_source["properties"]["monty:impact_detail"]
        impact_item = build_impact_from_response(
            response_item,
            MontyImpactExposureCategory(expected_detail["category"]),
            MontyImpactType(expected_detail["type"]),
            expected_detail["value"],
            unit=expected_detail["unit"],
            estimate_type=MontyEstimateType(expected_detail["estimate_type"]),
        )

        self.assertEqual(impact_item.id, impact_source["id"])
        self.assertEqual(impact_item.properties["monty:impact_detail"], expected_detail)
        self.assertEqual(impact_item.properties["monty:corr_id"], impact_source["properties"]["monty:corr_id"])
        self.assertEqual(impact_item.properties["monty:country_codes"], impact_source["properties"]["monty:country_codes"])
        self.assertEqual(impact_item.properties["monty:hazard_codes"], impact_source["properties"]["monty:hazard_codes"])

        impact_item.set_self_href(f"./{impact_item.id}.json")
        impact_item.validate(validator=self.validator)


if __name__ == "__main__":
    pytest.main([__file__])
