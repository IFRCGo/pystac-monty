"""Tests for the Response->Impact pairing"""

import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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

    def test_link_derived_from_response_helper(self) -> None:
        response_item = make_response_item()
        impact_item = build_impact_from_response(response_item, MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.DEATH, 12)
        impact_item.links = [link for link in impact_item.links if link.rel != "derived_from"]

        link_derived_from_response(impact_item, response_item)

        self.assertEqual([link.rel for link in impact_item.links if link.rel != "self"], ["derived_from"])

    def test_id_defaults_from_response_category_and_type(self) -> None:
        response_item = make_response_item(id="charter-response-1000-1144-1")
        impact_item = build_impact_from_response(
            response_item, MontyImpactExposureCategory.BUILDINGS, MontyImpactType.DAMAGED, 42
        )
        self.assertEqual(impact_item.id, "charter-response-1000-1144-1-buildings-damaged")


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


if __name__ == "__main__":
    pytest.main([__file__])
