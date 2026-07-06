"""Tests for Response item support: ResponseDetail, the pydantic validator, and the builder."""

import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pystac
import pytest
from pydantic import ValidationError
from pystac.validation import JsonSchemaSTACValidator

from pystac_monty.extension import (
    SCHEMA_URI,
    MontyExtension,
    MontyMethodology,
    MontyResponseStatus,
    MontyResponseType,
    ResponseDetail,
)
from pystac_monty.response import (
    build_response_item,
    filter_response_items,
    link_monitoring_update,
    link_related_response,
)
from pystac_monty.validators.response import ResponseDetailValidator
from tests.utils.test_cases import ARBITRARY_BBOX, ARBITRARY_GEOM

CURRENT_SCHEMA_URI = SCHEMA_URI
SUBMODULE_ROOT = Path(__file__).resolve().parents[2] / "monty-stac-extension"
SUBMODULE_SCHEMA_PATH = SUBMODULE_ROOT / "json-schema" / "schema.json"
# A real v1.3.0 Response item shipped in the vendored submodule. (#157 references
# `examples/unosat-responses/…`, which was never published upstream; the Charter
# response examples are the actual on-disk v1.3.0 Response fixtures.)
RESPONSE_FIXTURE_PATH = SUBMODULE_ROOT / "examples" / "charter-response" / "charter-response-1000-1144-1.json"


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
        status=MontyResponseStatus.PUBLISHED,
        producer="JRC",
        methodology=MontyMethodology.HUMAN_INTERPRETED,
        sendai_targets=["D", "G"],
    )
    kwargs.update(overrides)
    item = build_response_item(**kwargs)
    # Links resolve "href" from the target's self href; assign one so link-bearing
    # items (rel: prev / rel: related) serialize to schema-valid string hrefs.
    item.set_self_href(f"./{item.id}.json")
    return item


class ResponseDetailTest(unittest.TestCase):
    def test_round_trip(self) -> None:
        detail = ResponseDetail(
            type="eo-del",
            source_id="EMSR744",
            status=MontyResponseStatus.PUBLISHED,
            monitoring_number=2,
            producer="JRC",
            methodology=MontyMethodology.SEMI_AUTOMATED,
            sendai_targets=["B", "D"],
            sectors=["shelter"],
        )
        d = detail.to_dict()
        self.assertEqual(
            d,
            {
                "type": "eo-del",
                "source_id": "EMSR744",
                "status": "published",
                "monitoring_number": 2,
                "producer": "JRC",
                "methodology": "semi_automated",
                "sendai_targets": ["B", "D"],
                "sectors": ["shelter"],
            },
        )
        rebuilt = ResponseDetail.from_dict(d)
        self.assertEqual(rebuilt.to_dict(), d)

    def test_is_monitoring_update(self) -> None:
        self.assertFalse(ResponseDetail(type="eo-ref").is_monitoring_update())
        self.assertTrue(ResponseDetail(type="eo-mon", monitoring_number=3).is_monitoring_update())

    def test_sendai_targets_set(self) -> None:
        self.assertEqual(ResponseDetail(type="eo-ref").sendai_targets_set(), set())
        self.assertEqual(ResponseDetail(type="eo-del", sendai_targets=["D", "G"]).sendai_targets_set(), {"D", "G"})

    def test_extension_accessor_round_trip(self) -> None:
        item = make_response_item()
        monty = MontyExtension.ext(item)
        detail = monty.response_detail
        assert detail is not None
        self.assertEqual(detail.type, "eo-del")
        self.assertEqual(detail.status, "published")
        self.assertEqual(detail.sendai_targets_set(), {"D", "G"})

        monty.response_detail = None
        self.assertIsNone(monty.response_detail)


class ResponseDetailValidatorTest(unittest.TestCase):
    def test_valid_payload_passes(self) -> None:
        validated = ResponseDetailValidator(type="hum-shelter", sectors=["shelter"])
        self.assertEqual(validated.type, "hum-shelter")

    def test_rejects_bad_type_pattern(self) -> None:
        with self.assertRaises(ValidationError):
            ResponseDetailValidator(type="cems-del")

    def test_rejects_unknown_status(self) -> None:
        with self.assertRaises(ValidationError):
            ResponseDetailValidator(type="eo-del", status="active")

    def test_rejects_unknown_methodology(self) -> None:
        with self.assertRaises(ValidationError):
            ResponseDetailValidator(type="eo-del", methodology="guessed")

    def test_rejects_sendai_target_outside_a_to_g(self) -> None:
        with self.assertRaises(ValidationError):
            ResponseDetailValidator(type="eo-del", sendai_targets=["A", "Z"])

    def test_rejects_duplicate_sendai_targets(self) -> None:
        with self.assertRaises(ValidationError):
            ResponseDetailValidator(type="eo-del", sendai_targets=["A", "A"])

    def test_rejects_monitoring_number_below_one(self) -> None:
        with self.assertRaises(ValidationError):
            ResponseDetailValidator(type="eo-mon", monitoring_number=0)

    def test_rejects_legacy_response_type_field(self) -> None:
        with self.assertRaises(ValidationError):
            ResponseDetailValidator(type="eo-del", response_type="eo-del")

    def test_rejects_legacy_status_code_field(self) -> None:
        with self.assertRaises(ValidationError):
            ResponseDetailValidator(type="eo-del", status_code="F")

    def test_rejects_legacy_boolean_monitoring_field(self) -> None:
        with self.assertRaises(ValidationError):
            ResponseDetailValidator(type="eo-mon", monitoring=True)

    def test_rejects_unknown_top_level_key(self) -> None:
        with self.assertRaises(ValidationError):
            ResponseDetailValidator(type="eo-del", charter_activation_id=849)


class BuildResponseItemTest(unittest.TestCase):
    def test_sets_core_properties(self) -> None:
        item = make_response_item()
        self.assertEqual(item.properties["roles"], ["response"])
        self.assertEqual(item.properties["monty:corr_id"], "20260615T000000Z-ESP-FL-001-GCDB")
        self.assertEqual(item.properties["monty:country_codes"], ["ESP"])
        self.assertEqual(item.properties["monty:hazard_codes"], ["FL"])
        self.assertEqual(
            item.properties["monty:response_detail"],
            {
                "type": "eo-del",
                "source_id": "EMSR744",
                "status": "published",
                "producer": "JRC",
                "methodology": "human_interpreted",
                "sendai_targets": ["D", "G"],
            },
        )

    def test_invalid_response_detail_raises(self) -> None:
        with self.assertRaises(ValidationError):
            make_response_item(type="not-a-valid-type")

    def test_monitoring_update_adds_prev_link(self) -> None:
        prev_item = make_response_item(id="response-1")
        mon_item = make_response_item(id="response-2", monitoring_number=2, prev_response_item=prev_item)

        prev_links = [link for link in mon_item.links if link.rel == "prev"]
        self.assertEqual(len(prev_links), 1)
        self.assertIs(prev_links[0].target, prev_item)
        self.assertTrue(MontyExtension.ext(mon_item).response_detail.is_monitoring_update())

    def test_related_response_items_adds_bidirectional_related_link(self) -> None:
        cems_item = make_response_item(id="cems-response")
        charter_item = make_response_item(id="charter-response", type="eo-vap", producer="Airbus")
        link_related_response(cems_item, charter_item)

        cems_related = [link for link in cems_item.links if link.rel == "related"]
        charter_related = [link for link in charter_item.links if link.rel == "related"]
        self.assertEqual(len(cems_related), 1)
        self.assertEqual(len(charter_related), 1)
        self.assertEqual(cems_related[0].extra_fields["roles"], ["response"])
        self.assertEqual(charter_related[0].extra_fields["roles"], ["response"])
        self.assertIs(cems_related[0].target, charter_item)
        self.assertIs(charter_related[0].target, cems_item)

    def test_builder_wires_related_response_items_argument(self) -> None:
        charter_item = make_response_item(id="charter-response", type="eo-vap", producer="Airbus")
        cems_item = make_response_item(id="cems-response", related_response_items=[charter_item])

        self.assertEqual([link.rel for link in cems_item.links if link.rel != "self"], ["related"])
        self.assertEqual([link.rel for link in charter_item.links if link.rel != "self"], ["related"])

    def test_link_monitoring_update_helper(self) -> None:
        prev_item = make_response_item(id="response-1")
        mon_item = make_response_item(id="response-2", monitoring_number=2)
        link_monitoring_update(mon_item, prev_item)

        self.assertEqual([link.rel for link in mon_item.links if link.rel != "self"], ["prev"])


class FilterResponseItemsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.eo_item = make_response_item(id="eo-item", type="eo-del", producer="JRC")
        self.hum_item = make_response_item(
            id="hum-item",
            type="hum-shelter",
            producer="IFRC",
            methodology=MontyMethodology.AUTOMATED,
            status=MontyResponseStatus.FINISHED,
            sectors=["shelter"],
            sendai_targets=None,
        )
        self.no_detail_item = make_response_item(id="no-detail-item")
        MontyExtension.ext(self.no_detail_item).response_detail = None

    def test_filters_by_type(self) -> None:
        result = filter_response_items([self.eo_item, self.hum_item], type="hum-shelter")
        self.assertEqual([item.id for item in result], ["hum-item"])

    def test_filters_by_producer(self) -> None:
        result = filter_response_items([self.eo_item, self.hum_item], producer="JRC")
        self.assertEqual([item.id for item in result], ["eo-item"])

    def test_filters_by_methodology_and_status(self) -> None:
        result = filter_response_items(
            [self.eo_item, self.hum_item], methodology=MontyMethodology.AUTOMATED, status=MontyResponseStatus.FINISHED
        )
        self.assertEqual([item.id for item in result], ["hum-item"])

    def test_items_without_response_detail_are_excluded(self) -> None:
        result = filter_response_items([self.eo_item, self.no_detail_item])
        self.assertEqual([item.id for item in result], ["eo-item"])

    def test_no_filters_returns_all_items_with_detail(self) -> None:
        result = filter_response_items([self.eo_item, self.hum_item, self.no_detail_item])
        self.assertEqual({item.id for item in result}, {"eo-item", "hum-item"})


class ResponseSchemaValidationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.validator = CustomValidator()

    def test_bare_response_item_validates(self) -> None:
        item = make_response_item()
        item.validate(validator=self.validator)

    def test_monitoring_update_with_prev_link_validates(self) -> None:
        prev_item = make_response_item(id="response-1")
        mon_item = make_response_item(id="response-2", monitoring_number=2, prev_response_item=prev_item)
        mon_item.validate(validator=self.validator)

    def test_charter_co_activation_related_link_validates(self) -> None:
        charter_item = make_response_item(id="charter-response", type="eo-vap", producer="Airbus")
        cems_item = make_response_item(id="cems-response", related_response_items=[charter_item])
        cems_item.validate(validator=self.validator)

    def test_layered_disaster_extension_does_not_duplicate_status(self) -> None:
        """Charter VAP items layer `disaster:` instead of setting response_detail.status."""
        item = make_response_item(
            id="charter-response",
            type="eo-vap",
            status=None,
            producer="Airbus",
        )
        item.stac_extensions.append("https://terradue.github.io/stac-extensions-disaster/v1.1.0/schema.json")
        item.properties["disaster:class"] = "vap"
        item.properties["disaster:activation_id"] = 849
        item.properties["disaster:activation_status"] = "open"

        self.assertNotIn("status", item.properties["monty:response_detail"])
        # Only the schema's own extensions are enforced here; `disaster:` fields are
        # additionalProperties on the item and are not validated by this schema.
        item.validate(validator=self.validator)


class EoDatResponseTypeTest(unittest.TestCase):
    """`eo-dat` (Charter calibrated acquisition delivered as the response) is a
    first-class EO response code in response-taxonomy §2.1 and is required by the
    Charter transformer."""

    def test_eo_dat_in_enum(self) -> None:
        self.assertEqual(MontyResponseType.EO_DATA.value, "eo-dat")
        self.assertIn("eo-dat", {e.value for e in MontyResponseType})

    def test_eo_dat_passes_validator(self) -> None:
        validated = ResponseDetailValidator(type="eo-dat", producer="Airbus", sendai_targets=["D", "G"])
        self.assertEqual(validated.type, "eo-dat")

    def test_eo_dat_builds_and_validates(self) -> None:
        item = make_response_item(id="charter-response-1166-phr1a", type="eo-dat", methodology=None, sendai_targets=["D", "G"])
        self.assertEqual(item.properties["monty:response_detail"]["type"], "eo-dat")
        item.validate(validator=CustomValidator())


class ResponseFixtureRoundTripTest(unittest.TestCase):
    """Round-trip a real on-disk v1.3.0 Response STAC item through the accessor and
    re-validate it against the schema (the round-trip AC in #157)."""

    def setUp(self) -> None:
        self.validator = CustomValidator()

    def test_roundtrip_real_response_item(self) -> None:
        with RESPONSE_FIXTURE_PATH.open(encoding="utf-8") as f:
            source = json.load(f)

        item = pystac.Item.from_dict(source)
        detail = MontyExtension.ext(item).response_detail
        assert detail is not None
        self.assertEqual(detail.type, "eo-gra")
        self.assertEqual(detail.source_id, "1144-1")
        self.assertEqual(detail.producer, "Airbus")
        self.assertEqual(detail.methodology, MontyMethodology.HUMAN_INTERPRETED)
        self.assertEqual(detail.sendai_targets_set(), {"C", "D"})

        # response_detail survives a serialise round-trip unchanged, and the item stays schema-valid.
        self.assertEqual(item.to_dict()["properties"]["monty:response_detail"], source["properties"]["monty:response_detail"])
        item.validate(validator=self.validator)


if __name__ == "__main__":
    pytest.main([__file__])
