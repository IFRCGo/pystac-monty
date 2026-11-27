import json
import unittest
from datetime import datetime, timezone
from os import makedirs
from typing import List
from unittest.mock import patch

import pytest
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import MockGeocoder
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import DataType, File, Memory, PDCDataSourceType
from pystac_monty.sources.pdc import PDC_TYPES_WITHOUT_UNDRR_2025_CODE, PDCDataSource, PDCTransformer
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator
from tests.utils.test_utils import assert_processing_extension_fields, request_for_schema, validate_correlation_id

CURRENT_SCHEMA_URI = "https://ifrcgo.org/monty-stac-extension/v1.3.0/schema.json"
CURRENT_SCHEMA_MAPURL = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/json-schema/schema.json"  # noqa


def load_scenarios(scenarios: List[dict]) -> List[PDCTransformer]:
    """Load Scenarios"""
    transformers = []
    for scenario_item in scenarios:
        geocoder = MockGeocoder()
        pdc_data_source = PDCDataSource(
            data=PDCDataSourceType(
                source_url=scenario_item[0],
                uuid=scenario_item[1]["uuid"],
                hazard_data=File(path=scenario_item[1]["hazards_file_path"], data_type=DataType.FILE),
                exposure_detail_data=File(
                    path=scenario_item[1]["exposure_detail_file_path"],
                    data_type=DataType.FILE,
                ),
                geojson_path=scenario_item[1]["geojson_file_path"],
            )
        )
        transformers.append(PDCTransformer(pdc_data_source, geocoder))
    return transformers


scenario = [
    "https://sentry.pdc.org/hp_srv/services/hazards/t/json/get_active_hazards",
    {
        "hazards_file_path": "./tests/data/pdc/hazard_data.json",
        "exposure_detail_file_path": "./tests/data/pdc/exposure_detail.json",
        "uuid": "f2762e72-5169-487e-a3fa-30e21fc55893",
        "exposure_timestamp": "1738941728977",
        "geojson_file_path": "./tests/data/pdc/hazard_geo.geojson",
    },
]


class PDCTest(unittest.TestCase):
    scenarios = [scenario]

    def setUp(self) -> None:
        super().setUp()
        self.validator = CustomValidator()
        # create temporary folder
        makedirs(get_data_file("temp/pdc"), exist_ok=True)

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_transformer(self, transformer: PDCTransformer) -> None:
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

        items = transformer.make_items()
        self.assertTrue(len(items) > 0)
        source_event_item = None
        source_hazard_item = None
        source_impact_item = None

        for item in items:
            item_path = get_data_file(f"temp/pdc/{item.id}.json")
            with open(item_path, "w", encoding="utf-8") as f:
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
        self.assertIsNotNone(source_impact_item)

        # Verify Correlation ID
        hazard_profiles = MontyHazardProfiles()
        event_item_hazard_code = hazard_profiles.get_canonical_hazard_codes(source_event_item)[0].upper()
        validate_correlation_id(source_event_item.properties.get("monty:corr_id"), event_item_hazard_code)
        hazard_item_hazard_code = hazard_profiles.get_canonical_hazard_codes(source_hazard_item)[0].upper()
        validate_correlation_id(source_hazard_item.properties.get("monty:corr_id"), hazard_item_hazard_code)
        impact_item_hazard_code = hazard_profiles.get_canonical_hazard_codes(source_impact_item)[0].upper()
        validate_correlation_id(source_impact_item.properties.get("monty:corr_id"), impact_item_hazard_code)

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_transformer_item_links(self, transformer: PDCTransformer) -> None:
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

        items = transformer.make_items()
        self.assertTrue(len(items) > 0)
        source_event_item = None
        source_hazard_item = None
        source_impact_item = None

        for item in items:
            item_path = get_data_file(f"temp/pdc/{item.id}.json")
            with open(item_path, "w", encoding="utf-8") as f:
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
        self.assertIsNotNone(source_impact_item)

        # Verify Related links exists
        event_item_related_items = source_event_item.get_links(rel="related")
        hazard_item_related_items = source_hazard_item.get_links(rel="related")
        impact_item_related_items = source_impact_item.get_links(rel="related")
        event_item_self_link = source_event_item.self_href
        hazard_item_self_link = source_hazard_item.self_href
        impact_item_self_link = source_impact_item.self_href

        self.assertTrue(len(event_item_related_items) > 0)
        self.assertTrue(len(hazard_item_related_items) > 0)
        self.assertTrue(len(impact_item_related_items) > 0)
        assert all(link.href is not None and link.href != event_item_self_link for link in event_item_related_items)
        assert all(link.href is not None and link.href != hazard_item_self_link for link in hazard_item_related_items)
        assert all(link.href is not None and link.href != impact_item_self_link for link in impact_item_related_items)

        assert event_item_self_link in [item.href for item in hazard_item_related_items]
        assert hazard_item_self_link in [item.href for item in event_item_related_items]
        assert event_item_self_link in [item.href for item in impact_item_related_items]
        assert impact_item_self_link in [item.href for item in event_item_related_items]

    @parameterized.expand(load_scenarios(scenarios))
    def test_pdc_natural_hazard_codes_2025(self, transformer: PDCTransformer):
        # Test key natural hazards
        assert transformer._map_pdc_to_hazard_codes("FLOOD") == ["MH0600", "nat-hyd-flo-flo", "FL"]
        assert transformer._map_pdc_to_hazard_codes("EARTHQUAKE") == ["GH0101", "nat-geo-ear-gro", "EQ"]
        assert transformer._map_pdc_to_hazard_codes("TSUNAMI") == ["MH0705", "nat-geo-ear-tsu", "TS"]
        assert transformer._map_pdc_to_hazard_codes("WILDFIRE") == ["EN0205", "nat-cli-wil-wil", "WF"]
        assert transformer._map_pdc_to_hazard_codes("HIGHWIND") == ["MH0301", "nat-met-sto-sto", "VW"]

    @parameterized.expand(load_scenarios(scenarios))
    def test_pdc_tech_social_hazard_codes_2025(self, transformer: PDCTransformer):
        # Each triplet is one row of the crosswalk; these rows have no EM-DAT key
        assert transformer._map_pdc_to_hazard_codes("CIVILUNREST") == ["SO0103", "CE"]
        assert transformer._map_pdc_to_hazard_codes("POLITICALCONFLICT") == ["SO0103", "CE"]
        assert transformer._map_pdc_to_hazard_codes("TERRORISM") == ["SO0301", "OT"]
        assert transformer._map_pdc_to_hazard_codes("ACTIVESHOOTER") == ["SO0301", "OT"]

    @parameterized.expand(load_scenarios(scenarios))
    def test_all_pdc_hazard_types_mapped(self, transformer: PDCTransformer):
        valid_2025_codes = set(transformer.hazard_profiles.get_profiles()["undrr_2025_key"])
        pdc_types = [
            "AVALANCHE",
            "BIOMEDICAL",
            "DROUGHT",
            "EARTHQUAKE",
            "EXTREMETEMPERATURE",
            "FLOOD",
            "HIGHWIND",
            "LANDSLIDE",
            "SEVEREWEATHER",
            "STORM",
            "TORNADO",
            "CYCLONE",
            "TSUNAMI",
            "VOLCANO",
            "WILDFIRE",
            "WINTERSTORM",
            "ACTIVESHOOTER",
            "CIVILUNREST",
            "POLITICALCONFLICT",
            "TERRORISM",
        ]

        for hazard_type in pdc_types:
            codes = transformer._map_pdc_to_hazard_codes(hazard_type)
            assert codes is not None, f"No mapping for {hazard_type}"
            assert len(codes) >= 2, f"Insufficient codes for {hazard_type}"
            assert codes[0] in valid_2025_codes, f"{hazard_type}: {codes[0]} is not a UNDRR-ISC 2025 code"

    @parameterized.expand(load_scenarios(scenarios))
    def test_pdc_types_without_2025_code_give_no_codes(self, transformer: PDCTransformer):
        """HIPs 2025 has no code at the granularity of these types (issue #296)."""
        assert set(PDC_TYPES_WITHOUT_UNDRR_2025_CODE) == {
            "ACCIDENT",
            "COMBAT",
            "CYBER",
            "HIGHSURF",
            "MANMADE",
            "MARINE",
            "OCCURRENCE",
            "WEAPONS",
        }
        for hazard_type in PDC_TYPES_WITHOUT_UNDRR_2025_CODE:
            assert transformer._map_pdc_to_hazard_codes(hazard_type) is None

    @parameterized.expand(load_scenarios(scenarios))
    def test_pdc_event_uses_all_codes(self, transformer: PDCTransformer):
        items = transformer.make_items()
        event_item = items[0]
        # Create event item
        monty = MontyExtension.ext(event_item)

        # Should contain all codes
        assert len(monty.hazard_codes) >= 2
        assert monty.hazard_codes[0] in ["GH0300", "GH0101", "Peru"]  # 2025 codes


def test_transformer_without_exposure_detail():
    """Event and hazard items are created when the exposure detail is not available"""
    pdc_data_source = PDCDataSource(
        data=PDCDataSourceType(
            source_url=scenario[0],
            uuid=scenario[1]["uuid"],
            hazard_data=File(path=scenario[1]["hazards_file_path"], data_type=DataType.FILE),
            exposure_detail_data=None,
            geojson_path=scenario[1]["geojson_file_path"],
        )
    )
    geocoder = MockGeocoder()
    transformer = PDCTransformer(pdc_data_source, geocoder)

    with patch.object(geocoder, "get_iso3_from_point", return_value="PER"):
        items = transformer.make_items()

    roles = sorted(item.properties["roles"][1] for item in items)
    assert roles == ["event", "hazard"]
    event_item = next(item for item in items if MontyExtension.ext(item).is_source_event())
    # NOTE: Exposure timestamp is not part of the id without exposure detail
    assert event_item.id == f"pdc-event-{scenario[1]['uuid']}-{transformer.hazard_data['hazard_ID']}"
    assert MontyExtension.ext(event_item).country_codes == ["PER"]


def test_transformer_makes_no_item_for_type_without_2025_code():
    """A PDC type with no UNDRR-ISC 2025 code gives no item and one failed row (issue #296)"""
    pdc_data_source = PDCDataSource(
        data=PDCDataSourceType(
            source_url=scenario[0],
            uuid=scenario[1]["uuid"],
            hazard_data=File(path=scenario[1]["hazards_file_path"], data_type=DataType.FILE),
            exposure_detail_data=None,
            geojson_path=scenario[1]["geojson_file_path"],
        )
    )
    geocoder = MockGeocoder()
    transformer = PDCTransformer(pdc_data_source, geocoder)
    hazard_data = {**transformer._get_hazard_data(), "type_ID": "HIGHSURF"}

    with (
        patch.object(transformer, "_get_hazard_data", return_value=hazard_data),
        patch.object(geocoder, "get_iso3_from_point", return_value="PER"),
    ):
        items = transformer.make_items()

    assert items == []
    assert transformer.transform_summary.total_rows == 1
    assert transformer.transform_summary.failed_rows == 1


def test_transformer_without_exposure_detail_unknown_country():
    """Country falls back to UNK when the geocoder cannot resolve the hazard location"""
    pdc_data_source = PDCDataSource(
        data=PDCDataSourceType(
            source_url=scenario[0],
            uuid=scenario[1]["uuid"],
            hazard_data=File(path=scenario[1]["hazards_file_path"], data_type=DataType.FILE),
            exposure_detail_data=None,
            geojson_path=scenario[1]["geojson_file_path"],
        )
    )
    geocoder = MockGeocoder()
    transformer = PDCTransformer(pdc_data_source, geocoder)

    with patch.object(geocoder, "get_iso3_from_point", return_value=None):
        items = transformer.make_items()

    event_item = next(item for item in items if MontyExtension.ext(item).is_source_event())
    assert MontyExtension.ext(event_item).country_codes == ["UNK"]


def _transformer_with_exposure(tmp_path, exposure: dict) -> PDCTransformer:
    exposure_path = tmp_path / "exposure_detail.json"
    exposure_path.write_text(json.dumps(exposure))
    pdc_data_source = PDCDataSource(
        data=PDCDataSourceType(
            source_url=scenario[0],
            uuid=scenario[1]["uuid"],
            hazard_data=File(path=scenario[1]["hazards_file_path"], data_type=DataType.FILE),
            exposure_detail_data=File(path=str(exposure_path), data_type=DataType.FILE),
            geojson_path=scenario[1]["geojson_file_path"],
        )
    )
    return PDCTransformer(pdc_data_source, MockGeocoder())


def _load_exposure() -> dict:
    with open(scenario[1]["exposure_detail_file_path"]) as f:
        return json.load(f)


def test_transformer_skips_only_impacts_with_null_value(tmp_path):
    """Null values in the exposure detail skip only the related impact items"""
    exposure = _load_exposure()
    null_value = {"valueFormatted": None, "valueFormattedNoTrunc": None, "valueRounded": None, "value": None}
    exposure["totalByAdmin"][0]["population"]["households"] = null_value
    exposure["totalByCountry"][0]["population"]["households"] = null_value

    items = _transformer_with_exposure(tmp_path, exposure).make_items()

    roles = [item.properties["roles"][1] for item in items]
    assert roles.count("event") == 1
    assert roles.count("hazard") == 1
    impact_ids = [item.id for item in items if item.properties["roles"][1] == "impact"]
    assert impact_ids
    assert not [i for i in impact_ids if "-population-households-" in i]
    assert [i for i in impact_ids if "-population-total-" in i]


def test_transformer_invalid_exposure_still_creates_event_and_hazard(tmp_path):
    """Event and hazard items are created even if the exposure detail is invalid"""
    exposure = _load_exposure()
    exposure["totalByAdmin"] = "invalid"

    items = _transformer_with_exposure(tmp_path, exposure).make_items()

    assert sorted(item.properties["roles"][1] for item in items) == ["event", "hazard"]


def test_transformer_exposure_without_countries_uses_geocoder(tmp_path):
    """Country falls back to the hazard location when the exposure detail has no countries"""
    exposure = _load_exposure()
    exposure["totalByCountry"] = []
    exposure["totalByAdmin"] = []

    transformer = _transformer_with_exposure(tmp_path, exposure)
    with patch.object(transformer.geocoder, "get_iso3_from_point", return_value=None):
        items = transformer.make_items()

    assert sorted(item.properties["roles"][1] for item in items) == ["event", "hazard"]
    event_item = next(item for item in items if MontyExtension.ext(item).is_source_event())
    assert MontyExtension.ext(event_item).country_codes == ["UNK"]


def test_transformer_ids_with_exposure_detail(tmp_path):
    """Only impact ids embed the exposure timestamp; event and hazard ids are stable"""
    exposure = _load_exposure()
    transformer = _transformer_with_exposure(tmp_path, exposure)
    items = transformer.make_items()

    base = f"{scenario[1]['uuid']}-{transformer.hazard_data['hazard_ID']}"
    ts = PDCTransformer.get_exposure_timestamp_ms(exposure["timestamp"])
    by_role = {}
    for item in items:
        by_role.setdefault(item.properties["roles"][1], []).append(item.id)

    assert by_role["event"] == [f"pdc-event-{base}"]
    assert by_role["hazard"] == [f"pdc-hazard-{base}"]
    assert by_role["impact"]
    country = exposure["totalByAdmin"][0]["country"]
    assert f"pdc-impact-{base}-{ts}-1-population-total-{country}-0" in by_role["impact"]
    assert all(i.startswith(f"pdc-impact-{base}-{ts}-1-") for i in by_role["impact"])


def _transformer_with_exposures(tmp_path, exposures: List[dict]) -> PDCTransformer:
    exposure_files = []
    for index, exposure in enumerate(exposures):
        exposure_path = tmp_path / f"exposure_detail_{index}.json"
        exposure_path.write_text(json.dumps(exposure))
        exposure_files.append(File(path=str(exposure_path), data_type=DataType.FILE))
    pdc_data_source = PDCDataSource(
        data=PDCDataSourceType(
            source_url=scenario[0],
            uuid=scenario[1]["uuid"],
            hazard_data=File(path=scenario[1]["hazards_file_path"], data_type=DataType.FILE),
            exposure_detail_data=exposure_files,
            geojson_path=scenario[1]["geojson_file_path"],
        )
    )
    return PDCTransformer(pdc_data_source, MockGeocoder())


def _exposure_snapshot(timestamp: float, country: str | None = None, admin0: str | None = None) -> dict:
    exposure = _load_exposure()
    exposure["timestamp"] = timestamp
    if country:
        for admin in exposure["totalByAdmin"] + exposure["totalByCountry"]:
            admin["country"] = country
            admin["admin0"] = admin0 or country
    return exposure


def test_transformer_multiple_exposure_details_create_event_and_hazard_once(tmp_path):
    """Event and hazard items are created once; impact items are created for every snapshot"""
    timestamps = [1738800000.0, 1738900000.0, 1739000000.0]
    transformer = _transformer_with_exposures(tmp_path, [_exposure_snapshot(ts) for ts in timestamps])
    items = transformer.make_items()

    by_role = {}
    for item in items:
        by_role.setdefault(item.properties["roles"][1], []).append(item)

    assert len(by_role["event"]) == 1
    assert len(by_role["hazard"]) == 1

    base = f"{scenario[1]['uuid']}-{transformer.hazard_data['hazard_ID']}"
    impact_ids = [item.id for item in by_role["impact"]]
    assert len(impact_ids) == len(set(impact_ids))
    single_dir = tmp_path / "single"
    single_dir.mkdir()
    single_snapshot_count = len(_transformer_with_exposures(single_dir, [_exposure_snapshot(timestamps[0])]).make_items()) - 2
    assert len(impact_ids) == single_snapshot_count * len(timestamps)
    for ts in timestamps:
        assert [i for i in impact_ids if i.startswith(f"pdc-impact-{base}-{int(ts * 1000)}-1-")]

    # NOTE: Event and hazard link to the impact items of all the snapshots
    event_item, hazard_item = by_role["event"][0], by_role["hazard"][0]
    for item in (event_item, hazard_item):
        linked_impacts = {
            link.href.rsplit("/", 1)[-1]
            for link in item.links
            if link.rel == "related" and link.extra_fields["roles"] == ["impact"]
        }
        assert linked_impacts == set(impact_ids)


def test_transformer_multiple_exposure_details_country_union_earliest_first(tmp_path):
    """Event countries are collected from all the snapshots, earliest snapshot first"""
    exposures = [
        _exposure_snapshot(1738800000.0, "PER", "Peru"),
        _exposure_snapshot(1739000000.0, "ECU", "Ecuador"),
        _exposure_snapshot(1738900000.0, "PER", "Peru"),
    ]
    items = _transformer_with_exposures(tmp_path, exposures).make_items()

    event_item = next(item for item in items if MontyExtension.ext(item).is_source_event())
    assert MontyExtension.ext(event_item).country_codes == ["PER", "ECU"]
    assert {"Ecuador", "Peru"} <= set(event_item.properties["keywords"])


def test_transformer_multiple_exposure_details_skips_invalid_snapshot(tmp_path):
    """An invalid snapshot skips only its impact items"""
    invalid = _exposure_snapshot(1739000000.0)
    invalid["totalByAdmin"] = "invalid"
    valid = _exposure_snapshot(1738900000.0)
    items = _transformer_with_exposures(tmp_path, [invalid, valid]).make_items()

    roles = [item.properties["roles"][1] for item in items]
    assert roles.count("event") == 1
    assert roles.count("hazard") == 1
    impact_ids = [item.id for item in items if item.properties["roles"][1] == "impact"]
    assert impact_ids
    assert all("-1738900000000-1-" in i for i in impact_ids)


def test_transformer_impact_datetime_is_exposure_snapshot_timestamp(tmp_path):
    """Impact datetime is the exposure snapshot timestamp (ms or s); start/end stay as the event's"""
    snapshots = {
        1777638048660.0: datetime(2026, 5, 1, 12, 20, 48, 660000, tzinfo=timezone.utc),  # milliseconds
        1777663831.0: datetime(2026, 5, 1, 19, 30, 31, tzinfo=timezone.utc),  # seconds
    }
    transformer = _transformer_with_exposures(tmp_path, [_exposure_snapshot(ts) for ts in snapshots])
    items = transformer.make_items()
    event_item = next(item for item in items if item.properties["roles"][1] == "event")
    impact_items = [item for item in items if item.properties["roles"][1] == "impact"]
    assert impact_items

    for impact_item in impact_items:
        ts = next(ts for ts in snapshots if f"-{PDCTransformer.get_exposure_timestamp_ms(ts)}-1-" in impact_item.id)
        assert impact_item.datetime == snapshots[ts]
        assert impact_item.properties["start_datetime"] == event_item.properties["start_datetime"]
        assert impact_item.properties["end_datetime"] == event_item.properties["end_datetime"]
    assert event_item.datetime != snapshots[1777638048660.0]


def test_transformer_new_snapshot_keeps_primary_country_and_corr_id(tmp_path):
    """A new snapshot only appends countries, so country_codes[0] and corr_id stay the same"""
    t1 = _exposure_snapshot(1738897884.0, "PER", "Peru")
    t2 = _exposure_snapshot(1738997884.0, "ECU", "Ecuador")

    def event_of(path, exposures):
        path.mkdir()
        items = _transformer_with_exposures(path, exposures).make_items()
        return MontyExtension.ext(next(item for item in items if MontyExtension.ext(item).is_source_event()))

    before = event_of(tmp_path / "t1", [t1])
    # NOTE: Input order must not matter
    after = event_of(tmp_path / "t2", [t2, t1])

    assert before.country_codes == ["PER"]
    assert after.country_codes == ["PER", "ECU"]
    assert after.correlation_id == before.correlation_id


def test_transformer_sorts_mixed_timestamp_units_by_datetime(tmp_path):
    """Snapshots are sorted by datetime: a milliseconds timestamp is not always later than a seconds one"""
    earlier_ms = _exposure_snapshot(1738800000000.0, "PER", "Peru")
    later_s = _exposure_snapshot(1738900000.0, "ECU", "Ecuador")
    items = _transformer_with_exposures(tmp_path, [later_s, earlier_ms]).make_items()

    event_item = next(item for item in items if MontyExtension.ext(item).is_source_event())
    assert MontyExtension.ext(event_item).country_codes == ["PER", "ECU"]


def test_transformer_snapshots_in_same_second_have_unique_impact_ids(tmp_path):
    """Impact ids use the timestamp in milliseconds, so snapshots in the same second do not collide"""
    exposures = [_exposure_snapshot(1738897884.1), _exposure_snapshot(1738897884.7774327)]
    items = _transformer_with_exposures(tmp_path, exposures).make_items()

    impact_ids = [item.id for item in items if item.properties["roles"][1] == "impact"]
    assert impact_ids
    assert len(impact_ids) == len(set(impact_ids))
    assert [i for i in impact_ids if "-1738897884100-1-" in i]
    assert [i for i in impact_ids if "-1738897884777-1-" in i]


def test_transformer_duplicate_snapshot_timestamp_keeps_one(tmp_path):
    """Snapshots with the same timestamp are processed once"""
    exposures = [_exposure_snapshot(1738897884.0), _exposure_snapshot(1738897884.0)]
    items = _transformer_with_exposures(tmp_path, exposures).make_items()

    impact_ids = [item.id for item in items if item.properties["roles"][1] == "impact"]
    assert impact_ids
    assert len(impact_ids) == len(set(impact_ids))


def test_transformer_admin_rows_of_same_country_have_unique_impact_ids(tmp_path):
    """Impact ids include admin1/admin2, so totalByAdmin rows of the same country do not collide"""
    exposure = _load_exposure()
    row = exposure["totalByAdmin"][0]
    exposure["totalByAdmin"] = [
        {**row, "admin1": "Lima", "admin2": "Lima"},
        {**row, "admin1": "Lima", "admin2": "Huaral"},
        {**row, "admin1": "Ica", "admin2": None},
    ]
    items = _transformer_with_exposure(tmp_path, exposure).make_items()

    impact_ids = [item.id for item in items if item.properties["roles"][1] == "impact"]
    assert len(impact_ids) == len(set(impact_ids))
    assert [i for i in impact_ids if i.endswith("-population-total-PER-lima-lima-0")]
    assert [i for i in impact_ids if i.endswith("-population-total-PER-lima-huaral-0")]
    assert [i for i in impact_ids if i.endswith("-population-total-PER-ica-0")]


def test_transformer_admin_rows_without_admin1_admin2_have_unique_impact_ids(tmp_path, caplog):
    """totalByAdmin rows of the same country without admin1/admin2 get the row index in the impact id"""
    exposure = _load_exposure()
    row = exposure["totalByAdmin"][0]
    exposure["totalByAdmin"] = [
        {**row, "admin1": None, "admin2": None},
        {**row, "admin1": None, "admin2": None},
        {**row, "admin1": "Ica", "admin2": None},
        {**row, "admin1": None, "admin2": None},
    ]
    with caplog.at_level("WARNING", logger="pystac_monty.sources.pdc"):
        items = _transformer_with_exposure(tmp_path, exposure).make_items()

    impact_ids = [item.id for item in items if item.properties["roles"][1] == "impact"]
    assert len(impact_ids) == len(set(impact_ids))
    assert [i for i in impact_ids if i.endswith("-population-total-PER-0")]
    assert [i for i in impact_ids if i.endswith("-population-total-PER-1")]
    assert [i for i in impact_ids if i.endswith("-population-total-PER-ica-0")]
    assert [i for i in impact_ids if i.endswith("-population-total-PER-3")]
    assert "using row index" in caplog.text


def test_transformer_admin_id_part_is_ascii(tmp_path):
    """admin1/admin2 in the impact id are lowercase ASCII; names with no ASCII form are left out"""
    exposure = _load_exposure()
    row = exposure["totalByAdmin"][0]
    exposure["totalByAdmin"] = [
        {**row, "admin1": "San Martín", "admin2": "Región Ñuñoa/Sur"},
        {**row, "admin1": "東京都", "admin2": None},
    ]
    items = _transformer_with_exposure(tmp_path, exposure).make_items()

    impact_ids = [item.id for item in items if item.properties["roles"][1] == "impact"]
    assert all(i.isascii() for i in impact_ids)
    assert [i for i in impact_ids if i.endswith("-population-total-PER-san-martin-region-nunoa-sur-0")]
    assert [i for i in impact_ids if i.endswith("-population-total-PER-0")]


def test_data_source_reads_exposure_by_its_own_data_type():
    """Exposure details are read by their own data type, not by the hazard data type"""
    exposure = _load_exposure()
    pdc_data_source = PDCDataSource(
        data=PDCDataSourceType(
            source_url=scenario[0],
            uuid=scenario[1]["uuid"],
            hazard_data=File(path=scenario[1]["hazards_file_path"], data_type=DataType.FILE),
            exposure_detail_data=[
                Memory(content=exposure, data_type=DataType.MEMORY),
                Memory(content=json.dumps(exposure), data_type=DataType.MEMORY),
            ],
            geojson_path=scenario[1]["geojson_file_path"],
        )
    )
    assert pdc_data_source.get_exposure_detail_data() == [exposure, exposure]


def test_pdc_data_source_type_accepts_single_exposure_detail():
    """A single exposure detail is normalized to a list"""
    data = PDCDataSourceType(
        source_url=scenario[0],
        uuid=scenario[1]["uuid"],
        hazard_data=File(path=scenario[1]["hazards_file_path"], data_type=DataType.FILE),
        exposure_detail_data=File(path=scenario[1]["exposure_detail_file_path"], data_type=DataType.FILE),
        geojson_path=scenario[1]["geojson_file_path"],
    )
    assert len(data.exposure_detail_data) == 1
    assert PDCDataSourceType(**{**data.model_dump(), "exposure_detail_data": None}).exposure_detail_data == []


def test_impact_items_have_a_readable_title_and_the_impact_keywords():
    """The title names the impact field and the place; the keywords name the impact type and category"""
    pdc_data_source = PDCDataSource(
        data=PDCDataSourceType(
            source_url=scenario[0],
            uuid=scenario[1]["uuid"],
            hazard_data=File(path=scenario[1]["hazards_file_path"], data_type=DataType.FILE),
            exposure_detail_data=File(path=scenario[1]["exposure_detail_file_path"], data_type=DataType.FILE),
            geojson_path=scenario[1]["geojson_file_path"],
        )
    )
    items = PDCTransformer(pdc_data_source, MockGeocoder()).make_items()
    event_item = next(item for item in items if MontyExtension.ext(item).is_source_event())
    impacts = {item.id.split("-1-", 1)[-1]: item for item in items if MontyExtension.ext(item).is_source_impact()}
    event_title = event_item.properties["title"]
    event_keywords = event_item.properties["keywords"]

    capital = impacts["capital-total-PER-0"]
    assert capital.properties["title"] == f"{event_title} - Total capital (PER)"
    assert capital.properties["keywords"] == event_keywords + ["Loss (Cost)", "Global/Regional Currency (e.g. USD)"]

    population = impacts["population-total-PER-0"]
    assert population.properties["title"] == f"{event_title} - Total population (PER)"
    assert population.properties["keywords"] == event_keywords + ["Potentially Affected", "People (All Demographics)"]

    children = impacts["population-total0_4-PER-0"]
    assert children.properties["title"] == f"{event_title} - Population aged 0 to 4 (PER)"

    # Each impact item of the snapshot has its own title
    titles = [item.properties["title"] for item in impacts.values()]
    assert len(titles) == len(set(titles)) == 19
    # The event and hazard items do not change
    hazard_item = next(item for item in items if MontyExtension.ext(item).is_source_hazard())
    assert hazard_item.properties["title"] == event_title
    assert hazard_item.properties["keywords"] == event_keywords


def test_impact_items_are_modelled_exposure_not_observed_impact():
    """PDC gives the exposure of a hazard area from a model, not an observed impact"""
    pdc_data_source = PDCDataSource(
        data=PDCDataSourceType(
            source_url=scenario[0],
            uuid=scenario[1]["uuid"],
            hazard_data=File(path=scenario[1]["hazards_file_path"], data_type=DataType.FILE),
            exposure_detail_data=File(path=scenario[1]["exposure_detail_file_path"], data_type=DataType.FILE),
            geojson_path=scenario[1]["geojson_file_path"],
        )
    )
    items = PDCTransformer(pdc_data_source, MockGeocoder()).make_items()
    details = {
        item.id.split("-1-", 1)[-1]: item.properties["monty:impact_detail"]
        for item in items
        if MontyExtension.ext(item).is_source_impact()
    }

    assert len(details) == 19
    assert all(detail["estimate_type"] == "modelled" for detail in details.values())
    assert details["population-total-PER-0"]["type"] == "potentially_affected"
    assert details["capital-school-PER-0"]["type"] == "potentially_affected"
    # The capital in the area stays a cost, with the modelled estimate type
    assert details["capital-total-PER-0"]["type"] == "cost"
    # The 18 people and asset figures are counts; the payload gives no currency for the capital
    assert [key for key, detail in details.items() if detail.get("unit") != "count"] == ["capital-total-PER-0"]
    assert details["capital-total-PER-0"].get("unit") is None


def test_items_do_not_copy_the_pdc_category_id():
    """`category_id` was an undeclared copy of the PDC field `category_ID` in all the items"""
    pdc_data_source = PDCDataSource(
        data=PDCDataSourceType(
            source_url=scenario[0],
            uuid=scenario[1]["uuid"],
            hazard_data=File(path=scenario[1]["hazards_file_path"], data_type=DataType.FILE),
            exposure_detail_data=File(path=scenario[1]["exposure_detail_file_path"], data_type=DataType.FILE),
            geojson_path=scenario[1]["geojson_file_path"],
        )
    )
    items = PDCTransformer(pdc_data_source, MockGeocoder()).make_items()
    assert len(items) == 21
    assert all("category_id" not in item.properties for item in items)
