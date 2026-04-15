import json
import unittest
from os import makedirs
from typing import List

import pytest
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import MockGeocoder
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import DataType, File, PDCDataSourceType
from pystac_monty.sources.pdc import PDCDataSource, PDCTransformer
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator
from tests.utils.test_utils import validate_correlation_id

CURRENT_SCHEMA_URI = "https://ifrcgo.github.io/monty/v0.1.0/schema.json"
# CURRENT_SCHEMA_MAPURL = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/json-schema/schema.json"  # noqa


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
        assert transformer._map_pdc_to_hazard_codes("WILDFIRE") == ["EN0205", "nat-cli-wil-for", "WF"]

    @parameterized.expand(load_scenarios(scenarios))
    def test_pdc_tech_social_hazard_codes_2025(self, transformer: PDCTransformer):
        # Test technological/social hazards
        assert transformer._map_pdc_to_hazard_codes("CYBER") == ["TL0601", "OT"]
        assert transformer._map_pdc_to_hazard_codes("TERRORISM") == ["SO0203", "soc-soc-vio-vio", "OT"]
        assert transformer._map_pdc_to_hazard_codes("CIVILUNREST") == ["SO0202", "soc-soc-vio-vio", "OT"]

    @parameterized.expand(load_scenarios(scenarios))
    def test_all_pdc_hazard_types_mapped(self, transformer: PDCTransformer):
        # All PDC types from documentation
        pdc_types = [
            "AVALANCHE",
            "BIOMEDICAL",
            "DROUGHT",
            "EARTHQUAKE",
            "EXTREMETEMPERATURE",
            "FLOOD",
            "HIGHSURF",
            "LANDSLIDE",
            "MARINE",
            "SEVEREWEATHER",
            "STORM",
            "TORNADO",
            "CYCLONE",
            "TSUNAMI",
            "VOLCANO",
            "WILDFIRE",
            "WINTERSTORM",
            "ACCIDENT",
            "ACTIVESHOOTER",
            "CIVILUNREST",
            "COMBAT",
            "CYBER",
            "MANMADE",
            "OCCURRENCE",
            "POLITICALCONFLICT",
            "TERRORISM",
            "WEAPONS",
        ]

        for hazard_type in pdc_types:
            codes = transformer._map_pdc_to_hazard_codes(hazard_type)
            assert codes is not None, f"No mapping for {hazard_type}"
            assert len(codes) >= 2, f"Insufficient codes for {hazard_type}"
            # First code should be 2025 format
            assert codes[0].startswith(("MH", "GH", "BI", "EN", "TL", "SO", "OT"))

    @parameterized.expand(load_scenarios(scenarios))
    def test_pdc_event_uses_all_codes(self, transformer: PDCTransformer):
        items = transformer.make_items()
        event_item = items[0]
        # Create event item
        monty = MontyExtension.ext(event_item)

        # Should contain all codes
        assert len(monty.hazard_codes) >= 2
        assert monty.hazard_codes[0] in ["GH0300", "GH0101", "Peru"]  # 2025 codes
