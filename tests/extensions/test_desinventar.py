import json
import tempfile
from os import makedirs
from typing import List, Tuple, TypedDict
from unittest import TestCase

import pytest
import requests
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import MockGeocoder
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import DataType, DesinventarDataSourceType, File
from pystac_monty.sources.desinventar import (
    DesinventarDataSource,
    DesinventarTransformer,
)
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator
from tests.utils.test_utils import request_for_schema, validate_correlation_id

CURRENT_SCHEMA_URI = "https://ifrcgo.org/monty-stac-extension/v1.3.0/schema.json"
CURRENT_SCHEMA_MAPURL = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/json-schema/schema.json"

# UNDRR-ISC 2025 codes for which HazardProfiles.csv defines no EM-DAT cluster
# code, so pystac_monty.sources.desinventar.hazard_mapping only supplies
# [UNDRR code, GLIDE code] for them -- get_canonical_hazard_codes() therefore
# emits 2 codes, not 3, for events of these hazard types. Kept as an explicit,
# hand-maintained list (rather than derived from hazard_mapping) so that a
# future 2-element entry not accounted for here fails this test instead of
# silently passing.
UNDRR_2025_CODES_WITHOUT_EMDAT_CODE = {
    "BI0027",  # EPIZOOTIC
    "BI0204",  # Cholera
    "BI0219",  # MALARIA
    "BI0221",  # MEASLE
    "BI0222",  # MENINGITIS
    "BI0224",  # Mpox
    "BI0228",  # PLAGUE
    "BI0241",  # YELLOW FEVER
    "BI0301",  # ANIMAL DISEASE
    "BI0604",  # ANIMAL ATTACK
    "BI0605",  # SNAKE BITE
    "CH0201",  # AFLATOXIN
    "CH0400",  # ASPHYXIA
    "CH0601",  # INTOXICACION
    "CH0903",  # CHEMICAL SUBSTANCE
    "EN0102",  # Air pollution
    "EN0103",  # CONTAMINATION
    "EN0105",  # Acid rain
    "EN0201",  # Deforestación
    "EN0301",  # LAND DEGRADATION
    "EN0304",  # WETLAND LOSS/DEGRADATION
    "EN0402",  # SEA LEVEL RISE
    "GH0301",  # ROCK FALL
    "GH0309",  # SUBSIDENCE
    "GH0404",  # RIVERBANK EROSION
    "MH0303",  # GALE
    "MH0402",  # RAIN, HEAVY RAINS
    "MH0405",  # Snowfall
    "MH0406",  # SNOW STORM
    "MH0606",  # URBAN FLOOD
    "SO0103",  # CONFLICT
    "SO0301",  # GUNSHOT
    "TL0208",  # Nuclear accidents
    "TL0209",  # ELECTROCUTION
    "TL0210",  # Racionamiento
    "TL0302",  # POLLUTION
    "TL0307",  # MINING HAZARD
}

geocoder = MockGeocoder()


class DesinventarData(TypedDict):
    zip_file_url: str
    country_code: str
    iso3: str


class DesinventarScenario(TypedDict):
    name: str
    data: DesinventarData


grenada_data: DesinventarScenario = {
    "name": "Grenada subset",
    "data": {
        "zip_file_url": "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/docs/model/sources/DesInventar/DI_export_grd.zip",
        "country_code": "grd",
        "iso3": "GRD",
    },
}


def load_scenarios(
    scenarios: list[DesinventarScenario],
) -> List[Tuple[str, DesinventarTransformer]]:
    transformers: List[Tuple[str, DesinventarTransformer]] = []
    for scenario in scenarios:
        data = scenario["data"]
        # download zip file in temp folder
        response = requests.get(data["zip_file_url"])
        tmp_zip_file = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
        tmp_zip_file.write(response.content)
        data_source = DesinventarDataSource(
            data=DesinventarDataSourceType(
                tmp_zip_file=File(path=tmp_zip_file, data_type=DataType.FILE),
                source_url=data["zip_file_url"],
                country_code=data["country_code"],
                iso3=data["iso3"],
            )
        )
        transformers.append((data["country_code"], DesinventarTransformer(data_source, geocoder)))
    return transformers


class DesinventarTest(TestCase):
    scenarios = [grenada_data]

    def setUp(self) -> None:
        """Set up test environment"""
        super().setUp()
        self.validator = CustomValidator()
        # Create temporary folder for test outputs
        makedirs(get_data_file("temp/desinventar"), exist_ok=True)

    @parameterized.expand(load_scenarios(scenarios))  # type: ignore[misc]
    @pytest.mark.vcr()
    def test_transformer(self, country_code: str, transformer: DesinventarTransformer) -> None:
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

        items = list(transformer.get_stac_items())

        self.assertTrue(len(items) > 0)

        source_event_items = []
        source_impact_items = []

        for item in items:
            # Write pretty JSON in temporary folder for manual inspection
            item_path = get_data_file(f"temp/desinventar/{item.id}.json")
            with open(item_path, "w", encoding="utf-8") as f:
                json.dump(item.to_dict(), f, indent=2, ensure_ascii=False)

            # Validate item against schema
            item.validate(validator=self.validator)

            # Check item type
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_items.append(item)
            elif monty_item_ext.is_source_impact():
                source_impact_items.append(item)

        # Verify required items were created
        # source_event_items contains items
        self.assertTrue(len(source_event_items) > 0)
        # source_impact_items contains items
        self.assertTrue(len(source_impact_items) > 0)

        # Verify Correlation ID
        hazard_profiles = MontyHazardProfiles()
        for source_event_item in source_event_items:
            event_item_hazard_code = hazard_profiles.get_canonical_hazard_codes(source_event_item)[0].upper()
            validate_correlation_id(source_event_item.properties.get("monty:corr_id"), event_item_hazard_code)
        for source_impact_item in source_impact_items:
            impact_item_hazard_code = hazard_profiles.get_canonical_hazard_codes(source_impact_item)[0].upper()
            validate_correlation_id(source_impact_item.properties.get("monty:corr_id"), impact_item_hazard_code)

    @parameterized.expand(load_scenarios(scenarios))  # type: ignore[misc]
    @pytest.mark.vcr()
    def test_transformer_item_links(self, country_code: str, transformer: DesinventarTransformer) -> None:
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

        items = list(transformer.get_stac_items())

        self.assertTrue(len(items) > 0)

        source_event_items = []
        source_impact_items = []

        for item in items:
            # Write pretty JSON in temporary folder for manual inspection
            item_path = get_data_file(f"temp/desinventar/{item.id}.json")
            with open(item_path, "w", encoding="utf-8") as f:
                json.dump(item.to_dict(), f, indent=2, ensure_ascii=False)

            # Validate item against schema
            item.validate(validator=self.validator)

            # Check item type
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_items.append(item)
            elif monty_item_ext.is_source_impact():
                source_impact_items.append(item)

        # Verify required items were created
        # source_event_items contains items
        self.assertTrue(len(source_event_items) > 0)
        # source_impact_items contains items
        self.assertTrue(len(source_impact_items) > 0)

        # Verify Related links exists
        event_item_related_items = source_event_items[0].get_links(rel="related")
        impact_item_related_items = source_impact_items[0].get_links(rel="related")
        event_item_self_link = source_event_items[0].self_href
        impact_item_self_link = source_impact_items[0].self_href

        self.assertTrue(len(event_item_related_items) > 0)
        self.assertTrue(len(impact_item_related_items) > 0)

        assert all(link.href is not None and link.href != event_item_self_link for link in event_item_related_items)
        assert all(link.href is not None and link.href != impact_item_self_link for link in impact_item_related_items)

        assert event_item_self_link in [item.href for item in impact_item_related_items]
        assert impact_item_self_link in [item.href for item in event_item_related_items]

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_event_item_uses_all_codes(self, country_code: str, transformer: DesinventarTransformer) -> None:
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

        for item in transformer.get_stac_items():
            # write pretty json in a temporary folder
            item_path = get_data_file(f"temp/desinventar/{item.id}.json")
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event() and monty_item_ext.hazard_codes:
                undrr_2025_code = monty_item_ext.hazard_codes[0]
                if undrr_2025_code in UNDRR_2025_CODES_WITHOUT_EMDAT_CODE:
                    # No EM-DAT cluster defined for this hazard: [UNDRR code, GLIDE code]
                    assert len(monty_item_ext.hazard_codes) == 2
                else:
                    # Full trio: [UNDRR code, GLIDE code, EM-DAT cluster code]
                    assert len(monty_item_ext.hazard_codes) == 3

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_hazard_item_uses_2025_code_only(self, country_code: str, transformer: DesinventarTransformer) -> None:
        request_for_schema(url=CURRENT_SCHEMA_URI)  # Validate if the schema exists

        for item in transformer.get_stac_items():
            # write pretty json in a temporary folder
            item_path = get_data_file(f"temp/desinventar/{item.id}.json")
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_hazard() and monty_item_ext.hazard_codes:
                # Should contain only the first code (UNDRR-ISC 2025)
                assert len(monty_item_ext.hazard_codes) == 1
