"""Tests for pystac.tests.extensions.monty"""

import json
import unittest
from os import makedirs

import pytest
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.sources.gidd import GIDDDataSource, GIDDTransformer
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator

MOCK_GIDD_DATA = """
{
   "type":"FeatureCollection",
   "readme": "TITLE: Disasters Global Internal Displacement Database (GIDD)",
   "lastUpdated":"2025-01-31",
   "features":[
      {
         "type":"Feature",
         "geometry":{
            "type":"MultiPoint",
            "coordinates":[
               [
                  63.97263,
                  32.187931
               ]
            ]
         },
         "properties":{
            "ID":115637,
            "ISO3":"AFG",
            "Country":"Afghanistan",
            "Geographical region":"South Asia",
            "Figure cause":"Disaster",
            "Year":2023,
            "Figure category":"Internal Displacements",
            "Figure unit":"Household",
            "Reported figures":300,
            "Household size":8.04,
            "Total figures":2412,
            "Hazard category":"Weather related",
            "Hazard sub category":"Climatological",
            "Hazard type":"Drought",
            "Hazard sub type":"Drought",
            "Start date":"2023-06-07",
            "Start date accuracy":"Month",
            "End date":"2023-06-07",
            "End date accuracy":"Day",
            "Publishers":[
               "Media"
            ],
            "Sources":[
               "Displaced people",
               "Local residents",
               "Local Authorities"
            ],
            "Sources type":[
               "Other",
               "Civil Society",
               "Local Authority"
            ],
            "Event ID":17021,
            "Event name":"Afghanistan: Drought - Helmand (Wasir) - 07/06/2023",
            "Event cause":"Disaster",
            "Event main trigger":"Drought",
            "Event start date":"2023-06-07",
            "Event end date":"2023-06-07",
            "Event start date accuracy":"Month",
            "Event end date accuracy":"Month",
            "Is housing destruction":"No",
            "Event codes (Code:Type)":[
               [
                  "DR-2021-000022-AFG",
                  "Glide Number"
               ],
               [
                  "MDRAF007",
                  "IFRC Appeal ID"
               ]
            ],
            "Locations name":[
               "Washir, Helmand, Afghanistan"
            ],
            "Locations accuracy":[
               "District/Zone/Department (ADM2)"
            ],
            "Locations type":[
               "Origin"
            ],
            "Displacement occurred":"Displacement without preventive evacuations reported"
         }
      }
    ]
}"""


def load_scenarios(
    scenarios: list[tuple[str, str]],
) -> list[GIDDTransformer]:
    transformers = []
    for scenario in scenarios:
        data = scenario[1]
        gidd_data_source = GIDDDataSource(scenario[1], data)
        transformers.append(GIDDTransformer(gidd_data_source))
    return transformers


spain_flood = ("spain_flood", MOCK_GIDD_DATA)


class GIDDTest(unittest.TestCase):
    scenarios = [spain_flood]

    def setUp(self) -> None:
        super().setUp()
        self.validator = CustomValidator()
        # create temporary folder
        makedirs(get_data_file("temp/gidd"), exist_ok=True)

    @parameterized.expand(load_scenarios(scenarios))
    @pytest.mark.vcr()
    def test_transformer(self, transformer: GIDDTransformer) -> None:
        items = transformer.make_items()
        self.assertTrue(len(items) > 0)
        source_event_item = None
        source_impact_item = None
        for item in items:
            # write pretty json in a temporary folder for manual inspection
            item_path = get_data_file(f"temp/gidd/{item.id}.json")
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_item = item
            elif monty_item_ext.is_source_impact():
                source_impact_item = item

        self.assertIsNotNone(source_event_item)
        self.assertIsNotNone(source_impact_item)
