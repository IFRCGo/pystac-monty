import json
import unittest
from os import makedirs
from typing import List

import pytest
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import MockGeocoder
from pystac_monty.sources.common import DataType, File, GenericDataSource, Memory
from pystac_monty.sources.gfd import GFDDataSource, GFDTransformer
from pystac_monty.sources.utils import save_json_data_into_tmp_file
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator


def load_scenarios_from_file(data: List[dict]):
    transformers = []

    geocoder = MockGeocoder()

    data_file = save_json_data_into_tmp_file(data)
    gfd_data = GenericDataSource(source_url="http://www.gfd.test", input_data=File(path=data_file.name, data_type=DataType.FILE))
    gfd_data_source = GFDDataSource(data=gfd_data)
    transformers.append(GFDTransformer(gfd_data_source, geocoder))
    return transformers


def load_scenarios(data: List[dict]):
    transformers = []

    geocoder = MockGeocoder()

    gfd_data = GenericDataSource(source_url="http://www.gfd.test", input_data=Memory(content=data, data_type=DataType.MEMORY))
    gfd_data_source = GFDDataSource(data=gfd_data)
    transformers.append(GFDTransformer(gfd_data_source, geocoder))
    return transformers


data = [
    {
        "type": "Image",
        "bands": [
            {
                "id": "flooded",
                "data_type": {"type": "PixelType", "precision": "int", "min": 0, "max": 255},
                "dimensions": [5686, 8984],
                "crs": "EPSG:4326",
                "crs_transform": [0.002245788210298804, 0, 137.20418491999513, 0, -0.002245788210298804, -17.514902252120372],
            },
            {
                "id": "duration",
                "data_type": {"type": "PixelType", "precision": "int", "min": 0, "max": 65535},
                "dimensions": [5686, 8984],
                "crs": "EPSG:4326",
                "crs_transform": [0.002245788210298804, 0, 137.20418491999513, 0, -0.002245788210298804, -17.514902252120372],
            },
            {
                "id": "clear_views",
                "data_type": {"type": "PixelType", "precision": "int", "min": 0, "max": 65535},
                "dimensions": [5686, 8984],
                "crs": "EPSG:4326",
                "crs_transform": [0.002245788210298804, 0, 137.20418491999513, 0, -0.002245788210298804, -17.514902252120372],
            },
            {
                "id": "clear_perc",
                "data_type": {"type": "PixelType", "precision": "float"},
                "dimensions": [5686, 8984],
                "crs": "EPSG:4326",
                "crs_transform": [0.002245788210298804, 0, 137.20418491999513, 0, -0.002245788210298804, -17.514902252120372],
            },
            {
                "id": "jrc_perm_water",
                "data_type": {"type": "PixelType", "precision": "int", "min": 0, "max": 255},
                "dimensions": [5686, 8984],
                "crs": "EPSG:4326",
                "crs_transform": [0.002245788210298804, 0, 137.20418491999513, 0, -0.002245788210298804, -17.514902252120372],
            },
        ],
        "version": 1685079690200045,
        "id": "GLOBAL_FLOOD_DB/MODIS_EVENTS/V1/DFO_1586_From_20000218_to_20000301",
        "properties": {
            "dfo_centroid_y": -31.268059,
            "dfo_main_cause": "Monsoonal rain",
            "gfd_country_name": "['AUSTRALIA']",
            "dfo_centroid_x": 143.6978,
            "glide_index": "NA",
            "slope_threshold": 5,
            "dfo_severity": 2,
            "system:footprint": {
                "type": "LinearRing",
                "coordinates": [
                    [148.77567883770345, -37.692190384571404],
                    [149.9754567941598, -37.692185550952246],
                    [149.9749728427817, -17.513770729195038],
                    [149.17465716800353, -17.51377546120134],
                    [147.97772223305088, -17.512579461230274],
                    [146.3818089465066, -17.512579422994158],
                    [144.7858956959944, -17.512579420723743],
                    [143.1899824714047, -17.512579400063284],
                    [141.59406922326394, -17.512579402044587],
                    [139.99815600423668, -17.5125794432258],
                    [138.40224272344366, -17.512579385808014],
                    [137.20294874528088, -17.51377068144409],
                    [137.20246484353038, -37.69218554664975],
                    [139.59917774865693, -37.692190354915816],
                    [141.1950909277643, -37.69219034319341],
                    [143.58896079571315, -37.69219036366114],
                    [145.18487408134828, -37.69219039048359],
                    [147.17976561083674, -37.692190365837604],
                    [148.77567883770345, -37.692190384571404],
                ],
            },
            "threshold_b1b2": 0.711,
            "otsu_sample_res": 231.66,
            "dfo_displaced": 200,
            "id": 1586,
            "cc": "AUS",
            "dfo_validation_type": "News",
            "composite_type": "3Day",
            "system:time_end": 951868800000,
            "dfo_country": "Australia",
            "countries": "Australia",
            "dfo_other_country": "NA",
            "system:time_start": 950832000000,
            "dfo_dead": 1,
            "gfd_country_code": "['AS']",
            "threshold_type": "otsu",
            "threshold_b7": 1815.18,
            "system:asset_size": 8988914,
            "system:index": "DFO_1586_From_20000218_to_20000301",
        },
    }
]


class GFDTest(unittest.TestCase):
    scenarios = []

    def setUp(self) -> None:
        super().setUp()
        self.validator = CustomValidator()
        # create temporary folder
        makedirs(get_data_file("temp/gfd"), exist_ok=True)

    @parameterized.expand(load_scenarios(data))
    @pytest.mark.vcr()
    def test_transformer(self, transformer: GFDTransformer) -> None:
        items = transformer.make_items()
        self.assertTrue(len(items) > 0)
        source_event_item = None
        source_hazard_item = None
        source_impact_item = None

        for item in items:
            item_path = get_data_file(f"temp/gfd/{item.id}.json")
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

    @parameterized.expand(load_scenarios_from_file(data))
    @pytest.mark.vcr()
    def test_transformer_from_file(self, transformer: GFDTransformer) -> None:
        items = transformer.make_items()
        self.assertTrue(len(items) > 0)
        source_event_item = None
        source_hazard_item = None
        source_impact_item = None

        for item in items:
            item_path = get_data_file(f"temp/gfd/{item.id}.json")
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
