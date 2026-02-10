"""Tests for pystac.tests.extensions.monty"""

import json
import tempfile
import unittest
from os import makedirs
from typing import List, Tuple, Union

import pytest
from parameterized import parameterized

from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import MockGeocoder
from pystac_monty.sources.alerthub import AlertHubDataSource, AlertHubTransformer
from pystac_monty.sources.common import DataType, File, GenericDataSource, Memory
from pystac_monty.sources.utils import save_json_data_into_tmp_file
from tests.conftest import get_data_file
from tests.extensions.test_monty import CustomValidator

CURRENT_SCHEMA_URI = "https://ifrcgo.github.io/monty/v1.1.0/schema.json"
CURRENT_SCHEMA_MAPURL = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/json-schema/schema.json"

json_mock_data = {
    "data": {
        "public": {
            "historicalAlerts": {
                "limit": 10,
                "offset": 550,
                "count": 1026,
                "items": [
                    {
                        "id": "6518341",
                        "sent": "2026-02-18T03:44:00+00:00",
                        "status": "ACTUAL",
                        "msgType": "ALERT",
                        "identifier": "urn:oid:2.49.0.1.840.0.86ae699e8bc3cc99055dd10811948007e668357e.001.2",
                        "sender": "w-nws.webmaster@noaa.gov",
                        "source": None,
                        "scope": "Public",
                        "restriction": None,
                        "addresses": None,
                        "code": "IPAWSv1.0",
                        "note": None,
                        "references": None,
                        "incidents": None,
                        "url": "https://api.weather.gov/alerts/urn:oid:2.49.0.1.840.0.86ae699e8bc3cc99055dd10811948007e668357e.001.2",
                        "country": {"id": "206", "name": "United States", "iso3": "USA"},
                        "admin1s": [{"id": "-206", "name": "Unknown", "isUnknown": True, "alertCount": 512}],
                        "info": {
                            "id": "9591305",
                            "alertId": "6518341",
                            "event": "Avalanches",
                            "language": "en-US",
                            "category": "MET",
                            "severity": "SEVERE",
                            "urgency": "EXPECTED",
                            "certainty": "LIKELY",
                            "headline": "High Wind Warning issued February 17 at 8:44PM MST until February 19 at 9:00PM MST by NWS Midland/Odessa TX",  # noqa
                            "description": "* WHAT...For the first High Wind Warning, west winds 35 to 45 mph\nwith gusts up to 60 mph. For the second High Wind Warning, west\nwinds 45 to 65 mph with gusts up to 80 mph expected.\n\n* WHERE...Guadalupe Mountains of west Texas and Southeast New Mexico.\n\n* WHEN...For the first High Wind Warning, until 10 PM MST /11 PM\nCST/ this evening. For the second High Wind Warning, from 9 AM MST\n/10 AM CST/ Wednesday to 9 PM MST /10 PM CST/ Thursday.\n\n* IMPACTS...Travel will be difficult, especially for high profile\nvehicles like campers, vans, and tractor trailers. Aviation\ninterests may experience localized but extreme turbulence, or\nstrong downward airflows if flying in the near the Guadalupe\nMountains. Severe turbulence near the mountains will be hazardous\nfor low flying light aircraft.\n",  # noqa
                            "instruction": "Winds will be particularly hazardous at higher elevations where the\nstrongest winds are likely to occur. Be especially careful driving\nin these mountainous areas. People driving high-profile vehicles\nshould strongly consider postponing travel in these areas until the\nwinds subside.\n\nUse caution when driving as blowing dust may reduce visibility.\n\nDelay travel through Guadalupe Pass or find another route, if\npossible.\n\nUse caution if flying low near the mountains as severe turbulence is\npossible.",  # noqa
                            "onset": "2026-02-18T16:00:00+00:00",
                            "effective": "2026-02-18T03:44:00+00:00",
                            "expires": "2026-02-18T18:30:00+00:00",
                            "eventCode": "None",
                            "areas": [
                                {
                                    "id": "88592416",
                                    "alertInfoId": "9591305",
                                    "areaDesc": "Guadalupe Mountains of Eddy County; Guadalupe Mountains Above 7000 Feet; Guadalupe and Delaware Mountains",  # noqa
                                    "polygons": [],
                                    "geocodes": [
                                        {
                                            "id": "140925712",
                                            "alertInfoAreaId": "88592416",
                                            "valueName": "SAME",
                                            "value": "035015",
                                        },
                                        {
                                            "id": "140925713",
                                            "alertInfoAreaId": "88592416",
                                            "valueName": "SAME",
                                            "value": "048109",
                                        },
                                        {"id": "140925714", "alertInfoAreaId": "88592416", "valueName": "UGC", "value": "NMZ027"},
                                        {"id": "140925715", "alertInfoAreaId": "88592416", "valueName": "UGC", "value": "TXZ270"},
                                        {"id": "140925716", "alertInfoAreaId": "88592416", "valueName": "UGC", "value": "TXZ271"},
                                    ],
                                }
                            ],
                        },
                        "infos": [
                            {
                                "id": "9591305",
                                "alertId": "6518341",
                                "event": "Avalanches",
                                "language": "en-US",
                                "category": "MET",
                                "severity": "SEVERE",
                                "urgency": "EXPECTED",
                                "certainty": "LIKELY",
                                "headline": "High Wind Warning issued February 17 at 8:44PM MST until February 19 at 9:00PM MST by NWS Midland/Odessa TX",  # noqa
                                "description": "* WHAT...For the first High Wind Warning, west winds 35 to 45 mph\nwith gusts up to 60 mph. For the second High Wind Warning, west\nwinds 45 to 65 mph with gusts up to 80 mph expected.\n\n* WHERE...Guadalupe Mountains of west Texas and Southeast New Mexico.\n\n* WHEN...For the first High Wind Warning, until 10 PM MST /11 PM\nCST/ this evening. For the second High Wind Warning, from 9 AM MST\n/10 AM CST/ Wednesday to 9 PM MST /10 PM CST/ Thursday.\n\n* IMPACTS...Travel will be difficult, especially for high profile\nvehicles like campers, vans, and tractor trailers. Aviation\ninterests may experience localized but extreme turbulence, or\nstrong downward airflows if flying in the near the Guadalupe\nMountains. Severe turbulence near the mountains will be hazardous\nfor low flying light aircraft.\n",  # noqa
                                "instruction": "Winds will be particularly hazardous at higher elevations where the\nstrongest winds are likely to occur. Be especially careful driving\nin these mountainous areas. People driving high-profile vehicles\nshould strongly consider postponing travel in these areas until the\nwinds subside.\n\nUse caution when driving as blowing dust may reduce visibility.\n\nDelay travel through Guadalupe Pass or find another route, if\npossible.\n\nUse caution if flying low near the mountains as severe turbulence is\npossible.",  # noqa
                                "onset": "2026-02-18T16:00:00+00:00",
                                "effective": "2026-02-18T03:44:00+00:00",
                                "expires": "2026-02-18T18:30:00+00:00",
                                "eventCode": "None",
                            }
                        ],
                    },
                    {
                        "id": "6518336",
                        "sent": "2026-02-18T02:13:41+00:00",
                        "status": "ACTUAL",
                        "msgType": "ALERT",
                        "identifier": "2.49.0.1.643.0.20260218.021341.0192704.00.RU",
                        "sender": "web@mecom.ru",
                        "source": "None",
                        "scope": "Public",
                        "restriction": "None",
                        "addresses": "None",
                        "code": "None",
                        "note": "None",
                        "references": "None",
                        "incidents": "None",
                        "url": "https://meteoinfo.ru/hmc-output/cap/cap-feed/ru/20260218021341-0192704.xml",
                        "country": {"id": "161", "name": "Russian Federation", "iso3": "RUS"},
                        "admin1s": [{"id": "1149", "name": "Sakhalinskaya Oblast", "isUnknown": False, "alertCount": 38}],
                        "info": {
                            "id": "9591301",
                            "alertId": "6518336",
                            "event": "Tropical Cyclone",
                            "language": "ru",
                            "category": "MET",
                            "severity": "MODERATE",
                            "urgency": "IMMEDIATE",
                            "certainty": "LIKELY",
                            "headline": "Test Event",
                            "description": "Test Event",
                            "instruction": "None",
                            "onset": "2026-02-19T22:00:00+00:00",
                            "effective": "2026-02-18T02:13:00+00:00",
                            "expires": "2026-02-20T09:00:00+00:00",
                            "eventCode": "None",
                            "areas": [
                                {
                                    "id": "88592412",
                                    "alertInfoId": "9591301",
                                    "areaDesc": "",
                                    "polygons": [
                                        {
                                            "id": "10106041",
                                            "valuePolygon": {
                                                "type": "Polygon",
                                                "coordinates": [
                                                    [
                                                        [156.401, 50.626],
                                                        [156.351, 50.637],
                                                        [156.286, 50.647],
                                                        [156.198, 50.67],
                                                        [156.189, 50.674],
                                                        [156.181, 50.68],
                                                        [156.17, 50.691],
                                                        [156.164, 50.709],
                                                        [156.164, 50.729],
                                                        [156.169, 50.741],
                                                        [156.175, 50.754],
                                                        [156.189, 50.768],
                                                        [156.341, 50.851],
                                                        [156.353, 50.856],
                                                        [156.365, 50.859],
                                                        [156.443, 50.87],
                                                        [156.454, 50.871],
                                                        [156.468, 50.867],
                                                        [156.491, 50.846],
                                                        [156.496, 50.832],
                                                        [156.488, 50.753],
                                                        [156.452, 50.708],
                                                        [156.435, 50.691],
                                                        [156.414, 50.674],
                                                        [156.405, 50.655],
                                                        [156.401, 50.626],
                                                    ]
                                                ],
                                            },
                                        },
                                        {
                                            "id": "10106042",
                                            "valuePolygon": {
                                                "type": "Polygon",
                                                "coordinates": [
                                                    [
                                                        [153.981, 48.735],
                                                        [153.975, 48.736],
                                                        [153.973, 48.746],
                                                        [153.977, 48.762],
                                                        [153.988, 48.78],
                                                        [154.116, 48.898],
                                                        [154.129, 48.906],
                                                        [154.141, 48.91],
                                                        [154.161, 48.912],
                                                        [154.182, 48.914],
                                                        [154.216, 48.91],
                                                        [154.23, 48.899],
                                                        [154.227, 48.883],
                                                        [154.219, 48.871],
                                                        [154.189, 48.835],
                                                        [154.063, 48.743],
                                                        [154.054, 48.737],
                                                        [154.031, 48.727],
                                                        [154.019, 48.723],
                                                        [154.004, 48.722],
                                                        [153.981, 48.735],
                                                    ]
                                                ],
                                            },
                                        },
                                        {
                                            "id": "10106043",
                                            "valuePolygon": {
                                                "type": "Polygon",
                                                "coordinates": [
                                                    [
                                                        [155.226, 50.053],
                                                        [155.224, 50.053],
                                                        [155.212, 50.067],
                                                        [155.209, 50.081],
                                                        [155.222, 50.234],
                                                        [155.248, 50.301],
                                                        [155.392, 50.353],
                                                        [155.427, 50.364],
                                                        [155.438, 50.367],
                                                        [155.449, 50.368],
                                                        [155.545, 50.375],
                                                        [155.628, 50.378],
                                                        [155.638, 50.378],
                                                        [155.649, 50.381],
                                                        [155.665, 50.388],
                                                        [155.749, 50.446],
                                                        [155.763, 50.461],
                                                        [155.852, 50.592],
                                                        [155.856, 50.604],
                                                        [155.858, 50.617],
                                                        [155.861, 50.637],
                                                        [155.862, 50.653],
                                                        [155.883, 50.687],
                                                        [155.893, 50.696],
                                                        [155.979, 50.747],
                                                        [156.01, 50.762],
                                                        [156.022, 50.767],
                                                        [156.034, 50.771],
                                                        [156.046, 50.774],
                                                        [156.067, 50.776],
                                                        [156.077, 50.773],
                                                        [156.104, 50.761],
                                                        [156.114, 50.751],
                                                        [156.121, 50.72],
                                                        [156.125, 50.7],
                                                        [156.151, 50.522],
                                                        [155.893, 50.264],
                                                        [155.795, 50.189],
                                                        [155.786, 50.185],
                                                        [155.764, 50.181],
                                                        [155.742, 50.179],
                                                        [155.732, 50.179],
                                                        [155.702, 50.184],
                                                        [155.692, 50.184],
                                                        [155.619, 50.183],
                                                        [155.596, 50.178],
                                                        [155.526, 50.147],
                                                        [155.516, 50.142],
                                                        [155.499, 50.131],
                                                        [155.483, 50.119],
                                                        [155.471, 50.108],
                                                        [155.357, 50.053],
                                                        [155.338, 50.057],
                                                        [155.319, 50.06],
                                                        [155.298, 50.06],
                                                        [155.256, 50.058],
                                                        [155.245, 50.057],
                                                        [155.226, 50.053],
                                                    ]
                                                ],
                                            },
                                        },
                                        {
                                            "id": "10106044",
                                            "valuePolygon": {
                                                "type": "Polygon",
                                                "coordinates": [
                                                    [
                                                        [152.207, 47.125],
                                                        [152.237, 47.145],
                                                        [152.264, 47.16],
                                                        [152.289, 47.149],
                                                        [152.274, 47.127],
                                                        [152.254, 47.113],
                                                        [152.232, 47.102],
                                                        [152.174, 47.063],
                                                        [152.087, 46.978],
                                                        [152.033, 46.923],
                                                        [152.026, 46.91],
                                                        [152.016, 46.892],
                                                        [151.832, 46.783],
                                                        [151.822, 46.779],
                                                        [151.801, 46.773],
                                                        [151.782, 46.771],
                                                        [151.768, 46.773],
                                                        [151.737, 46.785],
                                                        [151.72, 46.795],
                                                        [151.712, 46.801],
                                                        [151.705, 46.844],
                                                        [151.715, 46.853],
                                                        [151.725, 46.857],
                                                        [151.745, 46.862],
                                                        [151.765, 46.862],
                                                        [151.835, 46.855],
                                                        [151.849, 46.857],
                                                        [151.86, 46.861],
                                                        [151.869, 46.866],
                                                        [152.02, 46.989],
                                                        [152.048, 47.017],
                                                        [152.104, 47.073],
                                                        [152.12, 47.091],
                                                        [152.181, 47.143],
                                                        [152.202, 47.16],
                                                        [152.221, 47.173],
                                                        [152.207, 47.125],
                                                    ]
                                                ],
                                            },
                                        },
                                        {
                                            "id": "10106045",
                                            "valuePolygon": {
                                                "type": "Polygon",
                                                "coordinates": [
                                                    [
                                                        [154.465, 49.168],
                                                        [154.469, 49.169],
                                                        [154.493, 49.167],
                                                        [154.565, 49.152],
                                                        [154.583, 49.146],
                                                        [154.594, 49.136],
                                                        [154.601, 49.122],
                                                        [154.596, 49.109],
                                                        [154.583, 49.101],
                                                        [154.521, 49.076],
                                                        [154.505, 49.074],
                                                        [154.487, 49.081],
                                                        [154.471, 49.098],
                                                        [154.441, 49.158],
                                                        [154.45, 49.168],
                                                        [154.465, 49.168],
                                                    ]
                                                ],
                                            },
                                        },
                                        {
                                            "id": "10106046",
                                            "valuePolygon": {
                                                "type": "Polygon",
                                                "coordinates": [
                                                    [
                                                        [155.448, 50.88],
                                                        [155.448, 50.898],
                                                        [155.474, 50.921],
                                                        [155.483, 50.926],
                                                        [155.494, 50.93],
                                                        [155.506, 50.931],
                                                        [155.57, 50.934],
                                                        [155.58, 50.934],
                                                        [155.601, 50.931],
                                                        [155.639, 50.921],
                                                        [155.649, 50.909],
                                                        [155.666, 50.871],
                                                        [155.67, 50.857],
                                                        [155.66, 50.828],
                                                        [155.644, 50.814],
                                                        [155.621, 50.806],
                                                        [155.588, 50.803],
                                                        [155.557, 50.804],
                                                        [155.547, 50.806],
                                                        [155.527, 50.81],
                                                        [155.498, 50.817],
                                                        [155.481, 50.828],
                                                        [155.473, 50.836],
                                                        [155.461, 50.851],
                                                        [155.449, 50.872],
                                                        [155.448, 50.88],
                                                    ]
                                                ],
                                            },
                                        },
                                    ],
                                    "geocodes": [],
                                }
                            ],
                        },
                        "infos": [
                            {
                                "id": "9591301",
                                "alertId": "6518336",
                                "event": "Tropical Cyclone",
                                "language": "ru",
                                "category": "MET",
                                "severity": "MODERATE",
                                "urgency": "IMMEDIATE",
                                "certainty": "LIKELY",
                                "headline": "Test Event",
                                "description": "Test Event",
                                "instruction": "None",
                                "onset": "2026-02-19T22:00:00+00:00",
                                "effective": "2026-02-18T02:13:00+00:00",
                                "expires": "2026-02-20T09:00:00+00:00",
                                "eventCode": "None",
                            }
                        ],
                    },
                ],
            }
        }
    }
}

DATA_FILE = save_json_data_into_tmp_file(json_mock_data)


def load_scenarios(
    scenarios: Union[List[Tuple[str, dict]], tempfile._TemporaryFileWrapper],
) -> list[AlertHubTransformer]:
    transformers = []
    if isinstance(scenarios, tempfile._TemporaryFileWrapper):
        alerthub_data_source = AlertHubDataSource(
            data=GenericDataSource(
                source_url="https://alerthub-api.ifrc.org/graphql/",
                input_data=File(path=DATA_FILE.name, data_type=DataType.FILE),
            )
        )
        geocoder = MockGeocoder()
        transformers.append(AlertHubTransformer(alerthub_data_source, geocoder))
    else:
        for scenario in scenarios:
            data = scenario[1]
            alert_data_source = AlertHubDataSource(
                data=GenericDataSource(source_url=scenario[0], input_data=Memory(content=data, data_type=DataType.MEMORY))
            )
            geocoder = MockGeocoder()
            transformers.append(AlertHubTransformer(alert_data_source, geocoder))
    return transformers


alerthub_data = [("https://alerthub-api.ifrc.org/graphql/", json_mock_data)]


class AlertHubTest(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.validator = CustomValidator()
        # create temporary folder
        makedirs(get_data_file("temp/alerthub"), exist_ok=True)

    @parameterized.expand(load_scenarios(DATA_FILE))
    @pytest.mark.vcr()
    def test_transformer_from_file(self, transformer: AlertHubTransformer) -> None:
        items = transformer.make_items()
        self.assertTrue(len(items) > 0)
        source_event_item = None
        source_hazard_item = None
        for item in items:
            # write pretty json in a temporary folder for manual inspection
            item_path = get_data_file(f"temp/alerthub/{item.id}.json")
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_item = item
            elif monty_item_ext.is_source_hazard():
                source_hazard_item = item

        self.assertIsNotNone(source_event_item)
        self.assertIsNotNone(source_hazard_item)

    @parameterized.expand(load_scenarios(alerthub_data))
    @pytest.mark.vcr()
    def test_transformer_from_data(self, transformer: AlertHubTransformer) -> None:
        items = transformer.make_items()
        self.assertTrue(len(items) > 0)
        source_event_item = None
        source_hazard_item = None
        for item in items:
            # write pretty json in a temporary folder for manual inspection
            item_path = get_data_file(f"temp/alerthub/{item.id}.json")
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext.is_source_event():
                source_event_item = item
            elif monty_item_ext.is_source_hazard():
                source_hazard_item = item

        self.assertIsNotNone(source_event_item)
        self.assertIsNotNone(source_hazard_item)

    @parameterized.expand(load_scenarios(DATA_FILE))
    def test_ifrc_hazard_codes_2025(self, transformer: AlertHubTransformer):
        assert transformer._get_hazard_codes("met", "flood watch") == ["MH0600", "nat-hyd-flo-flo", "FL"]
        assert transformer._get_hazard_codes("met", "tropical cyclone") == ["MH0306"]
        assert transformer._get_hazard_codes("met", "tsunami") == ["MH0705", "TS"]

    @parameterized.expand(load_scenarios(DATA_FILE))
    @pytest.mark.vcr()
    def test_hazard_item_uses_2025_code_only(self, transformer: AlertHubTransformer) -> None:
        for item in transformer.get_stac_items():
            # write pretty json in a temporary folder
            item_path = get_data_file(f"temp/alerthub/{item.id}.json")
            with open(item_path, "w") as f:
                json.dump(item.to_dict(), f, indent=2)
            item.validate(validator=self.validator)
            monty_item_ext = MontyExtension.ext(item)
            if monty_item_ext and monty_item_ext.is_source_hazard() and monty_item_ext.hazard_codes:
                # Should contain only the first code (UNDRR-ISC 2025)
                assert len(monty_item_ext.hazard_codes) == 1
