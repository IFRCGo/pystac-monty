"""Offline checks of the GIDD impact item id and impact type.

The fixture is one real GIDD record (ID 107479, event 22194, the 2023 Syria and Türkiye
earthquake), from monty-stac-extension docs/model/sources/IDMC.
"""

import copy
import json
from pathlib import Path

import pytest

from pystac_monty.extension import MontyExtension, MontyImpactType
from pystac_monty.geocoding import MockGeocoder
from pystac_monty.sources.common import DataType, GenericDataSource, Memory
from pystac_monty.sources.gidd import GIDDDataSource, GIDDTransformer

FIXTURE = Path(__file__).resolve().parents[1] / "data-files" / "gidd" / "gidd-107479.json"
SOURCE_URL = "https://helix-tools-api.idmcdb.org/external-api/gidd/disasters/disaster-export/"


def _impact_items(figure_category: str):
    feature = copy.deepcopy(json.loads(FIXTURE.read_text())[0])
    feature["properties"]["Figure category"] = figure_category
    data_source = GIDDDataSource(
        data=GenericDataSource(
            source_url=SOURCE_URL,
            input_data=Memory(content=json.dumps([feature]), data_type=DataType.MEMORY),
        )
    )
    transformer = GIDDTransformer(data_source, MockGeocoder())
    return [item for item in transformer.get_stac_items() if MontyExtension.ext(item).is_source_impact()]


def test_impact_id_separates_event_id_and_record_id():
    (item,) = _impact_items("Internal Displacements")
    assert item.id == "idmc-gidd-impact-22194-107479-internal-displacements"


@pytest.mark.parametrize(
    ("figure_category", "expected_type"),
    [
        ("Internal Displacements", MontyImpactType.INTERNALLY_DISPLACED_PERSONS),
        ("IDPs", MontyImpactType.INTERNALLY_DISPLACED_PERSONS),
        ("Deaths", MontyImpactType.DEATH),
        ("People displaced across borders", MontyImpactType.EXTERNALLY_DISPLACED_PERSONS),
    ],
)
def test_impact_type_follows_figure_category(figure_category, expected_type):
    (item,) = _impact_items(figure_category)
    assert MontyExtension.ext(item).impact_detail.type == expected_type
