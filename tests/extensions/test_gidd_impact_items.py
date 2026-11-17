"""Offline checks of the GIDD event and impact items.

The fixture holds two real GIDD records of event 22194 (the 2023 Syria and Türkiye
earthquake): 107479 (Syria, flow figure) and 136080 (Türkiye, stock figure). They come
from monty-stac-extension docs/model/sources/IDMC.
"""

import copy
import json
from pathlib import Path

import pytest

from pystac_monty.extension import MontyExtension, MontyImpactType
from pystac_monty.geocoding import MockGeocoder
from pystac_monty.sources.common import DataType, GenericDataSource, Memory
from pystac_monty.sources.gidd import GIDDDataSource, GIDDTransformer
from pystac_monty.sources.utils import IDMCUtils

FIXTURE = Path(__file__).resolve().parents[1] / "data-files" / "gidd" / "gidd-22194.json"
SOURCE_URL = "https://helix-tools-api.idmcdb.org/external-api/gidd/disaggregations/disaggregation-geojson/"


def _features():
    return copy.deepcopy(json.loads(FIXTURE.read_text()))


def _items(features):
    data_source = GIDDDataSource(
        data=GenericDataSource(
            source_url=SOURCE_URL,
            input_data=Memory(content=json.dumps(features), data_type=DataType.MEMORY),
        )
    )
    items = list(GIDDTransformer(data_source, MockGeocoder()).get_stac_items())
    events = [item for item in items if MontyExtension.ext(item).is_source_event()]
    impacts = {item.id: item for item in items if MontyExtension.ext(item).is_source_impact()}
    return events, impacts


def _impact_for_category(figure_category: str):
    feature = _features()[0]
    feature["properties"]["Figure category"] = figure_category
    _, impacts = _items([feature])
    (item,) = impacts.values()
    return item


def test_impact_id_separates_event_id_and_record_id():
    _, impacts = _items(_features())
    assert set(impacts) == {
        "idmc-gidd-impact-22194-107479-internal-displacements",
        "idmc-gidd-impact-22194-136080-idps",
    }


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
    item = _impact_for_category(figure_category)
    assert MontyExtension.ext(item).impact_detail.type == expected_type


def test_impact_items_use_their_own_record():
    features = _features()
    (event,), impacts = _items(features)
    turkiye = impacts["idmc-gidd-impact-22194-136080-idps"]
    source = features[1]["properties"]

    assert turkiye.geometry == features[1]["geometry"]
    assert turkiye.bbox == [38.27644, 37.764, 38.27644, 37.764]
    assert MontyExtension.ext(turkiye).country_codes == ["TUR"]
    assert "TUR" in turkiye.properties["keywords"]
    assert "SYR" not in turkiye.properties["keywords"]
    assert turkiye.properties["location"] == source["Locations name"]
    assert turkiye.properties["location_type"] == source["Locations type"]
    assert turkiye.properties["sources"] == source["Sources"]
    # The impact item keeps the correlation id of its event.
    assert MontyExtension.ext(turkiye).correlation_id == MontyExtension.ext(event).correlation_id


def test_event_lists_every_country_with_the_first_record_first():
    (event,), _ = _items(_features())
    assert MontyExtension.ext(event).country_codes == ["SYR", "TUR"]


def test_displacement_occurred_is_read():
    _, impacts = _items(_features())
    item = impacts["idmc-gidd-impact-22194-107479-internal-displacements"]
    assert item.properties["displacement_occured"] == "Displacement without preventive evacuations reported"


def test_impact_title_names_the_unit_once():
    _, impacts = _items(_features())
    item = impacts["idmc-gidd-impact-22194-107479-internal-displacements"]
    assert item.properties["title"] == "Internal Displacements-Household-Syria, Türkiye: Earthquake - 06/02/2023"


@pytest.mark.parametrize(
    ("hazard", "expected"),
    [
        (("Weather related", "Climatological", "Sea level rise", "Sea level rise"), ["EN0402", "nat-geo-env-slr", "OT"]),
        (("Weather related", "Hydrological", "Mass movement", "Avalanche"), ["MH0801", "nat-hyd-mmw-ava", "AV"]),
    ],
)
def test_hazard_codes_mapping(hazard, expected):
    assert IDMCUtils.hazard_codes_mapping(hazard=hazard) == expected
