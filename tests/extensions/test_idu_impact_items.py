"""Offline checks of the IDU impact items.

The fixture holds two real IDU records of event 26451 (August 2024 storms in the Balearic
Islands, Spain), in two different places, from monty-stac-extension docs/model/sources/IDMC.
"""

import json
from pathlib import Path

from pystac_monty.extension import MontyExtension
from pystac_monty.geocoding import MockGeocoder
from pystac_monty.sources.common import DataType, GenericDataSource, Memory
from pystac_monty.sources.idu import IDUDataSource, IDUTransformer

FIXTURE = Path(__file__).resolve().parents[1] / "data-files" / "idu" / "idu-26451.json"
SOURCE_URL = "https://helix-tools-api.idmcdb.org/external-api/idus/last-180-days/"


def _items():
    records = json.loads(FIXTURE.read_text())
    data_source = IDUDataSource(
        data=GenericDataSource(
            source_url=SOURCE_URL,
            input_data=Memory(content=json.dumps(records), data_type=DataType.MEMORY),
        )
    )
    items = list(IDUTransformer(data_source, MockGeocoder()).get_stac_items())
    impacts = {item.id: item for item in items if MontyExtension.ext(item).is_source_impact()}
    events = [item for item in items if MontyExtension.ext(item).is_source_event()]
    return records, events, impacts


def test_impact_items_use_their_own_record():
    records, (event,), impacts = _items()
    second = records[1]  # not the record that the event item is built from
    (item,) = [i for i in impacts.values() if f"-{second['id']}-" in i.id]

    assert item.geometry == {"type": "Point", "coordinates": (second["longitude"], second["latitude"])}
    assert item.bbox == [second["longitude"], second["latitude"], second["longitude"], second["latitude"]]
    assert item.assets["report"].href == second["source_url"]
    assert item.properties["location"] == second["locations_name"]
    assert "12 displacements" in item.properties["description"]
    assert MontyExtension.ext(item).country_codes == [second["iso3"]]
    # The impact item keeps the correlation id of its event.
    assert MontyExtension.ext(item).correlation_id == MontyExtension.ext(event).correlation_id
    # The event item still comes from the first record.
    assert event.geometry["coordinates"] == (records[0]["longitude"], records[0]["latitude"])
