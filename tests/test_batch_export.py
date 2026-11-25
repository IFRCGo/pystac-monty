"""Integration tests for batch export of existing transformers."""

from __future__ import annotations

import json
from pathlib import Path

from tests.conftest import get_data_file


def test_convert_glide_writes_role_collections(tmp_path: Path) -> None:
    from pystac_monty.sources.batch_export import convert_glide

    convert_glide(Path(get_data_file("glide/spain-flood.json")), tmp_path)
    assert (tmp_path / "glide-events" / "glide-events.json").is_file()
    assert (tmp_path / "glide-hazards" / "glide-hazards.json").is_file()
    events = list((tmp_path / "glide-events").glob("glide-event-*.json"))
    assert events


def test_cli_lists_registered_sources() -> None:
    from pystac_monty.sources.batch_export import BATCH_EXPORTS

    assert {"glide", "gfd", "gdacs", "charter", "cems", "idmc-gidd", "idmc-idu"}.issubset(BATCH_EXPORTS)


def test_convert_gdacs_writes_role_collections(tmp_path: Path) -> None:
    from pystac_monty.sources.batch_export import convert_gdacs

    convert_gdacs(Path(get_data_file("gdacs/fl-1102983-bundle.json")), tmp_path)
    assert (tmp_path / "gdacs-events" / "gdacs-events.json").is_file()
    assert (tmp_path / "gdacs-hazards" / "gdacs-hazards.json").is_file()
    events = list((tmp_path / "gdacs-events").glob("gdacs-event-*.json"))
    assert events


def test_run_batch_glide(tmp_path: Path) -> None:
    from pystac_monty.sources.batch_export import run_batch

    run_batch("glide", Path(get_data_file("glide/spain-flood.json")), tmp_path)
    collection = json.loads((tmp_path / "glide-events" / "glide-events.json").read_text(encoding="utf-8"))
    assert collection["id"] == "glide-events"


def _items(collection_dir: Path) -> dict[str, dict]:
    docs = [json.loads(path.read_text(encoding="utf-8")) for path in collection_dir.glob("*.json")]
    return {doc["id"]: doc for doc in docs if doc.get("type") == "Feature"}


def _assert_related_links_resolve(collection_dir: Path) -> int:
    """Each `related` link is relative and points at a file of the export. Returns the number of links."""
    count = 0
    for item in _items(collection_dir).values():
        for link in item["links"]:
            if link["rel"] != "related":
                continue
            assert link["href"].startswith("../"), f"{item['id']}: {link['href']!r} is not relative"
            assert (collection_dir / link["href"]).resolve().is_file(), f"{item['id']}: {link['href']} does not exist"
            assert link["type"] == "application/geo+json"
            count += 1
    return count


def test_convert_gidd_writes_events_and_impacts(tmp_path: Path) -> None:
    from pystac_monty.sources.batch_export import GIDD_SOURCE_URL, run_batch

    # The GIDD API gives a FeatureCollection. The test fixture holds its features only.
    features = json.loads(Path(get_data_file("gidd/gidd-22194.json")).read_text(encoding="utf-8"))
    input_path = tmp_path / "gidd.geojson"
    input_path.write_text(json.dumps({"type": "FeatureCollection", "features": features}), encoding="utf-8")
    output_dir = tmp_path / "out"

    run_batch("idmc-gidd", input_path, output_dir)

    events = _items(output_dir / "idmc-gidd-events")
    impacts = _items(output_dir / "idmc-gidd-impacts")
    assert set(events) == {"idmc-gidd-event-22194"}
    assert len(impacts) == 2
    assert all(impact_id.startswith("idmc-gidd-impact-22194-") for impact_id in impacts)
    # The output does not depend on the place of the input file.
    assert events["idmc-gidd-event-22194"]["assets"]["source"]["href"] == GIDD_SOURCE_URL
    assert _assert_related_links_resolve(output_dir / "idmc-gidd-events") == 2
    assert _assert_related_links_resolve(output_dir / "idmc-gidd-impacts") == 2


def test_convert_idu_writes_events_and_impacts(tmp_path: Path) -> None:
    from pystac_monty.sources.batch_export import IDU_SOURCE_URL, run_batch

    run_batch("idmc-idu", Path(get_data_file("idu/idu-26451.json")), tmp_path)

    events = _items(tmp_path / "idmc-idu-events")
    impacts = _items(tmp_path / "idmc-idu-impacts")
    assert set(events) == {"idmc-idu-event-26451"}
    assert len(impacts) == 2
    assert all(impact_id.startswith("idmc-idu-impact-26451-") for impact_id in impacts)
    via = [link["href"] for link in events["idmc-idu-event-26451"]["links"] if link["rel"] == "via"]
    assert via == [IDU_SOURCE_URL]
    assert _assert_related_links_resolve(tmp_path / "idmc-idu-events") == 2
    assert _assert_related_links_resolve(tmp_path / "idmc-idu-impacts") == 2


def test_related_links_between_exported_items_are_relative(tmp_path: Path) -> None:
    """GLIDE links the event and the hazard with the Item object as the target."""
    from pystac_monty.sources.batch_export import convert_glide

    convert_glide(Path(get_data_file("glide/spain-flood.json")), tmp_path)

    assert _assert_related_links_resolve(tmp_path / "glide-events") >= 1
    assert _assert_related_links_resolve(tmp_path / "glide-hazards") >= 1
