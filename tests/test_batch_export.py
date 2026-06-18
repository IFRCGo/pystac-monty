"""Integration tests for batch export of existing transformers."""

from __future__ import annotations

import json
from pathlib import Path

from tests.conftest import get_data_file


def test_convert_glide_writes_role_subcatalogs(tmp_path: Path) -> None:
    from pystac_monty.sources.batch_export import convert_glide

    convert_glide(Path(get_data_file("glide/spain-flood.json")), tmp_path)
    assert (tmp_path / "glide-events" / "glide-events.json").is_file()
    assert (tmp_path / "glide-hazards" / "glide-hazards.json").is_file()
    events = list((tmp_path / "glide-events").glob("glide-event-*.json"))
    assert events


def test_cli_lists_registered_sources() -> None:
    from pystac_monty.sources.batch_export import BATCH_EXPORTS

    assert {"glide", "gfd", "gdacs"}.issubset(BATCH_EXPORTS)


def test_run_batch_glide(tmp_path: Path) -> None:
    from pystac_monty.sources.batch_export import run_batch

    run_batch("glide", Path(get_data_file("glide/spain-flood.json")), tmp_path)
    collection = json.loads((tmp_path / "glide-events" / "glide-events.json").read_text(encoding="utf-8"))
    assert collection["id"] == "glide-events"
