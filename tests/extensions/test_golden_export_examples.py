"""Golden tests: batch export output must match ``monty-stac-extension/examples`` trees."""

from __future__ import annotations

from pathlib import Path

import pytest

from pystac_monty.sources.charter import convert_charter_activations
from tests.utils.golden_export_compare import assert_golden_export_subtrees_match

REPO_ROOT = Path(__file__).resolve().parents[2]
MONTY_EXT = REPO_ROOT / "monty-stac-extension"
CHARTER_MODEL = MONTY_EXT / "docs" / "model" / "sources" / "Charter"
CHARTER_EXAMPLES_SUBDIRS = ("charter-events", "charter-hazards", "charter-response")


def test_charter_export_matches_monty_extension_examples(tmp_path: Path) -> None:
    """Full Charter export equals normalized JSON in the submodule ``examples`` tree."""
    if not CHARTER_MODEL.is_dir() or not (MONTY_EXT / "examples" / "charter-events").is_dir():
        pytest.skip("monty-stac-extension Charter model/examples not present (submodule not initialized)")

    convert_charter_activations(CHARTER_MODEL, tmp_path)
    assert_golden_export_subtrees_match(
        examples_dir=MONTY_EXT / "examples",
        produced_dir=tmp_path,
        subdirs=CHARTER_EXAMPLES_SUBDIRS,
    )
