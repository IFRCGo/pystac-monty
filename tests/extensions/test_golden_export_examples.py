"""Golden tests: batch export output must match ``monty-stac-extension/examples`` trees.

Charter is the first source covered. To add another source later:

1. Commit golden JSON under ``monty-stac-extension/examples/<slug>-{events,hazards,...}``.
2. Register a test that calls the source's ``convert_*`` into a temp directory.
3. Reuse :func:`tests.utils.golden_export_compare.assert_golden_export_subtrees_match` with the
   appropriate ``subdirs`` tuple.
"""

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
    examples_root = MONTY_EXT / "examples"
    if not CHARTER_MODEL.is_dir():
        pytest.skip("monty-stac-extension Charter model directory not present")
    if not (examples_root / "charter-events").is_dir():
        pytest.skip("monty-stac-extension examples/charter-* not present")

    convert_charter_activations(CHARTER_MODEL, tmp_path)
    assert_golden_export_subtrees_match(
        examples_dir=examples_root,
        produced_dir=tmp_path,
        subdirs=CHARTER_EXAMPLES_SUBDIRS,
    )
