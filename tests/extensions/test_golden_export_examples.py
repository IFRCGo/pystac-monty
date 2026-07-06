"""Golden tests for generated static examples."""

from __future__ import annotations

import json
from pathlib import Path

from pystac_monty.sources.charter import convert_charter_example_activations


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_charter_export_matches_submodule_examples(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    submodule_root = repo_root / "monty-stac-extension"
    source_dir = submodule_root / "docs" / "model" / "sources" / "Charter"
    expected_root = submodule_root / "examples"

    convert_charter_example_activations(source_dir, tmp_path)

    expected_files = sorted(path for folder in expected_root.glob("charter-*") for path in folder.glob("*.json"))
    generated_files = sorted(path for folder in tmp_path.glob("charter-*") for path in folder.glob("*.json"))

    expected_rel = {path.relative_to(expected_root) for path in expected_files}
    generated_rel = {path.relative_to(tmp_path) for path in generated_files}

    missing = sorted(expected_rel - generated_rel)
    extra = sorted(generated_rel - expected_rel)
    changed = [
        rel for rel in sorted(expected_rel & generated_rel) if _load_json(expected_root / rel) != _load_json(tmp_path / rel)
    ]

    assert not missing, f"Missing generated Charter examples: {[str(path) for path in missing]}"
    assert not extra, f"Unexpected generated Charter examples: {[str(path) for path in extra]}"
    assert not changed, f"Generated Charter examples differ from golden files: {[str(path) for path in changed]}"
