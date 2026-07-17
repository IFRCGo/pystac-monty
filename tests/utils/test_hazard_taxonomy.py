"""Helpers and unit tests for validating hazard codes against taxonomy.md."""

from __future__ import annotations

import re
import unittest
from pathlib import Path

import pytest

UNDRR_2025_PATTERN = re.compile(r"^[A-Z]{2}\d{4}$")
GLIDE_PATTERN = re.compile(r"^[A-Z]{2}$")
EMDAT_PATTERN = re.compile(r"^(nat|tec)(-[a-z]{3}){3}$")


def monty_stac_extension_root() -> Path:
    return Path(__file__).resolve().parents[2] / "monty-stac-extension"


def taxonomy_md_path() -> Path:
    return monty_stac_extension_root() / "docs" / "model" / "taxonomy.md"


def _parse_markdown_table_rows(text: str, start_heading: str) -> list[list[str]]:
    """Return trimmed cell lists for table rows after *start_heading* until a non-table line."""
    start = text.find(start_heading)
    if start < 0:
        raise ValueError(f"Heading not found: {start_heading!r}")

    rows: list[list[str]] = []
    for line in text[start:].splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            if rows:
                break
            continue
        if re.match(r"^\|\s*-+\s*\|", stripped):
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        rows.append(cells)
    return rows


def load_valid_undrr_2025_codes() -> set[str]:
    """Parse the Complete 2025 Hazard List table from taxonomy.md."""
    path = taxonomy_md_path()
    text = path.read_text(encoding="utf-8")
    rows = _parse_markdown_table_rows(text, "#### Complete 2025 Hazard List")
    codes = {row[0] for row in rows if row and UNDRR_2025_PATTERN.match(row[0])}
    if not codes:
        raise ValueError(f"No UNDRR-ISC 2025 codes parsed from {path}")
    return codes


def load_valid_glide_codes() -> set[str]:
    """Parse the GLIDE Classification table from taxonomy.md."""
    path = taxonomy_md_path()
    text = path.read_text(encoding="utf-8")
    rows = _parse_markdown_table_rows(text, "### GLIDE Classification")
    codes = {row[0] for row in rows if row and GLIDE_PATTERN.match(row[0])}
    if not codes:
        raise ValueError(f"No GLIDE codes parsed from {path}")
    return codes


def load_valid_emdat_codes() -> set[str]:
    """Parse the EM-DAT Classification table from taxonomy.md."""
    path = taxonomy_md_path()
    text = path.read_text(encoding="utf-8")
    rows = _parse_markdown_table_rows(text, "| Classification Key |")
    codes = {row[0] for row in rows if row and EMDAT_PATTERN.match(row[0])}
    if not codes:
        raise ValueError(f"No EM-DAT codes parsed from {path}")
    return codes


def assert_hazard_code_dict_valid(
    hazard_codes: dict[str, list[str]],
    *,
    label: str,
) -> None:
    """Assert every UNDRR, GLIDE, and EM-DAT code in a source crosswalk dict is taxonomy-valid."""
    if not taxonomy_md_path().is_file():
        raise FileNotFoundError("monty-stac-extension submodule not initialized")

    valid_undrr = load_valid_undrr_2025_codes()
    valid_glide = load_valid_glide_codes()
    valid_emdat = load_valid_emdat_codes()

    for key, codes in hazard_codes.items():
        for code in codes:
            if UNDRR_2025_PATTERN.match(code):
                assert code in valid_undrr, f"{label}[{key!r}] UNDRR code {code!r} not in taxonomy.md"
            elif GLIDE_PATTERN.match(code):
                assert code in valid_glide, f"{label}[{key!r}] GLIDE code {code!r} not in taxonomy.md"
            elif EMDAT_PATTERN.match(code):
                assert code in valid_emdat, f"{label}[{key!r}] EM-DAT code {code!r} not in taxonomy.md"
            else:
                raise AssertionError(f"{label}[{key!r}] unrecognized hazard code {code!r}")


class HazardTaxonomyTest(unittest.TestCase):
    def test_rejects_unknown_emdat_prefix(self) -> None:
        if not taxonomy_md_path().is_file():
            self.skipTest("monty-stac-extension submodule not initialized")

        with pytest.raises(AssertionError, match="unrecognized hazard code 'tec-tra'"):
            assert_hazard_code_dict_valid(
                {"x": ["MH0600", "tec-tra"]},
                label="TEST_HAZARD_CODES",
            )

    def test_rejects_invalid_full_emdat_key(self) -> None:
        if not taxonomy_md_path().is_file():
            self.skipTest("monty-stac-extension submodule not initialized")

        with pytest.raises(AssertionError, match="EM-DAT code 'nat-zzz-zzz-zzz' not in taxonomy.md"):
            assert_hazard_code_dict_valid(
                {"x": ["nat-zzz-zzz-zzz"]},
                label="TEST_HAZARD_CODES",
            )
