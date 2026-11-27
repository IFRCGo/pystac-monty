"""Guard: source mappings must only contain UNDRR-ISC 2025 hazard codes.

The 2020 Hazard Information Profiles are deprecated. A 2020 code (or a code
that does not exist) in a source mapping goes into ``monty:hazard_codes``
unchanged, and the item then has no UNDRR-ISC 2025 code.
"""

import re
from pathlib import Path

import pytest

import pystac_monty
from pystac_monty.hazard_profiles import MontyHazardProfiles

SOURCES_DIR = Path(pystac_monty.__file__).parent / "sources"
UNDRR_CODE_LITERAL = re.compile(r"""["']([A-Z]{2}\d{4})["']""")

# Codes that are not in HIPs 2025 but are still in a source mapping.
# Each entry needs a decision on the correct 2025 code. Do not add entries:
# fix the mapping instead.
KNOWN_INVALID_CODES = {
    "pdc.py": {"MH0700", "OT0000", "SO0203", "TL0000", "TL0200"},
}


def _undrr_code_literals(path: Path) -> set[str]:
    codes: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        code_part = line.split("#", 1)[0]
        codes.update(UNDRR_CODE_LITERAL.findall(code_part))
    return codes


@pytest.mark.parametrize("source_file", sorted(SOURCES_DIR.glob("*.py")), ids=lambda p: p.name)
def test_source_mappings_only_use_undrr_2025_codes(source_file: Path) -> None:
    valid_codes = set(MontyHazardProfiles().get_profiles()[MontyHazardProfiles.UNDRR_2025_KEY_COLUMN].dropna())
    known_invalid = KNOWN_INVALID_CODES.get(source_file.name, set())

    found = _undrr_code_literals(source_file)
    invalid = found - valid_codes

    assert invalid - known_invalid == set(), f"{source_file.name} uses codes that are not UNDRR-ISC 2025 codes"
    assert known_invalid - invalid == set(), f"{source_file.name}: remove the fixed codes from KNOWN_INVALID_CODES"
