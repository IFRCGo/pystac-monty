from datetime import datetime

import pytest
from pystac import Item

from pystac_monty.extension import MontyExtension
from pystac_monty.hazard_profiles import MontyHazardProfiles

TEST_DATETIME = datetime(2024, 1, 1)


def test_load_profiles() -> None:
    """Test that hazard profiles can be loaded from CSV."""
    profile = MontyHazardProfiles()
    df = profile.get_profiles()
    assert not df.empty
    assert "undrr_key" in df.columns
    assert "emdat_key" in df.columns
    assert "glide_code" in df.columns


def test_get_cluster_code_undrr() -> None:
    """Test getting cluster code from UNDRR key."""
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["MH0035"]  # Drought

    cluster_code = profile.get_cluster_code(item)
    assert cluster_code == "nat-cli-dro-dro"


def test_get_cluster_code_emdat() -> None:
    """Test getting cluster code from EMDAT key."""
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["nat-cli-dro-dro"]  # Drought

    cluster_code = profile.get_cluster_code(item)
    assert cluster_code == "nat-cli-dro-dro"


def test_get_cluster_code_glide() -> None:
    """Test getting cluster code from GLIDE code."""
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["DR"]  # Drought

    cluster_code = profile.get_cluster_code(item)
    assert cluster_code == "nat-cli-dro-dro"


def test_get_cluster_code_multiple() -> None:
    """Test getting cluster code when multiple hazard codes are present."""
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    # Drought (UNDRR) and Flood (GLIDE)
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["MH0035", "FL"]

    cluster_code = profile.get_cluster_code(item)
    # Should return the first matching cluster code
    assert cluster_code == "nat-cli-dro-dro"


def test_get_cluster_code_no_hazard_codes() -> None:
    """Test error handling when no hazard codes are present."""
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = []

    with pytest.raises(ValueError, match="No hazard codes found in item"):
        profile.get_cluster_code(item)


def test_get_cluster_code_invalid_codes() -> None:
    """Test error handling when invalid hazard codes are provided."""
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["INVALID_CODE"]

    with pytest.raises(ValueError, match="No cluster code found for hazard codes"):
        profile.get_cluster_code(item)


def test_get_cluster_code_majority() -> None:
    """Test that the majority cluster code is returned when multiple codes are present."""
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    # Two floods (riverine) and one drought
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["MH0007", "FL", "MH0035"]

    cluster_code = profile.get_cluster_code(item)
    # Should return flood cluster code as it appears more times
    assert cluster_code == "nat-hyd-flo-riv"


def test_get_cluster_code_tie_breaker() -> None:
    """Test that when multiple cluster codes have the same count, the first in the list is chosen."""
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    # One drought (nat-cli-dro-dro) and one avalanche (nat-geo-ava-ava)
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["MH0035", "AV"]
    cluster_code = profile.get_cluster_code(item)
    # Should return drought as it comes first
    assert cluster_code == "nat-cli-dro-dro"


def test_get_cluster_code_glide_multiple_matches() -> None:
    """Test handling of multiple rows matching a GLIDE code with matching UNDRR key."""
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    # Add both GLIDE code and corresponding UNDRR key
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["FL", "MH0007"]

    cluster_code = profile.get_cluster_code(item)
    # Should match the row with matching UNDRR key
    assert cluster_code == "nat-hyd-flo-riv"


def test_get_cluster_code_glide_multiple_no_match() -> None:
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["nat-hyd-flo-flo"]

    cluster_code = profile.get_cluster_code(item)
    # Should use first available row when no matching keys found
    assert cluster_code == "nat-hyd-flo-flo"


def test_memory_management() -> None:
    """Test that profile data is properly freed when object is destroyed."""
    profile = MontyHazardProfiles()
    # Access the data to load it
    profile.get_profiles()
    assert profile.impact_information_profile_data is not None

    # Delete the object
    del profile

    # Create new instance to verify data is reloaded
    new_profile = MontyHazardProfiles()
    assert new_profile.impact_information_profile_data is None
    # Access data again to ensure it can be reloaded
    df = new_profile.get_profiles()
    assert not df.empty


def test_get_cluster_code_glide_multiple_emdat_match() -> None:
    """Test handling of multiple rows matching a GLIDE code with matching EMDAT key."""
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    # Add both GLIDE code and corresponding EMDAT key
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["FL", "nat-hyd-flo-riv"]

    cluster_code = profile.get_cluster_code(item)
    # Should match the row with matching EMDAT key
    assert cluster_code == "nat-hyd-flo-riv"


def test_get_cluster_code_glide_multiple_fallback() -> None:
    """Test handling of multiple rows matching a GLIDE code falling back to rows with no UNDRR key."""
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    # Add GLIDE code that matches multiple rows, but with no matching UNDRR/EMDAT keys
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["FL"]

    # First try to match rows with UNDRR keys (none will match)
    # Then fall back to first row with no UNDRR key
    cluster_code = profile.get_cluster_code(item)
    assert cluster_code == "nat-hyd-flo-flo"


# Tests for get_canonical_hazard_codes (HIPs 2025)


def test_get_canonical_hazard_codes_with_undrr_2025() -> None:
    """Test getting canonical codes when UNDRR 2025 code is already present."""
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    # Flood with UNDRR 2025 + GLIDE
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["MH0600", "FL"]

    canonical = profile.get_canonical_hazard_codes(item)
    assert len(canonical) == 2
    assert canonical[0] == "MH0600"  # UNDRR 2025
    assert canonical[1] == "FL"  # GLIDE


def test_get_canonical_hazard_codes_derive_from_glide() -> None:
    """Test deriving UNDRR 2025 code from GLIDE code."""
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    # Only GLIDE code - should derive MH0600 (General Flooding)
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["FL"]

    canonical = profile.get_canonical_hazard_codes(item)
    assert len(canonical) >= 1
    # First should be UNDRR 2025 code
    assert canonical[0].startswith("MH")  # Should be a Met & Hydro code
    assert len(canonical[0]) == 6  # Format: XX0000


def test_get_canonical_hazard_codes_full_trio() -> None:
    """Test getting full trio: UNDRR 2025 + GLIDE + EM-DAT."""
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    # Complete set for riverine flooding
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["MH0604", "FL", "nat-hyd-flo-riv"]

    canonical = profile.get_canonical_hazard_codes(item)
    assert len(canonical) == 3
    assert canonical[0] == "MH0604"  # UNDRR 2025
    assert canonical[1] == "FL"  # GLIDE
    assert canonical[2] == "nat-hyd-flo-riv"  # EM-DAT


def test_get_canonical_hazard_codes_earthquake() -> None:
    """Test canonical codes for earthquake."""
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    # Earthquake with GLIDE + EM-DAT
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["EQ", "nat-geo-ear-gro"]

    canonical = profile.get_canonical_hazard_codes(item)
    assert len(canonical) >= 2
    assert canonical[0] == "GH0101"  # UNDRR 2025 Earthquake
    assert canonical[1] == "EQ"  # GLIDE


def test_get_canonical_hazard_codes_only_emdat() -> None:
    """Test deriving canonical codes from only EM-DAT code."""
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    # Only EM-DAT code
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["nat-cli-dro-dro"]

    canonical = profile.get_canonical_hazard_codes(item)
    assert len(canonical) >= 1
    # Should derive UNDRR 2025 code for drought
    assert canonical[0] == "MH0401"  # Drought
    assert "nat-cli-dro-dro" in canonical  # EM-DAT should be preserved


def test_get_canonical_hazard_codes_tropical_cyclone() -> None:
    """Test canonical codes for tropical cyclone.

    Note: TC + nat-met-sto-tro maps to MH0306 (Depression or Cyclone) by default.
    For specific tropical cyclone (MH0309), it should be provided explicitly.
    """
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    # TC with EM-DAT code - defaults to first match (MH0306 Depression/Cyclone)
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["TC", "nat-met-sto-tro"]

    canonical = profile.get_canonical_hazard_codes(item)
    assert len(canonical) >= 2
    assert canonical[0] == "MH0306"  # Depression or Cyclone (first match)
    assert "TC" in canonical  # GLIDE


def test_get_canonical_hazard_codes_tropical_cyclone_specific() -> None:
    """Test canonical codes when specific tropical cyclone code is provided."""
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    # Explicit MH0309 for specific Tropical Cyclone
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["MH0309", "TC", "nat-met-sto-tro"]

    canonical = profile.get_canonical_hazard_codes(item)
    assert len(canonical) == 3
    assert canonical[0] == "MH0309"  # Specific Tropical Cyclone
    assert canonical[1] == "TC"  # GLIDE
    assert canonical[2] == "nat-met-sto-tro"  # EM-DAT


def test_get_canonical_hazard_codes_no_codes() -> None:
    """Test error handling when no hazard codes present."""
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = []

    with pytest.raises(ValueError, match="No hazard codes found in item"):
        profile.get_canonical_hazard_codes(item)


def test_get_canonical_hazard_codes_multiple_glide() -> None:
    """Test that only first GLIDE code is included in canonical set."""
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    # Multiple GLIDE codes (invalid per spec, but test graceful handling)
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["FL", "DR", "MH0600"]

    canonical = profile.get_canonical_hazard_codes(item)
    # Should have UNDRR 2025 + first GLIDE only (max 3 total)
    assert len(canonical) <= 3
    assert canonical[0] == "MH0600"  # UNDRR 2025
    # Only first GLIDE code should be included
    glide_codes = [c for c in canonical if c in ["FL", "DR"]]
    assert len(glide_codes) == 1
    assert glide_codes[0] == "FL"  # First one


def test_get_canonical_hazard_codes_wildfire() -> None:
    """Test canonical codes for wildfire."""
    profile = MontyHazardProfiles()
    item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
    # Wildfire
    MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["WF", "nat-cli-wil-wil"]

    canonical = profile.get_canonical_hazard_codes(item)
    assert len(canonical) >= 2
    assert canonical[0] == "EN0205"  # UNDRR 2025 Wildfires
    assert "WF" in canonical  # GLIDE
