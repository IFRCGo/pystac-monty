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
