from datetime import datetime
import math
import pytest
from pystac import Item

from pystac_monty.extension import MontyExtension
from pystac_monty.hazard_profiles import HazardProfiles, MontyHazardProfiles

TEST_DATETIME = datetime(2024, 1, 1)


class TestHazardProfilesAbstract:
    """Tests for the abstract HazardProfiles class."""

    def test_abstract_class(self) -> None:
        """Test that HazardProfiles is an abstract class that cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class HazardProfiles"):
            HazardProfiles()

    def test_abstract_method(self) -> None:
        """Test that get_cluster_code is an abstract method that must be implemented."""
        class ConcreteHazardProfiles(HazardProfiles):
            pass

        with pytest.raises(TypeError, match="Can't instantiate abstract class ConcreteHazardProfiles"):
            ConcreteHazardProfiles()

    def test_concrete_implementation(self) -> None:
        """Test that a concrete implementation can be created."""
        class ConcreteHazardProfiles(HazardProfiles):
            def get_cluster_code(self, item: Item) -> str:
                return "test-code"

        # This should not raise an exception
        profile = ConcreteHazardProfiles()
        item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
        assert profile.get_cluster_code(item) == "test-code"


class TestMontyHazardProfiles:
    """Tests for the MontyHazardProfiles class."""

    def test_load_profiles(self) -> None:
        """Test that hazard profiles can be loaded from CSV."""
        profile = MontyHazardProfiles()
        df = profile.get_profiles()
        assert not df.empty
        assert "undrr_key" in df.columns
        assert "emdat_key" in df.columns
        assert "glide_code" in df.columns

    def test_get_cluster_code_undrr(self) -> None:
        """Test getting cluster code from UNDRR key."""
        profile = MontyHazardProfiles()
        item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
        MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["MH0035"]  # Drought

        cluster_code = profile.get_cluster_code(item)
        assert cluster_code == "nat-cli-dro-dro"

    def test_get_cluster_code_emdat(self) -> None:
        """Test getting cluster code from EMDAT key."""
        profile = MontyHazardProfiles()
        item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
        MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["nat-cli-dro-dro"]  # Drought

        cluster_code = profile.get_cluster_code(item)
        assert cluster_code == "nat-cli-dro-dro"

    def test_get_cluster_code_glide(self) -> None:
        """Test getting cluster code from GLIDE code."""
        profile = MontyHazardProfiles()
        item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
        MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["DR"]  # Drought

        cluster_code = profile.get_cluster_code(item)
        assert cluster_code == "nat-cli-dro-dro"

    def test_get_cluster_code_multiple(self) -> None:
        """Test getting cluster code when multiple hazard codes are present."""
        profile = MontyHazardProfiles()
        item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
        # Drought (UNDRR) and Flood (GLIDE)
        MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["MH0035", "FL"]

        cluster_code = profile.get_cluster_code(item)
        # Should return the first matching cluster code
        assert cluster_code == "nat-cli-dro-dro"

    def test_get_cluster_code_no_hazard_codes(self) -> None:
        """Test error handling when no hazard codes are present."""
        profile = MontyHazardProfiles()
        item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
        MontyExtension.ext(item, add_if_missing=True).hazard_codes = []

        with pytest.raises(ValueError, match="No hazard codes found in item"):
            profile.get_cluster_code(item)

    def test_get_cluster_code_invalid_codes(self) -> None:
        """Test error handling when invalid hazard codes are provided."""
        profile = MontyHazardProfiles()
        item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
        MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["INVALID_CODE"]

        with pytest.raises(ValueError, match="No cluster code found for hazard codes"):
            profile.get_cluster_code(item)

    def test_get_cluster_code_majority(self) -> None:
        """Test that the majority cluster code is returned when multiple codes are present."""
        profile = MontyHazardProfiles()
        item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
        # Two floods (riverine) and one drought
        MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["MH0007", "FL", "MH0035"]

        cluster_code = profile.get_cluster_code(item)
        # Should return flood cluster code as it appears more times
        assert cluster_code == "nat-hyd-flo-riv"

    def test_get_cluster_code_tie_breaker(self) -> None:
        """Test that when multiple cluster codes have the same count, the first alphabetically is chosen."""
        profile = MontyHazardProfiles()
        item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
        # One drought (nat-cli-dro-dro) and one avalanche (AV)
        MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["MH0035", "AV"]

        cluster_code = profile.get_cluster_code(item)
        # Should return the first - "nat-cli-dro-dro" comes before "AV"
        assert cluster_code == "nat-cli-dro-dro"

    def test_get_cluster_code_glide_multiple_matches(self) -> None:
        """Test handling of multiple rows matching a GLIDE code with matching UNDRR key."""
        profile = MontyHazardProfiles()
        item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
        # Add both GLIDE code and corresponding UNDRR key
        MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["FL", "MH0007"]

        cluster_code = profile.get_cluster_code(item)
        # Should match the row with matching UNDRR key
        assert cluster_code == "nat-hyd-flo-riv"

    def test_get_cluster_code_glide_multiple_no_match(self) -> None:
        """Test handling when no matching keys are found."""
        profile = MontyHazardProfiles()
        item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
        MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["nat-hyd-flo-flo"]

        cluster_code = profile.get_cluster_code(item)
        # Should use first available row when no matching keys found
        assert cluster_code == "nat-hyd-flo-flo"

    def test_memory_management(self) -> None:
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

    def test_get_cluster_code_glide_multiple_emdat_match(self) -> None:
        """Test handling of multiple rows matching a GLIDE code with matching EMDAT key."""
        profile = MontyHazardProfiles()
        item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
        # Add both GLIDE code and corresponding EMDAT key
        MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["FL", "nat-hyd-flo-riv"]

        cluster_code = profile.get_cluster_code(item)
        # Should match the row with matching EMDAT key
        assert cluster_code == "nat-hyd-flo-riv"

    def test_get_cluster_code_glide_multiple_fallback(self) -> None:
        """Test handling of multiple rows matching a GLIDE code falling back to rows with no UNDRR key."""
        profile = MontyHazardProfiles()
        item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
        # Add GLIDE code that matches multiple rows, but with no matching UNDRR/EMDAT keys
        MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["FL"]

        # First try to match rows with UNDRR keys (none will match)
        # Then fall back to first row with no UNDRR key
        cluster_code = profile.get_cluster_code(item)
        assert cluster_code == "nat-hyd-flo-flo"

    def test_get_cluster_code_nan_fallback(self) -> None:
        """Test handling when cluster code is NaN and falls back to using the last hazard code."""
        profile = MontyHazardProfiles()
        item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
        
        # Create a test scenario where we'd get a NaN cluster code
        # We'll mock the get_profiles method to return a dataframe with NaN values
        import pandas as pd
        original_get_profiles = profile.get_profiles
        
        def mock_get_profiles():
            df = original_get_profiles()
            # Create a test row with NaN emdat_key for a specific glide_code
            test_row = pd.DataFrame({
                'undrr_key': [None],
                'glide_code': ['TEST_CODE'],
                'emdat_key': [None]  # This will be converted to NaN
            })
            return pd.concat([df, test_row])
        
        profile.get_profiles = mock_get_profiles
        
        # Set up the item with our test code and a fallback code
        MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["TEST_CODE", "MH0035"]
        
        # The code should fall back to using the last hazard code (MH0035 -> nat-cli-dro-dro)
        cluster_code = profile.get_cluster_code(item)
        assert cluster_code == "nat-cli-dro-dro"
        
        # Restore original method
        profile.get_profiles = original_get_profiles

    def test_get_cluster_code_all_nan(self) -> None:
        """Test handling when all cluster codes are NaN."""
        profile = MontyHazardProfiles()
        item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
        
        # Create a test scenario where all cluster codes would be NaN
        import pandas as pd
        original_get_profiles = profile.get_profiles
        
        def mock_get_profiles():
            df = original_get_profiles()
            # Create test rows with NaN emdat_key for all the hazard codes we'll use
            test_rows = pd.DataFrame({
                'undrr_key': [None, None],
                'glide_code': ['TEST_CODE1', 'TEST_CODE2'],
                'emdat_key': [None, None]  # These will be converted to NaN
            })
            return pd.concat([df, test_rows])
        
        profile.get_profiles = mock_get_profiles
        
        # Set up the item with only our test codes that will result in NaN cluster codes
        MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["TEST_CODE1", "TEST_CODE2"]
        
        # The code should raise a ValueError since no valid cluster codes were found
        with pytest.raises(ValueError, match="No cluster code found for hazard codes"):
            profile.get_cluster_code(item)
        
        # Restore original method
        profile.get_profiles = original_get_profiles

    def test_get_cluster_code_glide_multiple_no_undrr_match(self) -> None:
        """Test handling when multiple GLIDE codes match but none have matching UNDRR or EMDAT keys."""
        profile = MontyHazardProfiles()
        item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
        
        # Create a test scenario with multiple matching GLIDE codes but no matching UNDRR/EMDAT keys
        import pandas as pd
        original_get_profiles = profile.get_profiles
        
        def mock_get_profiles():
            df = original_get_profiles()
            # Create test rows with multiple entries for the same GLIDE code
            # but with UNDRR keys that won't match our hazard codes
            test_rows = pd.DataFrame({
                'undrr_key': ['UNDRR1', 'UNDRR2', None],
                'glide_code': ['TEST_GLIDE', 'TEST_GLIDE', 'TEST_GLIDE'],
                'emdat_key': ['EMDAT1', 'EMDAT2', 'EMDAT3']
            })
            return pd.concat([df, test_rows])
        
        profile.get_profiles = mock_get_profiles
        
        # Set up the item with only the test GLIDE code
        MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["TEST_GLIDE"]
        
        # The code should fall back to the first row with no UNDRR key
        cluster_code = profile.get_cluster_code(item)
        assert cluster_code == "EMDAT3"
        
        # Restore original method
        profile.get_profiles = original_get_profiles

    def test_custom_impact_cluster_code_column(self) -> None:
        """Test using a custom IMPACT_CLUSTER_CODE_COLUMN."""
        profile = MontyHazardProfiles()
        original_column = profile.IMPACT_CLUSTER_CODE_COLUMN
        
        try:
            # Change the column to use for cluster codes
            profile.IMPACT_CLUSTER_CODE_COLUMN = "glide_code"
            
            item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
            MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["MH0035"]  # Drought
            
            # Now it should return the glide_code instead of emdat_key
            cluster_code = profile.get_cluster_code(item)
            assert cluster_code == "DR"
        finally:
            # Restore the original column
            profile.IMPACT_CLUSTER_CODE_COLUMN = original_column
            
    def test_get_cluster_code_undrr_direct_match(self) -> None:
        """Test getting cluster code directly from UNDRR key column."""
        profile = MontyHazardProfiles()
        item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
        
        # Create a test scenario where we get a direct match from the undrr_key column
        import pandas as pd
        original_get_profiles = profile.get_profiles
        
        def mock_get_profiles():
            # Create a test dataframe with a direct match in the undrr_key column
            return pd.DataFrame({
                'undrr_key': ['TEST_UNDRR_KEY'],
                'glide_code': ['TEST_GLIDE'],
                'emdat_key': ['TEST_EMDAT_KEY']
            })
        
        profile.get_profiles = mock_get_profiles
        
        # Set up the item with our test undrr key
        MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["TEST_UNDRR_KEY"]
        
        # The code should get the cluster code directly from the undrr_key match
        cluster_code = profile.get_cluster_code(item)
        assert cluster_code == "TEST_EMDAT_KEY"
        
        # Restore original method
        profile.get_profiles = original_get_profiles
    
    def test_get_cluster_code_single_item(self) -> None:
        """Test getting cluster code when there's only one item in the list."""
        profile = MontyHazardProfiles()
        item = Item(id="test", geometry=None, bbox=None, datetime=TEST_DATETIME, properties={})
        
        # Set up the item with a single hazard code
        MontyExtension.ext(item, add_if_missing=True).hazard_codes = ["MH0035"]  # Drought
        
        # The code should return the single cluster code
        cluster_code = profile.get_cluster_code(item)
        assert cluster_code == "nat-cli-dro-dro"
