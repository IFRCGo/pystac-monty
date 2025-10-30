import importlib.resources
from abc import ABC, abstractmethod
from typing import List, Optional

import pandas as pd
from pystac import Item


class HazardProfiles(ABC):
    @abstractmethod
    def get_cluster_code(self, item: Item) -> str:
        pass

    @abstractmethod
    def get_keywords(self, hazard_codes: List[str]) -> List[str]:
        """Get human-readable keywords for a list of hazard codes.

        Args:
            hazard_codes: List of hazard codes (UNDRR 2025, GLIDE, or EM-DAT)

        Returns:
            List of human-readable keywords describing the hazards
        """
        pass


class MontyHazardProfiles(HazardProfiles):
    impact_information_profile_path = "HazardProfiles.csv"
    impact_information_profile_data = None
    IMPACT_CLUSTER_CODE_COLUMN = "emdat_key"

    # Column names for hazard code lookups
    # HIPs 2025 is the primary reference classification
    UNDRR_2025_KEY_COLUMN = "undrr_2025_key"
    # HIPs 2020 for backward compatibility
    UNDRR_2020_KEY_COLUMN = "undrr_key"
    GLIDE_CODE_COLUMN = "glide_code"
    EMDAT_KEY_COLUMN = "emdat_key"
    LABEL_COLUMN = "label"
    CLUSTER_LABEL_COLUMN = "cluster_label"
    FAMILY_LABEL_COLUMN = "family_label"

    # free impact_information_profile_data when the object is destroyed
    def __del__(self) -> None:
        if self.impact_information_profile_data is not None:
            del self.impact_information_profile_data

    def get_profiles(self) -> pd.DataFrame:
        if self.impact_information_profile_data is None:
            with importlib.resources.files("pystac_monty").joinpath(self.impact_information_profile_path).open("rb") as f:
                self.impact_information_profile_data = pd.read_csv(f)
        return self.impact_information_profile_data

    @staticmethod
    def get_undrr_2025_code(hazard_codes: List[str]) -> Optional[str]:
        """Extract the UNDRR-ISC 2025 code from a list of hazard codes.

        According to HIPs 2025 specification, hazard items MUST have exactly one
        UNDRR-ISC 2025 code (format: 2 letters + 4 digits, e.g., MH0600, GH0101).

        Args:
            hazard_codes: List of hazard codes that may contain UNDRR 2025, GLIDE, and/or EM-DAT codes

        Returns:
            The UNDRR-ISC 2025 code if found, None otherwise
        """
        import re

        # UNDRR-ISC 2025 code pattern: 2 letters + 4 digits
        undrr_2025_pattern = re.compile(r"^[A-Z]{2}\d{4}$")

        for code in hazard_codes:
            if undrr_2025_pattern.match(code):
                return code
        return None

    def get_keywords(self, hazard_codes: List[str]) -> List[str]:
        """Get human-readable keywords for a list of hazard codes.

        Generates keywords from hazard codes including:
        - Hazard labels (e.g., "Flooding", "Earthquake")
        - Cluster labels (e.g., "Water-related", "Seismic")
        - Family labels (e.g., "Meteorological & Hydrological", "Geological")

        Args:
            hazard_codes: List of hazard codes (UNDRR 2025/2020, GLIDE, or EM-DAT)

        Returns:
            List of unique human-readable keywords
        """
        if not hazard_codes:
            return []

        profiles = self.get_profiles()
        keywords = set()

        for code in hazard_codes:
            # Try to find the code in the profiles using all available columns
            row = None

            # Priority order: UNDRR 2025, UNDRR 2020, EM-DAT, GLIDE
            for col in [
                self.UNDRR_2025_KEY_COLUMN,
                self.UNDRR_2020_KEY_COLUMN,
                self.EMDAT_KEY_COLUMN,
                self.GLIDE_CODE_COLUMN,
            ]:
                if col in profiles.columns:
                    matching_rows = profiles[profiles[col] == code]
                    if not matching_rows.empty:
                        row = matching_rows.iloc[0]
                        break

            if row is not None:
                # Add label (hazard name)
                if self.LABEL_COLUMN in row and pd.notna(row[self.LABEL_COLUMN]):
                    keywords.add(row[self.LABEL_COLUMN])

                # Add cluster label
                if self.CLUSTER_LABEL_COLUMN in row and pd.notna(row[self.CLUSTER_LABEL_COLUMN]):
                    keywords.add(row[self.CLUSTER_LABEL_COLUMN])

                # Add family label
                if self.FAMILY_LABEL_COLUMN in row and pd.notna(row[self.FAMILY_LABEL_COLUMN]):
                    keywords.add(row[self.FAMILY_LABEL_COLUMN])

        return sorted(list(keywords))

    def get_cluster_code(self, item: Item) -> str:
        from pystac_monty.extension import MontyExtension

        monty = MontyExtension.ext(item)
        if not monty.hazard_codes:
            raise ValueError("No hazard codes found in item")

        profiles = self.get_profiles()
        # Get the cluster and family codes for each code in the list
        cluster_codes = []
        for c in monty.hazard_codes:
            cluster_code = None
            # Priority order: UNDRR 2025 (primary), UNDRR 2020 (backward compat), EM-DAT, GLIDE
            # first try to get the cluster code from the UNDRR 2025 key column
            if self.UNDRR_2025_KEY_COLUMN in profiles.columns:
                try:
                    cluster_code = profiles.loc[
                        profiles[self.UNDRR_2025_KEY_COLUMN] == c, self.IMPACT_CLUSTER_CODE_COLUMN
                    ].values[-1]
                except IndexError:
                    pass

            # then try the UNDRR 2020 key column for backward compatibility
            if not cluster_code:
                try:
                    cluster_code = profiles.loc[
                        profiles[self.UNDRR_2020_KEY_COLUMN] == c, self.IMPACT_CLUSTER_CODE_COLUMN
                    ].values[-1]
                except IndexError:
                    pass

            # then try the emdat key column
            if not cluster_code:
                try:
                    cluster_code = profiles.loc[
                        profiles[self.EMDAT_KEY_COLUMN] == c, self.IMPACT_CLUSTER_CODE_COLUMN
                    ].values[-1]
                except IndexError:
                    pass
            # finally try the glide key column
            if not cluster_code:
                try:
                    rows = profiles[profiles[self.GLIDE_CODE_COLUMN] == c]
                    # Several rows may match the glide code, so we must associate the rest of the hazard codes
                    # to find the most relevant cluster code
                    if len(rows) > 1:
                        # get the first having the undrr 2025 key in the hazard codes
                        if self.UNDRR_2025_KEY_COLUMN in profiles.columns:
                            for i, row in rows.iterrows():
                                if row[self.UNDRR_2025_KEY_COLUMN] in monty.hazard_codes:
                                    cluster_code = row[self.IMPACT_CLUSTER_CODE_COLUMN]
                                    break
                        # get the first having the undrr 2020 key in the hazard codes
                        if not cluster_code:
                            for i, row in rows.iterrows():
                                if row[self.UNDRR_2020_KEY_COLUMN] in monty.hazard_codes:
                                    cluster_code = row[self.IMPACT_CLUSTER_CODE_COLUMN]
                                    break
                                if row[self.EMDAT_KEY_COLUMN] in monty.hazard_codes:
                                    cluster_code = row[self.IMPACT_CLUSTER_CODE_COLUMN]
                                    break

                        # Get the first having no undrr key in the hazard codes
                        rows_no_undrr = rows[rows[self.UNDRR_2020_KEY_COLUMN].isna()]
                        if not cluster_code and len(rows_no_undrr) > 0:
                            cluster_code = rows_no_undrr.iloc[0][self.IMPACT_CLUSTER_CODE_COLUMN]
                        if not cluster_code or isinstance(cluster_code, float):
                            cluster_code = monty.hazard_codes[-1]

                    else:
                        cluster_code = cluster_codes[-1] if cluster_codes else None
                except IndexError:
                    pass
            if cluster_code:
                cluster_codes.append(cluster_code)
        if not cluster_codes or len(cluster_codes) == 0:
            raise ValueError("No cluster code found for hazard codes {}".format(monty.hazard_codes))

        # Remove the nan items
        cluster_codes = pd.Series(cluster_codes).dropna().tolist()

        if not cluster_codes:
            raise ValueError("No cluster code found for hazard codes {}".format(monty.hazard_codes))
        # In case of a tie, return the first item alphabetically
        count_dict = {code: cluster_codes.count(code) for code in set(cluster_codes)}
        max_count = max(count_dict.values())
        max_codes = [code for code, count in count_dict.items() if count == max_count]
        # Return the first max_code as it appears in the original cluster_codes list
        for code in cluster_codes:
            if code in max_codes:
                return str(code)
