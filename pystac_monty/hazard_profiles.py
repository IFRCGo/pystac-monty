import importlib.resources
from abc import ABC, abstractmethod

import pandas as pd
from pystac import Item


class HazardProfiles(ABC):
    @abstractmethod
    def get_cluster_code(self, item: Item) -> str:
        pass


class MontyHazardProfiles(HazardProfiles):
    impact_information_profile_path = "HazardProfiles.csv"
    impact_information_profile_data = None
    IMPACT_CLUSTER_CODE_COLUMN = "emdat_key"

    # free impact_information_profile_data when the object is destroyed
    def __del__(self) -> None:
        if self.impact_information_profile_data is not None:
            del self.impact_information_profile_data

    def get_profiles(self) -> pd.DataFrame:
        if self.impact_information_profile_data is None:
            with importlib.resources.files("pystac_monty").joinpath(self.impact_information_profile_path).open("rb") as f:
                self.impact_information_profile_data = pd.read_csv(f)
        return self.impact_information_profile_data

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
            # first try to get the cluster code from the hazard undrr key column
            try:
                cluster_code = profiles.loc[profiles["undrr_key"] == c, self.IMPACT_CLUSTER_CODE_COLUMN].values[-1]
            except IndexError:
                pass
            # then try the emdat key column
            if not cluster_code:
                try:
                    cluster_code = profiles.loc[profiles["emdat_key"] == c, self.IMPACT_CLUSTER_CODE_COLUMN].values[-1]
                except IndexError:
                    pass
            # finally try the glide key column
            if not cluster_code:
                try:
                    rows = profiles[profiles["glide_code"] == c]
                    # Several raow may match the glide code, so we must associate the rest of the hazard codes
                    # to find the most relevant cluster code
                    if len(rows) > 1:
                        # get the first having the undrr key in the hazard codes
                        for i, row in rows.iterrows():
                            if row["undrr_key"] in monty.hazard_codes:
                                cluster_code = row[self.IMPACT_CLUSTER_CODE_COLUMN]
                                break
                            if row["emdat_key"] in monty.hazard_codes:
                                cluster_code = row[self.IMPACT_CLUSTER_CODE_COLUMN]
                                break

                        # Get the first having no undrr key in the hazard codes
                        rows = rows[rows["undrr_key"].isna()]
                        if not cluster_code and len(rows) > 0:
                            cluster_code = rows.iloc[0][self.IMPACT_CLUSTER_CODE_COLUMN]

                    else:
                        cluster_code = cluster_codes[-1]
                except IndexError:
                    pass
            if cluster_code:
                cluster_codes.append(cluster_code)
        if not cluster_codes:
            raise ValueError("No cluster code found for hazard codes {}".format(monty.hazard_codes))

        # Remove the nan items
        cluster_codes = pd.Series(cluster_codes).dropna().tolist()
        # In case of a tie, return the first item alphabetically
        count_dict = {code: cluster_codes.count(code) for code in set(cluster_codes)}
        max_count = max(count_dict.values())
        max_codes = [code for code, count in count_dict.items() if count == max_count]
        return str(min(max_codes))
