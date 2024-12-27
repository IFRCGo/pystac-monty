import importlib

import pandas as df


class HazardProfiles:
    impact_information_profile_path = "ImpactInformationProfiles.csv"
    impact_information_profile_data = None

    # free impact_information_profile_data when the object is destroyed
    def __del__(self):
        if self.impact_information_profile_data:
            del self.impact_information_profile_data
            # close the file
            importlib.resources.close(self.impact_information_profile_data)

    def get_profiles(self) -> df.DataFrame:
        if self.impact_information_profile_data is None:
            with importlib.resources.open_binary("pystac_monty", self.impact_information_profile_path) as f:
                self.impact_information_profile_data = df.read_csv(f)
        return self.impact_information_profile_data

    def get_cluster_code(self, hazard_code: str | list[str]) -> str:
        profiles = self.get_profiles()
        codes = hazard_code if isinstance(hazard_code, list) else [hazard_code]
        # Get the cluster and family codes for each code in the list
        cluster_codes = []
        for c in codes:
            cluster_code = None
            try:
                cluster_code = profiles.loc[profiles["name"] == c, "link_group"].values[-1]
            except IndexError:
                if not cluster_code and c.__len__() == 2:
                    cluster_code = c
            if cluster_code:
                cluster_codes.append(cluster_code)
        if not cluster_codes:
            raise ValueError("No cluster code found for hazard code")
        # return the majority item cluster code in the list
        return max(set(cluster_codes), key=cluster_codes.count)
