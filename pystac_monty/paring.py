import importlib
from datetime import datetime

import pandas as df
from pystac import Item


class Pairing:
    impact_information_profile_path = "ImpactInformationProfiles.csv"
    impact_information_profile_data = None
    
    # free impact_information_profile_data when the object is destroyed
    def __del__(self):
        if self.impact_information_profile_data:
            del self.impact_information_profile_data
            # close the file
            importlib.resources.close(self.impact_information_profile_data)

    def get_profiles(self) -> df.DataFrame:
        if not self.impact_information_profile_data:
            with importlib.resources.open_binary('pystac_monty', self.impact_information_profile_path) as f:
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
                cluster_code = profiles.loc[profiles['name'] == c, "link_group"].values[-1]
            except IndexError:
                if not cluster_code and c.__len__() == 2:
                    cluster_code = c
            if cluster_code:
                cluster_codes.append(cluster_code)
        if not cluster_codes:
            raise ValueError("No cluster code found for hazard code")
        # return the majority item cluster code in the list
        return max(set(cluster_codes), key=cluster_codes.count)

    def generate_correlation_id(self, item: Item) -> str:
        # Get the necessary properties for creating the correlation id
        hazards = item.properties.get("monty:hazard_codes", [])
        country_codes = item.properties.get("monty:country_codes", [])
        event_datetime = item.datetime
        episode_number = item.properties.get("monty:episode_number", 0)

        if not hazards or not country_codes or not event_datetime or not episode_number:
            raise ValueError("Missing required properties to generate correlation id")

        # Assuming the first hazard code is the main one
        hazard_cluster_code = self.get_cluster_code(hazards[0])
        # This should be dynamically determined based on existing events
        eventdatestr = event_datetime.strftime("%Y%m%d")

        event_id = f"{eventdatestr}-{country_codes[0]}-{hazard_cluster_code}-{episode_number}-GCDB"  # noqa: E501
        return event_id
