from pystac import Item

from pystac_monty.hazard_profiles import HazardProfiles


class Pairing:
    def generate_correlation_id(self, item: Item, hazard_profiles: HazardProfiles) -> str:
        # Get the necessary properties for creating the correlation id
        hazards = item.properties.get("monty:hazard_codes", [])
        country_codes = item.properties.get("monty:country_codes", [])
        event_datetime = item.datetime
        episode_number = item.properties.get("monty:episode_number", 0)

        if not hazards or not country_codes or not event_datetime or not episode_number:
            raise ValueError("Missing required properties to generate correlation id")

        hazard_cluster_code = hazard_profiles.get_cluster_code(hazards)
        # This should be dynamically determined based on existing events
        eventdatestr = event_datetime.strftime("%Y%m%d")

        event_id = f"{eventdatestr}-{country_codes[0]}-{hazard_cluster_code}-{episode_number}-GCDB"  # noqa: E501
        return event_id
