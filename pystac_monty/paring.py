from pystac import Item

from pystac_monty.hazard_profiles import HazardProfiles

from .geo_blocks import GeoBlocks


class Pairing:
    """Generation of correlation id for Events Pairing"""

    def _return_bbox_centroid_coordinates(self, bbox: list):
        """Returns the centroid of the bbox"""
        return [round((bbox[1] + bbox[3]) / 2.0, 1), round((bbox[0] + bbox[2]) / 2.0, 1)]  # [lat, lon]

    def generate_correlation_id(self, item: Item, hazard_profiles: HazardProfiles) -> str:
        """Generate the correlation ID for events pairing"""
        # Get the necessary properties for creating the correlation id
        hazards = item.properties.get("monty:hazard_codes", [])
        country_codes = item.properties.get("monty:country_codes", [])
        event_datetime = item.datetime
        episode_number = item.properties.get("monty:episode_number", 0)
        geometry_lat_lon = self._return_bbox_centroid_coordinates(item.bbox)

        if not hazards or not country_codes or not event_datetime or not episode_number or not geometry_lat_lon:
            raise ValueError("Missing required properties to generate correlation id")

        hazard_cluster_code = hazard_profiles.get_canonical_hazard_codes(item)[0].upper()
        # This should be dynamically determined based on existing events
        eventdatestr = event_datetime.strftime("%Y%m%d")

        geoblocks_df = GeoBlocks.get_geoblocks_df()
        geoblocks_filtered_df = geoblocks_df[
            (geoblocks_df["lat_min"] <= geometry_lat_lon[0])
            & (geoblocks_df["lat_max"] > geometry_lat_lon[0])
            & (geoblocks_df["lon_min"] <= geometry_lat_lon[1])
            & (geoblocks_df["lon_max"] > geometry_lat_lon[1])
        ]
        block_id = int(geoblocks_filtered_df["block_id"].iloc[0]) if len(geoblocks_filtered_df) else -1

        event_id = f"{eventdatestr}-{country_codes[0]}-{block_id}-{hazard_cluster_code}-{episode_number}-GCDB"  # noqa: E501
        return event_id
