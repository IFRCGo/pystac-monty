from pystac import Item

from pystac_monty.hazard_profiles import HazardProfiles

from .geo_blocks import GeoBlocks


class Pairing:
    """Generation of correlation id for Events Pairing"""

    def _return_bbox_centroid_coordinates(self, bbox: list) -> list:
        """Returns the centroid of the bbox"""
        return [round((bbox[1] + bbox[3]) / 2.0, 1), round((bbox[0] + bbox[2]) / 2.0, 1)]  # [lat, lon]

    def _construct_correlation_id_str(
        self, date: str, country_code: str, block_id: int, hazard_code: str, episode_number: int
    ) -> str:
        """Construct the correlation id"""
        return f"{date}-{country_code}-{block_id}-{hazard_code}-{episode_number}-GCDB"  # noqa: E501

    def generate_correlation_id(self, item: Item, hazard_profiles: HazardProfiles) -> str:
        """Generate the correlation ID for events pairing"""
        # Get the necessary properties for creating the correlation id
        hazards = item.properties.get("monty:hazard_codes", [])
        country_codes = item.properties.get("monty:country_codes", [])
        event_datetime = item.datetime
        episode_number = item.properties.get("monty:episode_number", 0)

        if not hazards or not country_codes or not event_datetime or not episode_number:
            raise ValueError("Missing required properties to generate correlation id")

        hazard_cluster_code = hazard_profiles.get_canonical_hazard_codes(item)[0].upper()
        # This should be dynamically determined based on existing events
        eventdatestr = event_datetime.strftime("%Y%m%d")

        if not item.bbox:
            # When bbox is None, assign block_id to 0.
            block_id = 0
            return self._construct_correlation_id_str(
                date=eventdatestr,
                country_code=country_codes[0],
                block_id=block_id,
                hazard_code=hazard_cluster_code,
                episode_number=episode_number,
            )

        geometry_lat_lon = self._return_bbox_centroid_coordinates(item.bbox)

        geoblocks_df = GeoBlocks.get_geoblocks_df()
        geoblocks_filtered_df = geoblocks_df[
            (geoblocks_df["lat_min"] <= geometry_lat_lon[0])
            & (geoblocks_df["lat_max"] > geometry_lat_lon[0])
            & (geoblocks_df["lon_min"] <= geometry_lat_lon[1])
            & (geoblocks_df["lon_max"] > geometry_lat_lon[1])
        ]
        # NOTE: When we can't determine the block id, we assign 0.
        # In parquet file, the block id starts from 1 and so on.
        block_id = int(geoblocks_filtered_df["block_id"].iloc[0]) if len(geoblocks_filtered_df) else 0
        return self._construct_correlation_id_str(
            date=eventdatestr,
            country_code=country_codes[0],
            block_id=block_id,
            hazard_code=hazard_cluster_code,
            episode_number=episode_number,
        )
