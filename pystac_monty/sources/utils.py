import subprocess
import tempfile
from enum import Enum

from pystac_monty.extension import (
    MontyImpactExposureCategory,
    MontyImpactType,
)


class IDMCUtils:
    """IDMC GIDD and IDU utils"""

    class DisplacementType(Enum):
        """Displacement Types for GIDD and IDU sources"""

        DISASTER_TYPE = "Disaster"
        CONFLICT_TYPE = "Conflict"
        OTHER_TYPE = "Other"

    # TODO: For other types e.g. FORCED_TO_FLEE, IN_RELIEF_CAMP, DESTROYED_HOUSING,
    # PARTIALLY_DESTROYED_HOUSING, UNINHABITABLE_HOUSING, RETURNS, MULTIPLE_OR_OTHER
    # Handle them later.
    """All Impact Mappings for GIDD and IDU sources"""
    mappings = {
        "evacuated": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.EVACUATED),
        "displaced": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.INTERNALLY_DISPLACED_PERSONS),
        "relocated": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.RELOCATED),
        "sheltered": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.EMERGENCY_SHELTERED),
        "homeless": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.HOMELESS),
        "affected": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.TOTAL_AFFECTED),
        "IDPs": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.INTERNALLY_DISPLACED_PERSONS),
        "Internal Displacements": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.INTERNALLY_DISPLACED_PERSONS),
        "Deaths": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.DEATH),
        "People displaced across borders": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.EXTERNALLY_DISPLACED_PERSONS),
    }

    """Utils for IDMC GIDD and IDU"""

    @staticmethod
    def hazard_codes_mapping(hazard: tuple) -> list[str] | None:
        """Map IDU hazards to UNDRR-ISC 2020 Hazard Codes"""
        hazard = tuple((item.lower() if item else item for item in hazard))
        hazard_mapping = {
            ("geophysical", "geophysical", "earthquake", "earthquake"): ["nat-geo-ear-gro"],
            ("geophysical", "geophysical", "earthquake", "tsunami"): ["nat-geo-ear-tsu"],
            ("geophysical", "geophysical", "mass movement", "dry mass movement"): ["nat-geo-mmd-lan"],
            ("geophysical", "geophysical", "mass movement", "sinkhole"): ["nat-geo-mmd-sub"],
            ("geophysical", "geophysical", "volcanic activity", "volcanic activity"): ["nat-geo-vol-vol"],
            ("mixed disasters", "mixed disasters", "mixed disasters", "mixed disasters"): ["mix-mix-mix-mix"],
            ("weather related", "climatological", "desertification", "desertification"): ["EN0006", "nat-geo-env-des"],
            ("weather related", "climatological", "drought", "drought"): ["nat-cli-dro-dro"],
            ("weather related", "climatological", "erosion", "erosion"): ["EN0019", "nat-geo-env-soi"],
            ("weather related", "climatological", "salinisation", "salinization"): ["EN0007", "nat-geo-env-slr"],
            ("weather related", "climatological", "sea level rise", "sea level rise"): ["EN0023", "nat-geo-env-slr"],
            ("weather related", "climatological", "wildfire", "wildfire"): ["nat-cli-wil-wil"],
            ("weather related", "hydrological", "flood", "dam release flood"): ["tec-mis-col-col"],
            ("weather related", "hydrological", "flood", "flood"): ["nat-hyd-flo-flo"],
            ("weather related", "hydrological", "mass movement", "avalanche"): ["nat-hyd-mmw-ava"],
            ("weather related", "hydrological", "mass movement", "landslide/wet mass movement"): ["nat-hyd-mmw-lan"],
            ("weather related", "hydrological", "wave action", "rogue wave"): ["nat-hyd-wav-rog"],
            ("weather related", "meteorological", "extreme temperature", "cold wave"): ["nat-met-ext-col"],
            ("weather related", "meteorological", "extreme temperature", "heat wave"): ["nat-met-ext-hea"],
            ("weather related", "meteorological", "storm", "hailstorm"): ["nat-met-sto-hai"],
            ("weather related", "meteorological", "storm", "sand/dust storm"): ["nat-met-sto-san"],
            ("weather related", "meteorological", "storm", "storm surge"): ["nat-met-sto-sur"],
            ("weather related", "meteorological", "storm", "storm"): ["nat-met-sto-sto"],
            ("weather related", "meteorological", "storm", "tornado"): ["nat-met-sto-tor"],
            ("weather related", "meteorological", "storm", "typhoon/hurricane/cyclone"): ["nat-met-sto-tro"],
            ("weather related", "meteorological", "storm", "winter storm/blizzard"): ["nat-met-sto-bli"],
        }
        if hazard not in hazard_mapping:
            raise KeyError(f"Hazard {hazard} not found.")
        return hazard_mapping.get(hazard)


def order_data_file(filepath: str):
    """Order the data based on event_id"""
    result = subprocess.run(["jq", "sort_by(.event_id)", filepath], capture_output=True, text=True, check=True)

    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.write(result.stdout.encode())
    temp_file.close()

    return temp_file
