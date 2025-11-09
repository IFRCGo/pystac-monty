import json
import re
import subprocess
import tempfile
from enum import Enum

from pystac_monty.extension import (
    MontyImpactExposureCategory,
    MontyImpactType,
)


def phrase_to_dashed(phrase: str) -> str:
    return re.sub(r"[^\w]+", "-", phrase).strip("-").lower()


def save_json_data_into_tmp_file(data: dict) -> tempfile._TemporaryFileWrapper:
    tmpfile = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
    data = json.dumps(data).encode("utf-8")
    tmpfile.write(data)
    tmpfile.close()
    return tmpfile


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
    def hazard_codes_mapping(hazard: tuple) -> list[str]:
        """Map IDU hazards to UNDRR-ISC 2020 Hazard Codes"""
        hazard = tuple((item.lower() if item else item for item in hazard))
        hazard_mapping = {
            ("geophysical", "geophysical", "earthquake", "earthquake"): ["GH0101", "nat-geo-ear-gro", "EQ"],
            ("geophysical", "geophysical", "earthquake", "tsunami"): ["MH0705", "nat-geo-ear-tsu", "TS"],
            ("geophysical", "geophysical", "mass movement", "dry mass movement"): ["GH0300", "nat-geo-mmd-lan", "LS"],
            ("geophysical", "geophysical", "mass movement", "sinkhole"): ["GH0308", "nat-geo-mmd-sub", "OT"],
            ("geophysical", "geophysical", "volcanic activity", "volcanic activity"): ["GH0205", "nat-geo-vol-vol", "VO"],
            ("mixed disasters", "mixed disasters", "mixed disasters", "mixed disasters"): ["mix-mix-mix-mix"],
            ("weather related", "climatological", "desertification", "desertification"): ["EN0206", "nat-geo-env-des", "OT"],
            ("weather related", "climatological", "drought", "drought"): ["MH0401", "nat-cli-dro-dro", "DR"],
            ("weather related", "climatological", "erosion", "erosion"): ["GH0403", "nat-geo-env-soi", "OT"],
            ("weather related", "climatological", "salinisation", "salinization"): ["EN0303", "nat-geo-env-slr", "OT"],
            ("weather related", "climatological", "sea level rise", "sea level rise"): ["EN0303", "nat-geo-env-slr", "OT"],
            ("weather related", "climatological", "wildfire", "wildfire"): ["EN0205", "nat-cli-wil-wil", "WF"],
            ("weather related", "hydrological", "flood", "dam release flood"): ["TL0009", "tec-mis-col-col", "FL"],
            ("weather related", "hydrological", "flood", "flood"): ["MH0600", "nat-hyd-flo-flo", "FL"],
            ("weather related", "hydrological", "mass movement", "avalanche"): ["MH0801", "nat-geo-mmd-ava", "AV"],
            ("weather related", "hydrological", "mass movement", "landslide/wet mass movement"): [
                "GH0300",
                "nat-geo-mmd-lan",
                "LS",
            ],
            ("weather related", "hydrological", "wave action", "rogue wave"): ["MH0701", "nat-hyd-wav-rog", "OT"],
            ("weather related", "meteorological", "extreme temperature", "cold wave"): ["MH0502", "nat-met-ext-col", "CW"],
            ("weather related", "meteorological", "extreme temperature", "heat wave"): ["MH0501", "nat-met-ext-hea", "HT"],
            ("weather related", "meteorological", "storm", "hailstorm"): ["MH0404", "nat-met-sto-hai", "ST"],
            ("weather related", "meteorological", "storm", "sand/dust storm"): ["MH0201", "nat-met-sto-san", "VW"],
            ("weather related", "meteorological", "storm", "storm surge"): ["MH0703", "nat-met-sto-sur", "SS"],
            ("weather related", "meteorological", "storm", "storm"): ["MH0301", "nat-met-sto-sto", "VW"],
            ("weather related", "meteorological", "storm", "tornado"): ["MH0305", "nat-met-sto-tor", "TO"],
            ("weather related", "meteorological", "storm", "typhoon/hurricane/cyclone"): ["MH0309", "nat-met-sto-tro", "TC"],
            ("weather related", "meteorological", "storm", "winter storm/blizzard"): ["MH0403", "nat-met-sto-bli", "OT"],
        }
        if hazard not in hazard_mapping:
            raise KeyError(f"Hazard {hazard} not found.")
        return hazard_mapping.get(hazard, [])


def order_data_file(filepath: str, jq_filter: str):
    """Order the data based on given filter"""
    try:
        result = subprocess.run(["jq", jq_filter, filepath], capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        print("Error running jq:", e.stderr)
        raise

    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.write(result.stdout.encode())
    temp_file.close()

    return temp_file
