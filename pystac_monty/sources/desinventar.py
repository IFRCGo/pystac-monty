import logging
import tempfile
import typing
import xml.sax
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zipfile import ZipFile

import requests
from geopandas import gpd
from pystac import Link
from pystac.item import Item

from pystac_monty.extension import ImpactDetail, MontyEstimateType, MontyExtension, MontyImpactExposureCategory, MontyImpactType
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import DesinventarDataSourceType, MontyDataSourceV3, MontyDataTransformer
from pystac_monty.validators.desinventar import (
    STAC_EVENT_ID_PREFIX,
    STAC_HAZARD_ID_PREFIX,
    STAC_IMPACT_ID_PREFIX,
    DataRow,
    GeoDataEntry,
)

logger = logging.getLogger(__name__)

T = typing.TypeVar("T")


class DesinventarXMLHandler(xml.sax.ContentHandler):
    def __init__(self):
        self.current_section = ""
        self.current_element = ""
        self.current_data = {}
        self.buffer = ""

        self.eventos = []  # List of hazard types
        self.fichas = []  # List of disaster records
        self.level_maps = []  # List of geo records

    def startElement(self, name, attrs):
        if name in ("eventos", "fichas", "level_maps"):
            self.current_section = name
        elif name == "TR":
            self.current_data = {}
        else:
            self.current_element = name
            self.buffer = ""

    def characters(self, content):
        self.buffer += content

    def endElement(self, name):
        if name == "TR":
            if self.current_section == "eventos":
                self.eventos.append(self.current_data.copy())
            elif self.current_section == "fichas":
                self.fichas.append(self.current_data.copy())
            elif self.current_section == "level_maps":
                self.level_maps.append(self.current_data.copy())

            self.current_data = {}
        elif name in ("eventos", "fichas", "level_maps"):
            self.current_section = ""
        elif self.current_element:
            self.current_data[self.current_element] = self.buffer.strip()
            self.buffer = ""
            self.current_element = ""


# TODO: move to common utils
def get_list_item_safe(lst: list[T], index: int, default_value: T | None = None) -> T | None:
    try:
        return lst[index]
    except IndexError:
        return default_value


def parse_row_data(
    event_data: dict,
    hazard_name_mapping: dict[str, str],
    iso3: str,
    data_source_url: str | None,
):
    serial = event_data["serial"]
    # FIXME: Do we handle this as failure?
    if serial is None:
        return
    evento = event_data["evento"]

    # FIXME: Do we handle this as failure?
    if evento is None:
        return

    return DataRow(
        serial=serial,
        event=hazard_name_mapping[evento],
        comment=event_data["di_comments"],
        # source=extract_value(event_data, "fuentes"),
        deaths=event_data["muertos"],
        injured=event_data["heridos"],
        missing=event_data["desaparece"],
        houses_destroyed=event_data["vivdest"],
        houses_damaged=event_data["vivafec"],
        directly_affected=event_data["damnificados"],
        indirectly_affected=event_data["afectados"],
        relocated=event_data["reubicados"],
        evacuated=event_data["evacuados"],
        losses_in_dollar=event_data["valorus"],
        losses_local_currency=event_data["valorloc"],
        # education_centers=extract_value(event_data, "nescuelas"],
        # hospitals=extract_value(event_data, "nhospitales"],
        damages_in_crops_ha=event_data["nhectareas"],
        lost_cattle=event_data["cabezas"],
        damages_in_roads_mts=event_data["kmvias"],
        level0=event_data["level0"],
        level1=event_data["level1"],
        level2=event_data["level2"],
        # name0=extract_value(event_data, "name0"],
        # name1=extract_value(event_data, "name1"],
        # name2=extract_value(event_data, "name2"],
        # latitude=extract_value(event_data, "latitude"],
        # longitude=extract_value(event_data, "longitude"],
        # haz_maxvalue=extract_value(event_data, "magnitud2"],
        # glide=extract_value(event_data, "glide"],
        location=event_data["lugar"],
        # duration=extract_value(event_data, "duracion"],
        year=event_data["fechano"],
        month=event_data["fechames"],
        day=event_data["fechadia"],
        iso3=iso3,
        data_source_url=data_source_url,
    )


hazard_name_mappings = {
    "AIR CRASH": "AIRCRAFT CRASH",
    "AIRCRAFT DISASTER/MISSES": "AIRCRAFT CRASH",
    "Air crash": "AIRCRAFT CRASH",
    "PLANE CARSH": "AIRCRAFT CRASH",
    "PLANE CRASH": "AIRCRAFT CRASH",
    "FLUJOLODO": "ALLUVION",
    "Flujo de escombros": "ALLUVION",
    "ANIMAL INCIDENT": "ANIMAL ATTACK",
    "ANIMAL-CAUSED DAMAGE": "ANIMAL ATTACK",
    "ANNIMAL ATTACK": "ANIMAL ATTACK",
    "CONFLICT MAN WILD FAUNA": "ANIMAL ATTACK",
    "Harmful Animals": "ANIMAL ATTACK",
    "RAT": "ANIMAL ATTACK",
    "RATE IN FIELD": "ANIMAL ATTACK",
    "Rodent": "ANIMAL ATTACK",
    "SHARK ATTACK": "ANIMAL ATTACK",
    "WILD ANIMAL-CAUSED DAMAGE": "ANIMAL ATTACK",
    "WILD ANIMALS": "ANIMAL ATTACK",
    "attaque animal": "ANIMAL ATTACK",
    "hippos": "ANIMAL ATTACK",
    "ANIMAL DISEASES": "ANIMAL DISEASE",
    "EPIZOOTIA": "ANIMAL DISEASE",
    "EPIZOOTIE": "ANIMAL DISEASE",
    "EPIZOOTY": "ANIMAL DISEASE",
    "Epizooty": "ANIMAL DISEASE",
    "FOWL SMALLPOX": "ANIMAL DISEASE",
    "LIVESTOCK DISEASE OUTBREAK": "ANIMAL DISEASE",
    "Avalanche": "AVALANCHE",
    "CHEMICAL SPILLS": "CHEMICAL SUBSTANCE",
    "Chemical": "CHEMICAL SUBSTANCE",
    "COASTLINE": "COASTAL EROSION",
    "Coastal Erosion": "COASTAL EROSION",
    "LITORAL": "COASTAL EROSION",
    "SEAEROSION": "COASTAL EROSION",
    "SEDIMENTATION": "COASTAL EROSION",
    "coastal erosion": "COASTAL EROSION",
    "COLD": "COLD WAVE",
    "CW - Cold Wave": "COLD WAVE",
    "Cold Wave": "COLD WAVE",
    "OLA DE FRIO": "COLD WAVE",
    "Ola de frío": "COLD WAVE",
    "Onda fría": "COLD WAVE",
    "Severe colds": "COLD WAVE",
    "vague de froid": "COLD WAVE",
    "ARMED CONFLICT": "CONFLICT",
    "CE - Complex Emergency": "CONFLICT",
    "Civil strife and Police arson": "CONFLICT",
    "Conflict": "CONFLICT",
    "KONFLITU": "CONFLICT",
    "LANDED CONFLICT": "CONFLICT",
    "SOCIAL CONFLICTS": "CONFLICT",
    "SOCIAL UNREST": "CONFLICT",
    "TERROR ATTACK": "CONFLICT",
    "TERRORISM": "CONFLICT",
    "TERRORIST ATTACK": "CONFLICT",
    "WAR": "CONFLICT",
    "INDUSTRIAL POLLUTION": "CONTAMINATION",
    "OZONO": "CONTAMINATION",
    "POLLUTION MARINE": "CONTAMINATION",
    "CYCLONES": "CYCLONE",
    "Cyclone": "CYCLONE",
    "Cyclone wind": "CYCLONE",
    "GONU CYCLONE": "CYCLONE",
    "HURRICANE": "CYCLONE",
    "Hailstorm & Cyclone winds": "CYCLONE",
    "Hailstorm and cyclone": "CYCLONE",
    "Hurricane": "CYCLONE",
    "Ouragan": "CYCLONE",
    "TC - Tropical Cyclone": "CYCLONE",
    "TEMPU BAILORON": "CYCLONE",
    "TROP. DEPRESS.": "CYCLONE",
    "TROPICAL CYCLONE": "CYCLONE",
    "Tormenta Tropical": "CYCLONE",
    "Typhoon": "CYCLONE",
    "CHOLERA": "Cholera",
    "BUILDING FIRE": "DOMESTIC FIRE",
    "COMMERCIAL FIRE": "DOMESTIC FIRE",
    "Domestic Fire": "DOMESTIC FIRE",
    "FR - Fire": "DOMESTIC FIRE",
    "Fire": "DOMESTIC FIRE",
    "Fire Out break": "DOMESTIC FIRE",
    "Fire Outbreak": "DOMESTIC FIRE",
    "Fire Outbreak(shop)": "DOMESTIC FIRE",
    "HOUSE FIRE ACCIDENT": "DOMESTIC FIRE",
    "INCENDIO": "DOMESTIC FIRE",
    "INDUSTRIAL FIRE": "DOMESTIC FIRE",
    "INSTITUTIONAL FIRE": "DOMESTIC FIRE",
    "Incendio urbano": "DOMESTIC FIRE",
    "URBAN / RURAL FIRE": "DOMESTIC FIRE",
    "URBAN FIRE": "DOMESTIC FIRE",
    "DR - Drought": "DROUGHT",
    "DROUGTH": "DROUGHT",
    "DRY SPELL": "DROUGHT",
    "Draught": "DROUGHT",
    "Drought": "DROUGHT",
    "Dry Spells": "DROUGHT",
    "Sécheresse": "DROUGHT",
    "AHOGAMIENTO": "DROWNING",
    "BOAT CAPSIZED": "DROWNING",
    "BOAT SINKING": "DROWNING",
    "BOATS SANKING": "DROWNING",
    "Boat accident": "DROWNING",
    "Corriente de resaca": "DROWNING",
    "DROWN": "DROWNING",
    "Drowning": "DROWNING",
    "INTERVENTION EN MER": "DROWNING",
    "LAKE/RIVER ACCIDENT": "DROWNING",
    "MARINE ACCIDENT": "DROWNING",
    "Maritime disaster": "DROWNING",
    "Muerte por Inmersión": "DROWNING",
    "NAUFRAGIO": "DROWNING",
    "NOYADE": "DROWNING",
    "Naufrage": "DROWNING",
    "Naufragio": "DROWNING",
    "Rough Seas": "DROWNING",
    "SEA LOST": "DROWNING",
    "SHIP ACCIDENT": "DROWNING",
    "SHIPS  ACCEDNTS": "DROWNING",
    "SINKING": "DROWNING",
    "WATER ACCIDENT": "DROWNING",
    "EARTH TREMORS": "EARTHQUAKE",
    "EARTHQUAKE AND TSUNAMI": "EARTHQUAKE",
    "EQ - Earthquake": "EARTHQUAKE",
    "Earthquake": "EARTHQUAKE",
    "GROUND VIBRATIO": "EARTHQUAKE",
    "Tremblement de terre": "EARTHQUAKE",
    "THUNDER STORM": "THUNDERSTORM",
    "Thunder": "THUNDERSTORM",
    "Thunder Storm": "THUNDERSTORM",
    "Thunderstorm": "THUNDERSTORM",
    "Tondering": "THUNDERSTORM",
    "Electrocution": "ELECTROCUTION",
    "Erosion": "EROSION",
    "Erosión": "EROSION",
    "Erossion": "EROSION",
    "EXPLOSIÓN": "EXPLOSIONS",
    "Explosion": "EXPLOSIONS",
    "Explosions": "EXPLOSIONS",
    "Explotion": "EXPLOSIONS",
    "Industrial explosions": "EXPLOSIONS",
    "BIOLOGICAL": "EPIDEMIC",
    "DISEASE EPIDEMICS": "EPIDEMIC",
    "Disease": "EPIDEMIC",
    "EP - Epidemic": "EPIDEMIC",
    "EPIDEMIC COVID-19 Gen Records": "EPIDEMIC",
    "EPIDEMIC/PANDEMIC": "EPIDEMIC",
    "EPIDEMICS": "EPIDEMIC",
    "Epidemics": "EPIDEMIC",
    "HUMAN DISEASE": "EPIDEMIC",
    "KONZO DISEASE": "EPIDEMIC",
    "PANDEMIC": "EPIDEMIC",
    "FLASH FLOODS": "FLASH FLOOD",
    "Falshflood": "FLASH FLOOD",
    "Flash Flood": "FLASH FLOOD",
    "Flash Floods": "FLASH FLOOD",
    "Flash Food": "FLASH FLOOD",
    "Flash Foods": "FLASH FLOOD",
    "Flash flood": "FLASH FLOOD",
    "Flashflood": "FLASH FLOOD",
    "Flashfoods": "FLASH FLOOD",
    "Raudal": "FLASH FLOOD",
    "SPATE": "FLASH FLOOD",
    "Torrent": "FLASH FLOOD",
    "CYCLONE & FLOOD": "FLOOD",
    "Cyclone  and floods": "FLOOD",
    "Desbordamiento": "FLOOD",
    "FL - Flood": "FLOOD",
    "FLOODS": "FLOOD",
    "Flood": "FLOOD",
    "Flooding": "FLOOD",
    "Flooding of Bua river": "FLOOD",
    "Floods": "FLOOD",
    "Flsah Flood": "FLOOD",
    "Heavy rains, storm and floods": "FLOOD",
    "INUNDACIÓN": "FLOOD",
    "INUNDASAUN": "FLOOD",
    "Inondation": "FLOOD",
    "Inundación": "FLOOD",
    "Inundación gradual": "FLOOD",
    "Inundación repentina": "FLOOD",
    "Stormy Rains & Floods": "FLOOD",
    "fash floods": "FLOOD",
    "floods": "FLOOD",
    "Fog": "FOG",
    "AHI HAN": "FOREST FIRE",
    "AHI HAN RAI": "FOREST FIRE",
    "BUSH FIRE": "FOREST FIRE",
    "Bush fire": "FOREST FIRE",
    "FEUX DE BROUSSES": "FOREST FIRE",
    "FIRE HAZARDS": "FOREST FIRE",
    "FOREST FIRE  / WILD FIRE": "FOREST FIRE",
    "FORESTFIRE": "FOREST FIRE",
    "Feux": "FOREST FIRE",
    "Feux de Forêts": "FOREST FIRE",
    "Forest Fire": "FOREST FIRE",
    "Forest fire": "FOREST FIRE",
    "Incendio de campo": "FOREST FIRE",
    "Incendio de interfase": "FOREST FIRE",
    "Incendios forestales": "FOREST FIRE",
    "Strong winds and bush fire": "FOREST FIRE",
    "WF - Wild Fires": "FOREST FIRE",
    "WILD FIRE": "FOREST FIRE",
    "Wild fire": "FOREST FIRE",
    "ХЭЭРИЙН ТҮЙМЭР": "FOREST FIRE",
    "Frost": "FROST",
    "Storm/Gale": "GALE",
    "MURDER": "GUNSHOT",
    "HAIL": "HAIL STORM",
    "HAILSTORM": "HAIL STORM",
    "Hail": "HAIL STORM",
    "Hail storm": "HAIL STORM",
    "Hailstorm": "HAIL STORM",
    "Hailstorms": "HAIL STORM",
    "Heavy Hailstorms and Rains": "HAIL STORM",
    "HEATWAVE": "HEAT WAVE",
    "vague de chaleur": "HEAT WAVE",
    "CLOUD BURST": "HEAVY RAINS",
    "Cyclone and Rainstorm": "HEAVY RAINS",
    "HEAVY RAIN": "HEAVY RAINS",
    "Hailstorm and heavy rains": "HEAVY RAINS",
    "Heavy Rain": "HEAVY RAINS",
    "Heavy Rain With Lightnings": "HEAVY RAINS",
    "Heavy Rains?": "HEAVY RAINS",
    "Heavy rain": "HEAVY RAINS",
    "Heavy rain storm": "HEAVY RAINS",
    "Heavy rains": "HEAVY RAINS",
    "Heavy rains and hailstorm": "HEAVY RAINS",
    "Heavy rains and storm": "HEAVY RAINS",
    "Heavy rains and storms": "HEAVY RAINS",
    "Heavy rains and winds": "HEAVY RAINS",
    "Heavy rains, winds, Hailstorm": "HEAVY RAINS",
    "Heavy rainstorm": "HEAVY RAINS",
    "PLUIES EXTREME": "HEAVY RAINS",
    "RAIN OUT OF SAISON": "HEAVY RAINS",
    "RAINSTORM": "HEAVY RAINS",
    "Rain": "HEAVY RAINS",
    "Rain Storm": "HEAVY RAINS",
    "Rainstorm": "HEAVY RAINS",
    "Rainstorms": "HEAVY RAINS",
    "STRONG RAIN": "HEAVY RAINS",
    "Storm and heavy rains": "HEAVY RAINS",
    "Stormy Rains": "HEAVY RAINS",
    "Stormy rains and Hailstorm": "HEAVY RAINS",
    "Strongs winds and heavy rains": "HEAVY RAINS",
    "TORRENTIAL RAINS": "HEAVY RAINS",
    "LAND SUBSIDENCE": "Hundimiento",
    "Subsidence": "Hundimiento",
    "Food Poisoning": "INTOXICACION",
    "INTOXICACIÓN": "INTOXICACION",
    "INTOXICATION": "INTOXICACION",
    "Intoxicación": "INTOXICACION",
    "poisoning": "INTOXICACION",
    "Lahar": "LAHAR",
    "BUILDING SLIDE": "LANDSLIDE",
    "CUTTING FAILURE": "LANDSLIDE",
    "DESLIZAMIENTO": "LANDSLIDE",
    "EARTH SLIP": "LANDSLIDE",
    "LAND SLIDE": "LANDSLIDE",
    "LANDSLIDES": "LANDSLIDE",
    "LS - Landslide": "LANDSLIDE",
    "Landslide": "LANDSLIDE",
    "MUDSLIDE": "LANDSLIDE",
    "Mudslide": "LANDSLIDE",
    "RETAINING WF": "LANDSLIDE",
    "ROCK/BOULDER SLIDE": "LANDSLIDE",
    "Rockslide": "LANDSLIDE",
    "Emission/Leak": "LEAK OR SPILL",
    "OIL SPILL": "LEAK OR SPILL",
    "OIL SPILLS": "LEAK OR SPILL",
    "OIL/HNS Spill": "LEAK OR SPILL",
    "Spill": "LEAK OR SPILL",
    "ELECTRICSTORM": "LIGHTNING",
    "ELECTRIQUE STORM": "LIGHTNING",
    "LIGHTENING": "LIGHTNING",
    "LIGHTING": "LIGHTNING",
    "LIGHTNING STRIKE": "LIGHTNING",
    "Lightening": "LIGHTNING",
    "Lightning": "LIGHTNING",
    "THUNDER LIGHTING STROKE": "LIGHTNING",
    "foudre": "LIGHTNING",
    "АЯНГА": "LIGHTNING",
    "DESERT LOCUST": "LOCUST CRISIS",
    "LOCUST": "LOCUST CRISIS",
    "RAVAGERS AND LOCUSTS PILGRIMS": "LOCUST CRISIS",
    "Measles": "MEASLE",
    "ANIMALS/ INSECTS": "PEST",
    "Army worms": "PEST",
    "Armyworm": "PEST",
    "BAD GRASS INVASION": "PEST",
    "BEETLE": "PEST",
    "BIRD INFESTATION": "PEST",
    "CROP PESTS AND DISEASE": "PEST",
    "Chenille": "PEST",
    "DESTRUCTION BY INSECTS": "PEST",
    "DETECTED SLUG": "PEST",
    "FOLIAR DISEASE": "PEST",
    "GRANIVORE BIRDS": "PEST",
    "GRANIVOROUS BIRDS": "PEST",
    "INSECT INFESTATION": "PEST",
    "Mealy bug": "PEST",
    "PEST AND INSECT INFESTATION": "PEST",
    "PEST INFESTATION": "PEST",
    "PEST OUTBREAK": "PEST",
    "PLANT DISEASE": "PEST",
    "RICE YELLOW MOTTLE VIRUS": "PEST",
    "VERMIN INFESTATION": "PEST",
    "YELLOW RUST": "PEST",
    "fungal toxins": "PEST",
    "RIVERINE FLOOD": "RIVER FLOOD",
    "River Flood": "RIVER FLOOD",
    "Riverine flood": "RIVER FLOOD",
    "RiverineFlood": "RIVER FLOOD",
    "RIVER BANK COLLAPSE": "RIVERBANK EROSION",
    "A. TRANSITO": "ROAD ACCIDENT",
    "AC - Accident": "ROAD ACCIDENT",
    "ACCIDENT TRAFFIC": "ROAD ACCIDENT",
    "ACCIDENTE": "ROAD ACCIDENT",
    "AKSIDENTE": "ROAD ACCIDENT",
    "AVIATION ACCIDENT": "ROAD ACCIDENT",
    "Accident": "ROAD ACCIDENT",
    "Accidente de transporte": "ROAD ACCIDENT",
    "Aviation accident": "ROAD ACCIDENT",
    "MINE ACCIDENT": "ROAD ACCIDENT",
    "MOTOR VEHICLE ACCIDENT": "ROAD ACCIDENT",
    "Major Accident": "ROAD ACCIDENT",
    "Major Accidents": "ROAD ACCIDENT",
    "NAVIGATION ACCIDENT": "ROAD ACCIDENT",
    "ROAD ACCIDENTS": "ROAD ACCIDENT",
    "ROAD TRAFFIC ACCIDENT": "ROAD ACCIDENT",
    "Road Accident": "ROAD ACCIDENT",
    "Siniestros de tránsito": "ROAD ACCIDENT",
    "Slippery Road": "ROAD ACCIDENT",
    "TRAFFIC ACCIDENT": "ROAD ACCIDENT",
    "TRAFIC ACCIDENT": "ROAD ACCIDENT",
    "TRAIN ACCIDENT": "ROAD ACCIDENT",
    "TRANSPORTATION ACCIDENT": "ROAD ACCIDENT",
    "Traffic accident": "ROAD ACCIDENT",
    "Train Accident": "ROAD ACCIDENT",
    "Train accident": "ROAD ACCIDENT",
    "Train/Bus accident": "ROAD ACCIDENT",
    "ROCK COLLAPSE": "ROCK FALL",
    "Rock Fall": "ROCK FALL",
    "SAND STORM": "SANDSTORM",
    "Sand Storm": "SANDSTORM",
    "Sandstorm": "SANDSTORM",
    "SNOWSTORM": "SNOW STORM",
    "Snow Storm": "SNOW STORM",
    "Snowstorm": "SNOW STORM",
    "HEAVY RAINFALL & STRONG WINDS": "STRONG WIND",
    "Heavy Storm": "STRONG WIND",
    "Heavy Wind": "STRONG WIND",
    "Heavy rains and strong winds": "STRONG WIND",
    "Heavy storms": "STRONG WIND",
    "Heavy winds": "STRONG WIND",
    "Heavy winds and rain": "STRONG WIND",
    "Heavy winds and rains": "STRONG WIND",
    "High Winds": "STRONG WIND",
    "RAIN AND STRONG WIND": "STRONG WIND",
    "Rains, Heavy winds": "STRONG WIND",
    "SS - Storm Surge": "STRONG WIND",
    "ST - Severe Local Storm": "STRONG WIND",
    "STRONG STORM": "STRONG WIND",
    "STRONG WINDS": "STRONG WIND",
    "STRONGWIND": "STRONG WIND",
    "Storm": "STRONG WIND",
    "Storm wind": "STRONG WIND",
    "Storms": "STRONG WIND",
    "Strong winds": "STRONG WIND",
    "Strong winds and heavy rains": "STRONG WIND",
    "Strong winds/Hailstrom": "STRONG WIND",
    "WIND": "STRONG WIND",
    "WINDSTORM": "STRONG WIND",
    "Wind Storm": "STRONG WIND",
    "Wind Strom": "STRONG WIND",
    "Windstorm": "STRONG WIND",
    "Windstrom": "STRONG WIND",
    "Winstrom": "STRONG WIND",
    "Winter Storm": "STRONG WIND",
    "Wndstrom": "STRONG WIND",
    "flash flood, windstrom": "STRONG WIND",
    "widstorm": "STRONG WIND",
    "windstond": "STRONG WIND",
    "windstorm with heavy rain": "STRONG WIND",
    "windstrm": "STRONG WIND",
    "windtorm": "STRONG WIND",
    "BUILDING COLLAPSE": "STRUCTURE",
    "Building Collap": "STRUCTURE",
    "COLAPSO ESTRUCTURAL": "STRUCTURE",
    "CONSTRUCTION COLLAPSE": "STRUCTURE",
    "Collapse of Gabage Fill": "STRUCTURE",
    "Collapse of building": "STRUCTURE",
    "Collapse of buildings": "STRUCTURE",
    "DESPRENDIMIENTO DE TECHO": "STRUCTURE",
    "Desprendimiento de Techo": "STRUCTURE",
    "STRUCTURAL COLLAPSE": "STRUCTURE",
    "STRUCTURAL FIRE": "STRUCTURE",
    "Structural Collapse": "STRUCTURE",
    "WALL COLLAPSE": "STRUCTURE",
    "collapse": "STRUCTURE",
    "БАРИЛГЫН ОСОЛ": "STRUCTURE",
    "STORM SURGE": "SURGE",
    "Storm Surge": "SURGE",
    "Surge": "SURGE",
    "TIDAL SURGE": "SURGE",
    "HEAVY SNOW": "Snowfall",
    "SNOWFALL": "Snowfall",
    "Snow": "Snowfall",
    "High Waves": "TIDAL WAVES",
    "Montagne Wave": "TIDAL WAVES",
    "SWELL": "TIDAL WAVES",
    "TIDAL WAVE": "TIDAL WAVES",
    "Mini Tornado": "TORNADO",
    "Tornado": "TORNADO",
    "Tornados": "TORNADO",
    "Tromba marina": "TORNADO",
    "TRAIN DERAILMENT": "TRAIN CRASH",
    "TRAINS ACCEDNTS": "TRAIN CRASH",
    "TS - Tsunami": "TSUNAMI",
    "Windom flood": "URBAN FLOOD",
    "Mud Volcano": "VOLCANO",
    "VOLCANIC ACTIVITY": "VOLCANO",
    "ERUPTION": "VOLCANO",
    "Collapse of Mine": "MINING HAZARD",
    "MINE DISASTER": "MINING HAZARD",
    "Mining accidents": "MINING HAZARD",
    "RAINS": "RAIN",
}

hazard_mapping = {
    "ALLUVION": ["GH0303", "nat-hyd-mmw-mud", "MS"],  # Mud flow
    "AVALANCHE": ["MH0801", "nat-geo-mmd-ava", "AV"],  # Avalanche
    "ACCIDENT": ["TL0007", "tec-mis-col-col", "AC"],  # Structural Failure
    "BOAT CAPSIZE": ["TL0050", "tec-tra-wat-wat", "AC"],
    "COASTAL EROSION": ["GH0405", "nat-geo-env-sed", "OT"],  # Coastal erosion
    "COLD WAVE": ["MH0502", "nat-met-ext-col", "CW"],  # Cold wave
    "CYCLONE": ["MH0309", "nat-met-sto-tro", "TC"],  # Tropical cyclone
    "DROUGHT": ["MH0401", "nat-cli-dro-dro", "DR"],  # Drought
    "EARTHQUAKE": ["GH0101", "nat-geo-ear-gro", "EQ"],  # Earthquake
    "EPIDEMIC": ["BI0101", "nat-bio-epi-dis", "EP"],  # Epidemic
    "EPIZOOTIC": ["BI0027", "nat-bio-ani-ani"],  # Animal Diseases (Not Zoonoses)
    "EROSION": ["GH0403", "nat-geo-env-soi", "OT"],  # Soil erosion
    "EXPLOSION": ["TL0029", "tec-ind-exp-exp", "AC"],  # Explosion
    "FAMINE": None,
    "FIRE": ["EN0205", "nat-cli-wil-wil", "WF"],  # Fire
    "FLASH FLOOD": ["MH0603", "nat-hyd-flo-fla", "FF"],  # Flash flood
    "FLOOD": ["MH0600", "nat-hyd-flo-flo", "FL"],  # Flood
    "FOG": ["MH0202", "nat-met-fog-fog", "OT"],  # Fog
    "FOREST FIRE": ["EN0205", "nat-cli-wil-for", "WF"],  # Forest fire
    "FROST": ["MH0505", "nat-met-ext-sev", "OT"],  # Severe frost
    "HAIL STORM": ["MH0404", "nat-met-sto-hai", "ST"],  # Hailstorm
    "HEAT WAVE": ["MH0501", "nat-met-ext-hea", "HT"],  # Heat wave
    "LAHAR": ["GH0204", "nat-geo-vol-lah", "VO"],  # Lahar
    "LANDSLIDE": ["GH0300", "nat-geo-mmd-lan", "LS"],  # Landslide
    "LEAK": ["TL0030", "tec-ind-che-che", "AC"],  # Chemical leak
    "LIQUEFACTION": ["GH0307", "nat-geo-ear-gro", "EQ"],  # Ground liquefaction
    "OTHER": None,  # Other
    "PANIC": None,
    "RAIN": ["MH0402", "OT"],
    "SANDSTORM": ["MH0201", "nat-met-sto-san", "VW"],  # Sandstorm
    "SNOW STORM": ["MH0406", "OT"],  # Snow Storm
    "STORM": ["MH0103", "nat-met-sto-sto", "ST"],  # Thunderstorm
    "STRONG WIND": ["MH0301", "nat-met-sto-sto", "VW"],  # Strong wind
    "STRUCT.COLLAPSE": ["TL0005", "tec-mis-col-col", "AC"],  # Structural collapse
    "SUBSIDENCE": ["GH0309", "nat-geo-ear-gro"],  # Subsidence
    "SURGE": ["MH0703", "nat-met-sto-sur", "SS"],  # Storm surge
    "THUNDERSTORM": ["MH0103", "nat-met-sto-sto", "ST"],  # Thunderstorm
    "TORNADO": ["MH0305", "nat-met-sto-tor", "TO"],  # Tornado
    "TSUNAMI": ["MH0705", "nat-geo-ear-tsu", "TS"],  # Tsunami
    "VOLCANO": ["GH0205", "nat-geo-vol-vol", "VO"],  # Volcanic eruption
    "COASTAL FLOOD": ["MH0601", "nat-hyd-flo-coa", "FL"],  # Coastal flood
    "MENINGITIS": ["BI0222", "OT"],
    "PLAGUE": ["BI0228", "OT"],
    "ROAD ACCIDENT": ["TL0405", "tec-tra-roa-roa", "AC"],
    "ANIMAL ATTACK": ["BI0604", "OT"],  # Human-Wildlife Conflict
    "MEASLE": ["BI0221", "OT"],
    "POLLUTION": ["TL0302", "OT"],
    "STRUCTURE": ["TL0201", "tec-mis-col-col", "AC"],
    "ANIN BOT": None,
    "RIVERBANK EROSION": ["GH0404", "OT"],
    "DOMESTIC FIRE": ["TL0305", "tec-ind-fir-fir", "FR"],
    "DROWNING": ["TL0403", "tec-tra-wat-wat", "AC"],
    "HEAVY RAINS": ["MH0402", "OT"],
    "LIGHTNING": ["MH0102", "nat-met-sto-lig", "ST"],
    "PEST": ["BI0401", "nat-bio-inf-loc", "IN"],
    "ANIMAL DISEASE": ["BI0301", "OT"],  # Animal Diseases (Not Zoonoses)
    "LOCUST CRISIS": ["BI0402", "nat-bio-inf-loc", "IN"],
    "CONFLICT": ["SO0103", "CE"],
    "GALE": ["MH0303", "OT"],
    "INTOXICACION": ["CH0601", "OT"],
    "Caída de Arbol": None,
    "Racionamiento": ["TL0210", "OT"],
    "Hundimiento": ["GH0308", "nat-geo-mmd-sub", "OT"],
    "ELECTROCUTION": ["TL0209", "OT"],  # TODO: check this; this mapping not available
    "Cholera": ["BI0204", "OT"],  # Cholera(Human)
    "RIVER FLOOD": ["MH0604", "nat-hyd-flo-riv", "FL"],
    "TREE FALLEN": None,
    "MINING HAZARD": ["TL0307", "OT"],
    "CAÍDA DE ARBOL": None,
    "SNAKE BITE": ["BI0605", "OT"],
    "ROCK FALL": ["GH0301", "OT"],
    "Snowfall": ["MH0405", "OT"],
    "Deforestación": ["EN0201", "OT"],
    "CONTAMINATION": ["EN0103", "OT"],
    "ARBOL CAIDO": None,
    "OTHER_AC": None,
    "GUNSHOT": ["SO0301", "OT"],  # TODO Mapping not found
    "BREACH": ["TL0205", "tec-mis-col-col", "FL"],
    "URBAN FLOOD": ["MH0606", "OT"],
    "Hambruna": None,
    "TIDAL WAVES": ["MH0701", "nat-hyd-wav-rog", "OT"],
    "Búsqueda": None,
    "YELLOW FEVER": ["BI0241", "OT"],
    "DZUD": ["MH0503", "nat-met-ext-sev", "OT"],
    "Other": None,
    "EXPLOSIONS": ["TL0304", "tec-ind-exp-exp", "AC"],
    "CHEMICAL SUBSTANCE": ["CH0903", "OT"],  # Chemical Warfare Agents
    "Desaparecido (S)": None,
    "CLIMATE CHANGE": None,
    "LEAK OR SPILL": ["TL0301", "tec-ind-che-che", "AC"],
    "GAS SPILLS": ["TL0301", "tec-ind-gas-gas", "AC"],
    "PEAT": None,
    "TRAIN CRASH": ["TL0404", "tec-tra-rai-rai", "AC"],
    "Nuclear accidents": ["TL0208", "OT"],
    "PROJECTILE": None,
    "LALORAN BOOT": None,
    "Traslados": None,
    "TD - Technical Disaster": None,
    "Rescate": None,
    "LAINNYA": None,
    "MALNUTRITION": None,
    "Stuck": None,
    "MALARIA": ["BI0219", "OT"],
    "HAILSTONE": None,
    "AIRCRAFT CRASH": ["TL0401", "tec-tra-air-air", "AC"],  # Air Transportation Accident
    "Food Insecurity": None,
    "PORT AREA HAZARD": None,
    "OT - Other": None,
    "RESCATE": None,
    "LAND DEGRADATION": ["EN0301", "OT"],
    "EXTREME TEMPERATURE": None,
    "FISSURES": ["GH0311", "nat-geo-ear-gro", "EQ"],
    "BÚSQUEDA": None,
    "AFLATOXIN": ["CH0201", "OT"],
    "Mpox": ["BI0224", "OT"],
    "Nawaa": None,
    "Freezing Rain": ["MH0506", "nat-met-ext-sev", "OT"],
    "Napolo": None,
    "Stormy": None,
    "INDUSTRIAL DISASTER": None,
    "Geomedical": None,
    "Famine": None,
    "Acid rain": ["EN0105", "OT"],
    "ASPHYXIA": ["CH0400", "OT"],
    "FALL INTO A WELL": None,
    "FAILEN TREES": None,
    "SEA LEVEL RISE": ["EN0402", "OT"],
    "GLACIAL LAKE OUTBURST FLOOD": ["MH0607", "nat-cli-glo-glo", "FL"],
    "AUTHER": None,
    "incursion": None,
    "W": None,
    "Riverinflood": None,
    "birds": None,
    "Livestock": None,
    "Mal uvuljuulekh": None,
    "Road": None,
    "Malnutrition": None,
    "HIGH TIDE": None,
    "Air pollution": ["EN0102", "OT"],
    "HUNGER/FAMINE": None,
    "DROPP OFF": None,
    "WETLAND LOSS/DEGRADATION": ["EN0304", "OT"],
}


# FIXME: cleanup named temporary file
class DesinventarDataSource(MontyDataSourceV3):
    tmp_zip_file: tempfile._TemporaryFileWrapper
    country_code: str
    iso3: str

    def __init__(self, data: DesinventarDataSourceType, eoapi_url: str | None = None):
        super().__init__(root=data, eoapi_url=eoapi_url)
        self.tmp_zip_file = data.tmp_zip_file.path
        self.country_code = data.country_code
        self.iso3 = data.iso3

    @classmethod
    def from_zip_file(cls, zip_file: ZipFile, country_code: str, iso3: str):
        fp = zip_file.fp
        if fp is None:
            raise Exception("Failed to process the zip file")

        content = fp.read()
        tmp_zip_file = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
        tmp_zip_file.write(content)

        return cls(tmp_zip_file, country_code, iso3)

    @classmethod
    def from_path(cls, zip_file_path: str, country_code: str, iso3: str):
        with open(zip_file_path, "rb") as source_file:
            content = source_file.read()
            tmp_zip_file = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
            tmp_zip_file.write(content)

            return cls(tmp_zip_file, country_code, iso3)

    @classmethod
    def from_url(cls, zip_file_url: str, country_code: str, iso3: str, timeout: int = 600):
        response = requests.get(zip_file_url, timeout=timeout)
        tmp_zip_file = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
        tmp_zip_file.write(response.content)

        return cls(tmp_zip_file, country_code, iso3)

    @contextmanager
    def with_xml_file(self) -> typing.Generator[typing.IO[bytes], None, None]:
        xml_file = None
        try:
            with ZipFile(self.tmp_zip_file.name, "r") as zf_ref:
                xml_file = zf_ref.open(f"DI_export_{self.country_code}.xml")
                yield xml_file
        except Exception:
            xml_file = None
        finally:
            if xml_file:
                xml_file.close()


class DesinventarTransformer(MontyDataTransformer[DesinventarDataSource]):
    """Transform DesInventar data to STAC items"""

    source_name = "desinventar"

    data_source: DesinventarDataSource
    hazard_profiles = MontyHazardProfiles()
    geo_data_mapping: Dict[str, GeoDataEntry] = {}
    geo_data_cache: Dict[str, Tuple[Dict[str, Any], List[float]]] = {}
    errored_events: Dict[str, int] = {}

    def parse_with_sax(self, xml_file: typing.Generator[typing.IO[bytes], None, None]):
        """XML parsing handler"""
        handler = DesinventarXMLHandler()
        xml.sax.parse(xml_file, handler)
        return handler

    def _create_event_item_from_row(self, row: DataRow) -> Item | None:
        # FIXME: Do we treat this as error or noise
        if not row.event_start_date:
            return None

        # FIXME: Do we treat this as error or noise
        if row.event is None:
            return None

        # FIXME: Do we treat this as error or noise
        canonical_event = hazard_name_mappings.get(row.event, row.event)
        if (hazard_codes := hazard_mapping.get(canonical_event)) is None:
            return None

        geojson, bbox = self._get_geojson_and_bbox_from_row(row)
        geojson_features = geojson.get("features", None) if geojson is not None else None

        geometry: dict[str, Any] | None = None
        if geojson_features is not None and len(geojson_features) > 0:
            geometry = geojson_features[0].get("geometry", None)
            # TODO: investigate if properties can be added to keywords
            # properties = geojson_features[0].get('properties', None)

        # Fall back to the geometry of the country currently being processed
        # when the source has no admin-level geometry for this row.
        if geometry is None and self.geocoder:
            geom_data = self.geocoder.get_geometry_from_iso3(self.data_source.iso3, simplified=True)
            if geom_data:
                geometry = geom_data["geometry"]
                bbox = geom_data["bbox"]

        if not geometry:
            logger.warning(f"Geometry could not be determined for {row.event_stac_id}. Skipping.")
            return None

        item = Item(
            id=row.event_stac_id,
            geometry=geometry,
            bbox=bbox,
            datetime=row.event_start_date,
            start_datetime=row.event_start_date,
            # FIXME: calculate end date
            end_datetime=row.event_start_date,
            properties={"title": row.event_title, "description": row.event_description},
        )

        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)

        monty.src_event_id = f"{row.iso3}-{row.serial}"
        monty.episode_number = 1  # Desinventar doesn't have episodes
        monty.hazard_codes = hazard_codes

        monty.hazard_codes = self.hazard_profiles.get_canonical_hazard_codes(item=item)

        monty.country_codes = [row.iso3.upper()]

        hazard_keywords = self.hazard_profiles.get_keywords(monty.hazard_codes)
        item.properties["keywords"] = list(set(hazard_keywords + monty.country_codes))

        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]
        # Add source link
        if row.data_source_url:
            item.add_link(
                Link(
                    "via",
                    row.data_source_url,
                    # XXX: the server does not support "zip" so using "octet-stream" for the time being.
                    "application/octet-stream",
                    f"DesInventar export zip file for {self.data_source.iso3}",
                )
            )

        return item

    # FIXME: This is not used anymore
    def _create_hazard_item_from_row(self, row: DataRow) -> Optional[Item]:
        event_item = self._create_event_item_from_row(row)

        if event_item is None:
            return None

        hazard_item = event_item.clone()
        hazard_item.id = hazard_item.id.replace(STAC_EVENT_ID_PREFIX, STAC_HAZARD_ID_PREFIX)

        # TODO: set collection and roles
        return hazard_item

    def _create_impact_item(
        self,
        base_item: Item,
        row_id: str,
        field: str,
        value: float | None,
        category: MontyImpactExposureCategory,
        impact_type: MontyImpactType,
        unit: str,
    ) -> Optional[Item]:
        """Create an impact item from a base item and a row data"""

        if value is None or value == 0:
            return None

        impact_item = base_item.clone()

        # TODO: We should make a util function for this
        impact_item.properties["title"] = f"{base_item.properties['title']} - {field}"
        impact_item.id = f"{STAC_IMPACT_ID_PREFIX}{row_id}-{field}"

        impact_item.set_collection(self.get_impact_collection())
        impact_item.properties["roles"] = ["source", "impact"]

        monty = MontyExtension.ext(impact_item)
        monty.impact_detail = ImpactDetail(
            category=category, type=impact_type, value=value, unit=unit, estimate_type=MontyEstimateType.PRIMARY
        )

        return impact_item

    def _create_impact_items_from_row(self, row: DataRow, event_item: Item) -> List[Item]:
        impact_items = [
            self._create_impact_item(
                event_item,
                row.serial,
                "deaths",
                row.deaths,
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.DEATH,
                "count",
            ),
            self._create_impact_item(
                event_item,
                row.serial,
                "injured",
                row.injured,
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.INJURED,
                "count",
            ),
            self._create_impact_item(
                event_item,
                row.serial,
                "missing",
                row.missing,
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.MISSING,
                "count",
            ),
            self._create_impact_item(
                event_item,
                row.serial,
                "houses_destroyed",
                row.houses_destroyed,
                MontyImpactExposureCategory.BUILDINGS,
                MontyImpactType.DESTROYED,
                "count",
            ),
            self._create_impact_item(
                event_item,
                row.serial,
                "houses_damaged",
                row.houses_damaged,
                MontyImpactExposureCategory.BUILDINGS,
                MontyImpactType.DAMAGED,
                "count",
            ),
            self._create_impact_item(
                event_item,
                row.serial,
                "directly_affected",
                row.directly_affected,
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.DIRECTLY_AFFECTED,
                "count",
            ),
            self._create_impact_item(
                event_item,
                row.serial,
                "indirectly_affected",
                row.indirectly_affected,
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.INDIRECTLY_AFFECTED,
                "count",
            ),
            self._create_impact_item(
                event_item,
                row.serial,
                "relocated",
                row.relocated,
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.RELOCATED,
                "count",
            ),
            self._create_impact_item(
                event_item,
                row.serial,
                "evacuated",
                row.evacuated,
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.EVACUATED,
                "count",
            ),
            self._create_impact_item(
                event_item,
                row.serial,
                "losses_in_dollar",
                row.losses_in_dollar,
                MontyImpactExposureCategory.USD_UNSURE,
                MontyImpactType.LOSS_COST,
                "USD",
            ),
            self._create_impact_item(
                event_item,
                row.serial,
                "losses_local_currency",
                row.losses_local_currency,
                MontyImpactExposureCategory.LOCAL_CURRENCY,
                MontyImpactType.LOSS_COST,
                "Unknown",
            ),
            # TODO: verify what the value represents and probaly move to response
            # self.create_impact_item(
            #     event_item,
            #     row.serial,
            #     'education_centers',
            #     row.education_centers,
            #     MontyImpactExposureCategory.EDUCATION_CENTERS,
            #     MontyImpactType.UNDEFINED,
            #     'count'
            # ),
            # self.create_impact_item(
            #     event_item,
            #     row.serial,
            #     'hospitals',
            #     row.hospitals,
            #     MontyImpactExposureCategory.HOSPITALS,
            #     MontyImpactType.UNDEFINED,
            #     'count'
            # ),
            self._create_impact_item(
                event_item,
                row.serial,
                "damages_in_crops_ha",
                row.damages_in_crops_ha,
                MontyImpactExposureCategory.CROPS,
                MontyImpactType.DAMAGED,
                "hectare",
            ),
            self._create_impact_item(
                event_item,
                row.serial,
                "lost_cattle",
                row.lost_cattle,
                MontyImpactExposureCategory.CATTLE,
                MontyImpactType.MISSING,
                "count",
            ),
            self._create_impact_item(
                event_item,
                row.serial,
                "damages_in_roads_mts",
                row.damages_in_roads_mts,
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.DAMAGED,
                "m",
            ),
        ]

        return [item for item in impact_items if item is not None]

    def _get_geojson_and_bbox_from_row(self, row: DataRow) -> Tuple[Dict[str, Any] | None, List[float] | None]:
        level = row.lowest_level

        if level is None:
            return (None, None)

        code = self.geo_data_mapping[level]["property_code"]
        if code is None:
            return (None, None)

        cached_data = self.geo_data_cache.get(f"{level}:{getattr(row, level)}", None)
        if cached_data is not None:
            return cached_data

        gfd = self.geo_data_mapping[level]["shapefile_data"]
        if gfd is None:
            return (None, None)

        try:
            filtered_gfd = gfd[gfd[code] == getattr(row, level)].copy()
        except KeyError:
            return (None, None)
        if isinstance(filtered_gfd, gpd.GeoDataFrame):
            # Use a tolerance value for simplification (smaller values will keep more detail)
            filtered_gfd["geometry"] = filtered_gfd["geometry"].apply(
                lambda geom: geom.simplify(tolerance=0.01, preserve_topology=True)
            )

            geojson = filtered_gfd.to_geo_dict()
            bbox = typing.cast(
                List[float],
                filtered_gfd.total_bounds.tolist(),
            )

            response = (geojson, bbox)
            self.geo_data_cache[f"{level}:{code}"] = response

            return response

        return (None, None)

    def _generate_geo_data_mapping(self, level_maps: list) -> Dict[str, GeoDataEntry]:
        geo_data: Dict[str, GeoDataEntry] = {}
        for level_row in level_maps:
            file_path = level_row["filename"]
            level = level_row["map_level"]
            property_code = level_row["lev_code"]

            if file_path is not None:
                shp_file_name = Path(str(file_path)).name
                shapefile_data = typing.cast(
                    gpd.GeoDataFrame,
                    gpd.read_file(f"zip://{self.data_source.tmp_zip_file.name}!{shp_file_name}"),
                )
            else:
                shapefile_data = None
            geo_data[f"level{level}"] = {
                "level": str(level) if level is not None else None,
                "property_code": str(property_code) if property_code is not None else None,
                "shapefile_data": shapefile_data,
            }
        return geo_data

    @staticmethod
    def _generate_hazard_name_mapping(hazard: dict):
        hazard_name_mapping: Dict[str, str] = {}

        for hazard_detail in hazard:
            key = hazard_detail["nombre"]
            value = hazard_detail["nombre_en"]

            hazard_name_mapping[str(key)] = str(value)

        return hazard_name_mapping

    def get_stac_items(self) -> typing.Generator[Item, None, None]:
        # TODO: Use sax xml parser for memory efficient usage
        with self.data_source.with_xml_file() as xml_file:
            rows = self.parse_with_sax(xml_file)

            self.geo_data_mapping = self._generate_geo_data_mapping(rows.level_maps)

            hazard_name_mapping = self._generate_hazard_name_mapping(rows.eventos)

            events = rows.fichas

            self.transform_summary.mark_as_started()
            for event_row in events:
                self.transform_summary.increment_rows()
                try:
                    if row_data := parse_row_data(
                        event_row,
                        hazard_name_mapping,
                        self.data_source.iso3,
                        self.data_source.source_url,
                    ):
                        if event_item := self._create_event_item_from_row(row_data):
                            impact_items = self._create_impact_items_from_row(row_data, event_item)

                            all_items = [event_item] + impact_items
                            self.set_item_hrefs(items=all_items, eoapi_url=self.data_source.eoapi_url)
                            self.add_related_links(event_item=event_item, impact_items=impact_items)

                            yield event_item
                            yield from impact_items
                        else:
                            self.transform_summary.increment_failed_rows()
                except Exception:
                    self.transform_summary.increment_failed_rows()
                    logger.warning("Failed to process DesInventar data", exc_info=True)
            self.transform_summary.mark_as_complete()

    # FIXME: This is deprecated
    def make_items(self):
        return list(self.get_stac_items())
