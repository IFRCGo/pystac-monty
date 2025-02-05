import json
import re
import tempfile
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TypedDict
from zipfile import ZipFile

import pytz
import requests
from geopandas import gpd
from lxml import etree
from pystac import Link
from pystac.item import Item

from pystac_monty.extension import (
    ImpactDetail,
    MontyEstimateType,
    MontyExtension,
    MontyImpactExposureCategory,
    MontyImpactType,
)
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import MontyDataTransformer

STAC_EVENT_ID_PREFIX = "desinventar-event-"
STAC_HAZARD_ID_PREFIX = "desinventar-hazard-"
STAC_IMPACT_ID_PREFIX = "desinventar-impact-"


# TODO: move to common utils
def get_list_item_safe(list, index, default_value=None):
    try:
        return list[index]
    except IndexError:
        return default_value


class GeoDataEntry(TypedDict):
    level: Optional[str]
    property_code: Optional[str]
    shapefile_data: Optional[gpd.GeoDataFrame]


class DataRow(TypedDict):
    # Properties extracted from desinventar

    serial: str
    comment: Optional[str]
    source: Optional[str]

    deaths: Optional[str]
    injured: Optional[str]
    missing: Optional[str]
    houses_destroyed: Optional[str]
    houses_damaged: Optional[str]
    directly_affected: Optional[str]
    indirectly_affected: Optional[str]
    relocated: Optional[str]
    evacuated: Optional[str]
    losses_in_dollar: Optional[str]
    losses_local_currency: Optional[str]
    education_centers: Optional[str]
    hospitals: Optional[str]
    damages_in_crops_ha: Optional[str]
    lost_cattle: Optional[str]
    damages_in_roads_mts: Optional[str]

    level0: Optional[str]
    level1: Optional[str]
    level2: Optional[str]
    name0: Optional[str]
    name1: Optional[str]
    name2: Optional[str]
    latitude: Optional[str]
    longitude: Optional[str]

    haz_maxvalue: Optional[str]
    event: Optional[str]
    glide: Optional[str]
    location: Optional[str]

    duration: Optional[str]
    year: Optional[str]
    month: Optional[str]
    day: Optional[str]


# TODO: complete this mapping
hazard_mapping = {
    "ALLUVION": ["MH0051", "nat-hyd-mmw-mud"],  # Mud flow
    "AVALANCHE": ["MH0050", "nat-hyd-mmw-ava"],  # Avalanche
    "ACCIDENT": ["tec-mis-col-col"],
    "BIOLOGICAL": ["nat-bio-epi-dis"],  # Epidemic
    "BOAT CAPSIZE": ["tec-tra-wat-wat", "TL0050"],
    "COASTAL EROSION": ["EN0020", "nat-geo-env-coa "],  # Coastal erosion
    "COLD WAVE": ["MH0049", "nat-met-ext-col"],  # Cold wave
    "CYCLONE": ["MH0057", "nat-met-tro-tro"],  # Tropical cyclone
    "DROUGHT": ["MH0035", "nat-met-dro-dro"],  # Drought
    "EARTHQUAKE": ["GH0001", "nat-geo-ear-grd"],  # Earthquake
    "ELECTRIC STORM": ["MH0002", "nat-met-sto-sto"],  # Thunderstorm
    "EPIDEMIC": ["nat-bio-epi-dis"],  # Epidemic
    "EPIZOOTIC": ["BI0027", "nat-bio-ani-ani"],  # Animal Diseases (Not Zoonoses)
    "EROSION": ["EN0019"],  # Soil erosion
    "ERUPTION": ["VO", "nat-geo-vol-vol"],  # Volcanic eruption
    "EXPLOSION": ["tec-mis-exp-exp"],  # Explosion
    "FAMINE": None,
    "FIRE": ["EN0013", "nat-cli-wil-wil"],  # Fire
    "FLASH FLOOD": ["MH0006", "nat-hyd-flo-fla"],  # Flash flood
    "FLOOD": ["nat-hyd-flo-flo"],  # Flood
    "FOG": ["MH0016", "nat-met-fog-fog"],  # Fog
    "FOREST FIRE": ["nat-cli-wil-for"],  # Forest fire
    "FROST": ["MH0043", "nat-met-ext-sev"],  # Severe frost
    "HAIL STORM": ["MH0036", "nat-met-sto-hai"],  # Hailstorm
    "HAILSTORM": ["MH0036", "nat-met-sto-hai"],  # Hailstorm
    "HEAT WAVE": ["MH0047", "nat-met-ext-hea"],  # Heat wave
    "LAHAR": ["GH0013", "nat-geo-vol-lah"],  # Lahar
    "LANDSLIDE": ["GH0007", "nat-hyd-mmw-lan"],  # Landslide
    "LEAK": ["TL0030", "tec-ind-che-che"],  # Chemical leak
    "LIQUEFACTION": ["GH0003", "nat-geo-ear-gro"],  # Ground liquefaction
    "OTHER": ["OT"],  # Other
    "PANIC": None,
    "PLAGUE": None,
    "POLLUTION": None,
    "RAIN": ["nat-met-sto-sto"],  # Storm
    "RAINS": ["nat-met-sto-sto"],  # Storm
    "SANDSTORM": ["MH0015", "nat-met-sto-san"],  # Sandstorm
    "SEDIMENTATION": ["nat-geo-env-sed"],  # Sedimentation
    "SNOW STORM": ["MH0039", "nat-met-sto-sto"],  # Snow Storm
    "SNOWSTORM": ["MH0039", "nat-met-sto-sto"],  # Snow Storm
    "STORM": ["nat-met-sto-sto"],  # Storm
    "STRONG WIND": ["MH0060", "nat-met-sto-sto"],  # Strong wind
    "STRUCT.COLLAPSE": ["TL0005", "tec-mis-col-col"],  # Structural collapse
    "SUBSIDENCE": ["GH0005", "nat-geo-mmd-sub"],  # Subsidence
    "SURGE": ["MH0027", "nat-met-sto-sur"],  # Storm surge
    "THUNDERSTORM": ["MH0003", "nat-met-sto-sto"],  # Thunderstorm
    "TORNADO": ["MH0059", "nat-met-sto-tor"],  # Tornado
    "TSUNAMI": ["MH0029", "nat-geo-ear-tsu"],  # Tsunami
    "WINDSTORM": ["MH0060", "nat-met-sto-sto"],  # Strong wind
    "Inundación": ["nat-hyd-flo-flo"],  # Flood
    "HURRICANE": ["MH0057", "nat-met-sto-tro"],  # Tropical cyclone
    "VOLCANO": ["nat-geo-vol-vol"],  # Volcanic eruption
    "COASTAL FLOOD": ["MH0004", "nat-hyd-flo-coa"],  # Coastal flood
}


def strtoi(s: str | None, default: str | int | None = None):
    if s is None:
        return default or s

    try:
        return int(s)
    except ValueError:
        return default or s


def natural_keys(text):
    return [strtoi(c) for c in re.split(r"(\d+)", text)]


def get_lowest_level(all_levels_in_order: List[str], row_data: DataRow):
    for level in all_levels_in_order:
        if row_data[level] is not None:
            return level

    return None


class DesinventarDataSource:
    tmp_zip_file: tempfile._TemporaryFileWrapper
    source_url: str
    country_code: str
    iso3: str

    def __init__(self, tmp_zip_file: tempfile._TemporaryFileWrapper, country_code: str, iso3: str, source_url: str = None):
        # self.tmp_zip_file = tempfile.NamedTemporaryFile(suffix=".zip")

        self.tmp_zip_file = tmp_zip_file
        self.country_code = country_code
        self.iso3 = iso3
        self.source_url = source_url

    @classmethod
    def from_zip_file(cls, zip_file: ZipFile, country_code: str, iso3: str):
        fp = zip_file.fp
        if fp is None:
            raise Exception("failed to process the zip file")

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
    def from_url(cls, zip_file_url: str, country_code: str, iso3: str):
        response = requests.get(zip_file_url)
        tmp_zip_file = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
        tmp_zip_file.write(response.content)

        return cls(tmp_zip_file, country_code, iso3)

    @contextmanager
    def with_xml_file(self):
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


class DesinventarTransformer(MontyDataTransformer):
    """Transform DesInventar data to STAC items"""

    data_source: DesinventarDataSource
    hazard_profiles = MontyHazardProfiles()
    hazard_name_mapping: Dict[str, str] = {}
    geo_data_mapping: Dict[str, GeoDataEntry] = {}
    geo_data_cache: Dict[str, Tuple[Dict[str, Any], List[float]]] = {}
    errored_events: Dict[str, int] = {}

    def __init__(self, data_source: DesinventarDataSource) -> None:
        super().__init__("desiventar")
        self.data_source = data_source
        self.events_collection_id = "desinventar-events"
        self.events_collection_url = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/examples/desinventar-events/desinventar-events.json"  # noqa: E501

        self.impacts_collection_id = "desinventar-impacts"
        self.impacts_collection_url = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/examples/desinventar-impacts/desinventar-impacts.json"  # noqa: E501

    def create_datetimes(self, row: DataRow) -> datetime | None:
        start_year = strtoi(row["year"], None)
        start_month = strtoi(row["month"], 1)
        start_day = strtoi(row["day"], 1)

        if start_year and start_month and start_day:
            # Note: the int cast here is safe
            try:
                start_dt = datetime(int(start_year), int(start_month), int(start_day))
                return pytz.utc.localize(start_dt)
            except Exception:
                return None

        return None

    def get_items(self) -> List[Item]:
        data_list = self.load_data()

        if data_list is None or len(data_list) == 0:
            return []

        items = [
            item
            for row in data_list
            for item in [
                self.create_event_item_from_row(row),
                # No need to create hazard items for now
                # self.create_hazard_item_from_row(row),
                *self.create_impact_items_from_row(row),
            ]
            if item is not None
        ]

        print("\nNOTE: cannot map events for:\n", json.dumps(self.errored_events, indent=2, ensure_ascii=False))
        return items

    def create_event_item_from_row(self, row: DataRow) -> Optional[Item]:
        if not row["serial"]:
            return None

        start_date = self.create_datetimes(row)

        if not start_date:
            return None

        geojson, bbox = self.get_geojson_and_bbox_from_row(row)
        geojson_features = geojson.get("features", None) if geojson is not None else None

        if geojson_features is not None and len(geojson_features) > 0:
            geometry = geojson_features[0].get("geometry", None)
            # TODO: investigate if properties can be added to keywords
            # properties = geojson_features[0].get('properties', None)
        else:
            geometry = None
            # properties = None

        item = Item(
            id=f"{STAC_EVENT_ID_PREFIX}{self.data_source.iso3}-{row['serial']}",
            geometry=geometry,
            bbox=bbox,
            datetime=start_date,
            start_datetime=start_date,
            # FIXME: calculate end date
            end_datetime=start_date,
            properties={
                "title": f"{row['event']} in {row['location']} on {start_date}",
                "description": f"{row['event']} in {row['location']}: {row['comment']}",
            },
        )

        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = 1  # Desinventar doesn't have episodes

        if row["event"] is None:
            return None

        event = row["event"]
        try:
            hazard_codes = hazard_mapping[event]
        except KeyError:
            hazard_codes = None
            count = self.errored_events.get(event, 0)
            self.errored_events[event] = count + 1

        if not hazard_codes:
            return None

        monty.hazard_codes = hazard_codes

        monty.country_codes = [self.data_source.iso3.upper()]
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        # Add source link
        item.add_link(
            Link(
                "via",
                self.data_source.source_url,
                "application/zip",
                "DesInventar export zip file for {}".format(self.data_source.iso3),
            )
        )

        return item

    def create_hazard_item_from_row(self, row: DataRow) -> Optional[Item]:
        event_item = self.create_event_item_from_row(row)

        if event_item is None:
            return None

        hazard_item = event_item.clone()
        hazard_item.id = hazard_item.id.replace(STAC_EVENT_ID_PREFIX, STAC_HAZARD_ID_PREFIX)

        # TODO: set collection and roles
        return hazard_item

    def create_impact_item(
        self,
        base_item: Item,
        row_id: str,
        field: str,
        value: str | None,
        category: MontyImpactExposureCategory,
        impact_type: MontyImpactType,
        unit: str,
    ) -> Optional[Item]:
        """Create an impact item from a base item and a row data"""

        if value is None or value == "0":
            return None

        impact_item = base_item.clone()

        # TODO: We should make a util function for this
        impact_item.properties["title"] = f"{base_item.properties['title']} - {field}"
        impact_item.id = f"{STAC_IMPACT_ID_PREFIX}{row_id}-{field}"

        impact_item.set_collection(self.get_impact_collection())
        impact_item.properties["roles"] = ["source", "impact"]

        monty = MontyExtension.ext(impact_item)
        monty.impact_detail = ImpactDetail(
            category=category, type=impact_type, value=float(value), unit=unit, estimate_type=MontyEstimateType.PRIMARY
        )

        return impact_item

    def create_impact_items_from_row(self, row: DataRow) -> List[Item]:
        event_item = self.create_event_item_from_row(row)

        if event_item is None:
            return []

        impact_items = [
            self.create_impact_item(
                event_item,
                row["serial"],
                "deaths",
                row["deaths"],
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.DEATH,
                "count",
            ),
            self.create_impact_item(
                event_item,
                row["serial"],
                "injured",
                row["injured"],
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.INJURED,
                "count",
            ),
            self.create_impact_item(
                event_item,
                row["serial"],
                "missing",
                row["missing"],
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.MISSING,
                "count",
            ),
            self.create_impact_item(
                event_item,
                row["serial"],
                "houses_destroyed",
                row["houses_destroyed"],
                MontyImpactExposureCategory.BUILDINGS,
                MontyImpactType.DESTROYED,
                "count",
            ),
            self.create_impact_item(
                event_item,
                row["serial"],
                "houses_damaged",
                row["houses_damaged"],
                MontyImpactExposureCategory.BUILDINGS,
                MontyImpactType.DAMAGED,
                "count",
            ),
            self.create_impact_item(
                event_item,
                row["serial"],
                "directly_affected",
                row["directly_affected"],
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.DIRECTLY_AFFECTED,
                "count",
            ),
            self.create_impact_item(
                event_item,
                row["serial"],
                "indirectly_affected",
                row["indirectly_affected"],
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.INDIRECTLY_AFFECTED,
                "count",
            ),
            self.create_impact_item(
                event_item,
                row["serial"],
                "relocated",
                row["relocated"],
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.RELOCATED,
                "count",
            ),
            self.create_impact_item(
                event_item,
                row["serial"],
                "evacuated",
                row["evacuated"],
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.EVACUATED,
                "count",
            ),
            self.create_impact_item(
                event_item,
                row["serial"],
                "losses_in_dollar",
                row["losses_in_dollar"],
                MontyImpactExposureCategory.USD_UNSURE,
                MontyImpactType.LOSS_COST,
                "USD",
            ),
            self.create_impact_item(
                event_item,
                row["serial"],
                "losses_local_currency",
                row["losses_local_currency"],
                MontyImpactExposureCategory.LOCAL_CURRENCY,
                MontyImpactType.LOSS_COST,
                "Unknown",
            ),
            # TODO: verify what the value represents and probaly move to response
            # self.create_impact_item(
            #     event_item,
            #     row['serial'],
            #     'education_centers',
            #     row['education_centers'],
            #     MontyImpactExposureCategory.EDUCATION_CENTERS,
            #     MontyImpactType.UNDEFINED,
            #     'count'
            # ),
            # self.create_impact_item(
            #     event_item, row['serial'],
            #     'hospitals',
            #     row['hospitals'],
            #     MontyImpactExposureCategory.HOSPITALS,
            #     MontyImpactType.UNDEFINED,
            #     'count'
            # ),
            self.create_impact_item(
                event_item,
                row["serial"],
                "damages_in_crops_ha",
                row["damages_in_crops_ha"],
                MontyImpactExposureCategory.CROPS,
                MontyImpactType.DAMAGED,
                "hectare",
            ),
            self.create_impact_item(
                event_item,
                row["serial"],
                "lost_cattle",
                row["lost_cattle"],
                MontyImpactExposureCategory.CATTLE,
                MontyImpactType.MISSING,
                "count",
            ),
            self.create_impact_item(
                event_item,
                row["serial"],
                "damages_in_roads_mts",
                row["damages_in_roads_mts"],
                MontyImpactExposureCategory.ALL_PEOPLE,
                MontyImpactType.DAMAGED,
                "m",
            ),
        ]

        return [item for item in impact_items if item is not None]

    def get_geojson_and_bbox_from_row(self, row: DataRow) -> Tuple[Dict[str, Any] | None, List[float] | None]:
        applicable_geo_levels = ["level2", "level1", "level0"]
        level = get_lowest_level(applicable_geo_levels, row)

        if level is None:
            return (None, None)

        code = self.geo_data_mapping[level]["property_code"]
        if code is None:
            return (None, None)

        cached_data = self.geo_data_cache.get(f"{level}:{code}", None)
        if cached_data is not None:
            return cached_data

        gfd = self.geo_data_mapping[level]["shapefile_data"]
        if gfd is None:
            return (None, None)

        filtered_gfd = gfd[gfd[code] == row[level]].copy()
        if isinstance(filtered_gfd, gpd.GeoDataFrame):
            # Use a tolerance value for simplification (smaller values will keep more detail)
            filtered_gfd["geometry"] = filtered_gfd["geometry"].apply(
                lambda geom: geom.simplify(tolerance=0.01, preserve_topology=True)
            )

            geojson = filtered_gfd.to_geo_dict()
            bbox = filtered_gfd.total_bounds.tolist()

            self.geo_data_cache[f"{level}:{code}"] = (geojson, bbox)

            return (geojson, bbox)

        return (None, None)

    def _generate_geo_data_mapping(self, root: etree._Element):
        level_maps = root.xpath("//level_maps/TR")

        geo_data: Dict[str, GeoDataEntry] = {}

        for level_row in level_maps:
            file_path = get_list_item_safe(level_row.xpath("filename/text()"), 0)
            level = get_list_item_safe(level_row.xpath("map_level/text()"), 0)
            property_code = get_list_item_safe(level_row.xpath("lev_code/text()"), 0)

            if file_path is not None:
                shp_file_name = Path(str(file_path)).name
                shapefile_data = gpd.read_file(f"zip://{self.data_source.tmp_zip_file.name}!{shp_file_name}")
            else:
                shapefile_data = None

            geo_data[f"level{level}"] = {
                "level": str(level) if level is not None else None,
                "property_code": str(property_code) if property_code is not None else None,
                "shapefile_data": shapefile_data,
            }

        self.geo_data_mapping = geo_data

    def _generate_hazard_name_mapping(self, root: etree._Element):
        hazard_details = root.xpath("//eventos/TR")

        for hazard_detail in hazard_details:
            key = get_list_item_safe(hazard_detail.xpath("nombre/text()"), 0)
            value = get_list_item_safe(hazard_detail.xpath("nombre_en/text()"), 0)

            self.hazard_name_mapping[str(key)] = str(value)

    def load_data(self):
        with self.data_source.with_xml_file() as xml_file:
            tree = etree.parse(xml_file)
            root = tree.getroot()

            self._generate_geo_data_mapping(root)
            self._generate_hazard_name_mapping(root)

            events = root.xpath("//fichas/TR")
            data: List[DataRow] = []

            def extract_value(obj: etree._Element, key: str, default: str | None = None):
                (xpath_value,) = (obj.xpath(f"{key}/text()"),)
                value = get_list_item_safe(xpath_value, 0)

                return str(value) if value is not None else default

            for event_row in events:
                serial = extract_value(event_row, "serial")
                if serial is None:
                    continue

                evento = extract_value(event_row, "evento")
                if evento is None:
                    continue

                row_data: DataRow = {
                    "serial": serial,
                    "comment": extract_value(event_row, "di_comments"),
                    "source": extract_value(event_row, "fuentes"),
                    "deaths": extract_value(event_row, "muertos"),
                    "injured": extract_value(event_row, "heridos"),
                    "missing": extract_value(event_row, "desaparece"),
                    "houses_destroyed": extract_value(event_row, "vivdest"),
                    "houses_damaged": extract_value(event_row, "vivafec"),
                    "directly_affected": extract_value(event_row, "damnificados"),
                    "indirectly_affected": extract_value(event_row, "afectados"),
                    "relocated": extract_value(event_row, "reubicados"),
                    "evacuated": extract_value(event_row, "evacuados"),
                    "losses_in_dollar": extract_value(event_row, "valorus"),
                    "losses_local_currency": extract_value(event_row, "valorloc"),
                    "education_centers": extract_value(event_row, "nescuelas"),
                    "hospitals": extract_value(event_row, "nhospitales"),
                    "damages_in_crops_ha": extract_value(event_row, "nhectareas"),
                    "lost_cattle": extract_value(event_row, "cabezas"),
                    "damages_in_roads_mts": extract_value(event_row, "kmvias"),
                    "level0": extract_value(event_row, "level0"),
                    "level1": extract_value(event_row, "level1"),
                    "level2": extract_value(event_row, "level2"),
                    "name0": extract_value(event_row, "name0"),
                    "name1": extract_value(event_row, "name1"),
                    "name2": extract_value(event_row, "name2"),
                    "latitude": extract_value(event_row, "latitude"),
                    "longitude": extract_value(event_row, "longitude"),
                    "haz_maxvalue": extract_value(event_row, "magnitud2"),
                    "event": self.hazard_name_mapping[evento],
                    "glide": extract_value(event_row, "glide"),
                    "location": extract_value(event_row, "lugar"),
                    "duration": extract_value(event_row, "duracion"),
                    "year": extract_value(event_row, "fechano"),
                    "month": extract_value(event_row, "fechames"),
                    "day": extract_value(event_row, "fechadia"),
                }

                data.append(row_data)

            return data
