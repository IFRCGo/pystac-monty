import logging
import tempfile
import typing
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TypedDict
from zipfile import ZipFile

import pydantic
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
from pystac_monty.validators.desinventar import (
    STAC_EVENT_ID_PREFIX,
    STAC_HAZARD_ID_PREFIX,
    STAC_IMPACT_ID_PREFIX,
    DataRow,
    GeoDataEntry,
)

logger = logging.getLogger(__name__)

T = typing.TypeVar("T")


# TODO: move to common utils
def get_list_item_safe(lst: list[T], index: int, default_value: T | None = None) -> T | None:
    try:
        return lst[index]
    except IndexError:
        return default_value


def extract_value_from_xml(obj: etree._Element, key: str) -> Any:
    (xpath_value,) = (obj.xpath(f"{key}/text()"),)
    value = get_list_item_safe(xpath_value, 0)
    return value


def parse_row_data(
    xml_row_data: etree._Element,
    hazard_name_mapping: dict[str, str],
    iso3: str,
    data_source_url: str | None,
):
    serial = extract_value_from_xml(xml_row_data, "serial")
    # FIXME: Do we handle this as failure?
    if serial is None:
        return

    evento = extract_value_from_xml(xml_row_data, "evento")
    # FIXME: Do we handle this as failure?
    if evento is None:
        return

    return DataRow(
        serial=serial,
        event=hazard_name_mapping[evento],
        comment=extract_value_from_xml(xml_row_data, "di_comments"),
        # source=extract_value(xml_row_data, "fuentes"),
        deaths=extract_value_from_xml(xml_row_data, "muertos"),
        injured=extract_value_from_xml(xml_row_data, "heridos"),
        missing=extract_value_from_xml(xml_row_data, "desaparece"),
        houses_destroyed=extract_value_from_xml(xml_row_data, "vivdest"),
        houses_damaged=extract_value_from_xml(xml_row_data, "vivafec"),
        directly_affected=extract_value_from_xml(xml_row_data, "damnificados"),
        indirectly_affected=extract_value_from_xml(xml_row_data, "afectados"),
        relocated=extract_value_from_xml(xml_row_data, "reubicados"),
        evacuated=extract_value_from_xml(xml_row_data, "evacuados"),
        losses_in_dollar=extract_value_from_xml(xml_row_data, "valorus"),
        losses_local_currency=extract_value_from_xml(xml_row_data, "valorloc"),
        # education_centers=extract_value(xml_row_data, "nescuelas"),
        # hospitals=extract_value(xml_row_data, "nhospitales"),
        damages_in_crops_ha=extract_value_from_xml(xml_row_data, "nhectareas"),
        lost_cattle=extract_value_from_xml(xml_row_data, "cabezas"),
        damages_in_roads_mts=extract_value_from_xml(xml_row_data, "kmvias"),
        level0=extract_value_from_xml(xml_row_data, "level0"),
        level1=extract_value_from_xml(xml_row_data, "level1"),
        level2=extract_value_from_xml(xml_row_data, "level2"),
        # name0=extract_value(xml_row_data, "name0"),
        # name1=extract_value(xml_row_data, "name1"),
        # name2=extract_value(xml_row_data, "name2"),
        # latitude=extract_value(xml_row_data, "latitude"),
        # longitude=extract_value(xml_row_data, "longitude"),
        # haz_maxvalue=extract_value(xml_row_data, "magnitud2"),
        # glide=extract_value(xml_row_data, "glide"),
        location=extract_value_from_xml(xml_row_data, "lugar"),
        # duration=extract_value(xml_row_data, "duracion"),
        year=extract_value_from_xml(xml_row_data, "fechano"),
        month=extract_value_from_xml(xml_row_data, "fechames"),
        day=extract_value_from_xml(xml_row_data, "fechadia"),
        iso3=iso3,
        data_source_url=data_source_url,
    )


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


# FIXME: cleanup named temporary file
class DesinventarDataSource:
    tmp_zip_file: tempfile._TemporaryFileWrapper
    source_url: str | None
    country_code: str
    iso3: str

    def __init__(self, tmp_zip_file: tempfile._TemporaryFileWrapper, country_code: str, iso3: str, source_url: str | None = None):
        self.tmp_zip_file = tmp_zip_file
        self.country_code = country_code
        self.iso3 = iso3
        self.source_url = source_url

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
    def from_url(cls, zip_file_url: str, country_code: str, iso3: str):
        response = requests.get(zip_file_url)
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


class DesinventarTransformer(MontyDataTransformer):
    """Transform DesInventar data to STAC items"""

    source_name = 'desinventar'

    data_source: DesinventarDataSource
    hazard_profiles = MontyHazardProfiles()
    geo_data_mapping: Dict[str, GeoDataEntry] = {}
    geo_data_cache: Dict[str, Tuple[Dict[str, Any], List[float]]] = {}
    errored_events: Dict[str, int] = {}

    def _create_event_item_from_row(self, row: DataRow) -> Optional[Item]:
        # FIXME: Do we treat this as error or noise
        if not row.event_start_date:
            return None

        # FIXME: Do we treat this as error or noise
        if row.event is None:
            return None

        # FIXME: Do we treat this as error or noise
        if (hazard_codes := hazard_mapping.get(row.event)) is None:
            return None

        geojson, bbox = self._get_geojson_and_bbox_from_row(row)
        geojson_features = geojson.get("features", None) if geojson is not None else None

        geometry: dict[str, Any] | None = None
        if geojson_features is not None and len(geojson_features) > 0:
            geometry = geojson_features[0].get("geometry", None)
            # TODO: investigate if properties can be added to keywords
            # properties = geojson_features[0].get('properties', None)

        item = Item(
            id=row.event_stac_id,
            geometry=geometry,
            bbox=bbox,
            datetime=row.event_start_date,
            start_datetime=row.event_start_date,
            # FIXME: calculate end date
            end_datetime=row.event_start_date,
            properties={
                "title": row.event_title,
                "description": row.event_description
            },
        )

        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)

        monty.episode_number = 1  # Desinventar doesn't have episodes
        monty.hazard_codes = hazard_codes
        monty.country_codes = [row.iso3.upper()]
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]
        # Add source link
        if row.data_source_url:
            item.add_link(
                Link(
                    "via",
                    row.data_source_url,
                    "application/zip",
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
            category=category,
            type=impact_type,
            value=value,
            unit=unit,
            estimate_type=MontyEstimateType.PRIMARY
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

    def _generate_geo_data_mapping(self, root: etree._Element) -> Dict[str, GeoDataEntry]:
        geo_data: Dict[str, GeoDataEntry] = {}

        level_maps = root.xpath("//level_maps/TR")
        for level_row in level_maps:
            file_path = get_list_item_safe(level_row.xpath("filename/text()"), 0)
            level = get_list_item_safe(level_row.xpath("map_level/text()"), 0)
            property_code = get_list_item_safe(level_row.xpath("lev_code/text()"), 0)

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
                "shapefile_data": shapefile_data
            }

        return geo_data

    @staticmethod
    def _generate_hazard_name_mapping(root: etree._Element):
        hazard_name_mapping: Dict[str, str] = {}

        hazard_details = root.xpath("//eventos/TR")
        for hazard_detail in hazard_details:
            key = get_list_item_safe(hazard_detail.xpath("nombre/text()"), 0)
            value = get_list_item_safe(hazard_detail.xpath("nombre_en/text()"), 0)

            hazard_name_mapping[str(key)] = str(value)

        return hazard_name_mapping

    def get_stac_items(self) -> typing.Generator[Item, None, None]:
        # TODO: Use sax xml parser for memory efficient usage
        with self.data_source.with_xml_file() as xml_file:
            tree = etree.parse(xml_file)
            root = tree.getroot()

            self.geo_data_mapping = self._generate_geo_data_mapping(root)
            hazard_name_mapping = self._generate_hazard_name_mapping(root)

            events = root.xpath("//fichas/TR")

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
                            yield event_item
                            yield from self._create_impact_items_from_row(row_data, event_item)
                        else:
                            self.transform_summary.increment_failed_rows()
                except Exception:
                    self.transform_summary.increment_failed_rows()
                    logger.error('Failed to process desinventar', exc_info=True)
            self.transform_summary.mark_as_complete()

    # FIXME: This is deprecated
    def make_items(self):
        return list(self.get_stac_items())
