from datetime import datetime
from typing import Optional, TypedDict

import pydantic
import pytz
from geopandas import gpd

STAC_EVENT_ID_PREFIX = "desinventar-event-"
STAC_HAZARD_ID_PREFIX = "desinventar-hazard-"
STAC_IMPACT_ID_PREFIX = "desinventar-impact-"


class GeoDataEntry(TypedDict):
    level: Optional[str]
    property_code: Optional[str]
    shapefile_data: Optional[gpd.GeoDataFrame]


# Properties extracted from desinventar
class DataRow(pydantic.BaseModel):
    serial: str
    comment: str | None
    # source: str | None

    deaths: float | None
    injured: float | None
    missing: float | None
    houses_destroyed: float | None
    houses_damaged: float | None
    directly_affected: float | None
    indirectly_affected: float | None
    relocated: float | None
    evacuated: float | None
    losses_in_dollar: float | None
    losses_local_currency: float | None
    # education_centers: str | None
    # hospitals: str | None
    damages_in_crops_ha: float | None
    lost_cattle: float | None
    damages_in_roads_mts: float | None

    level0: str | None
    level1: str | None
    level2: str | None
    # name0: str | None
    # name1: str | None
    # name2: str | None
    # latitude: str | None
    # longitude: str | None

    # haz_maxvalue: str | None
    event: str | None
    # glide: str | None
    location: str | None

    # duration: str | None
    year: int
    month: int | None
    day: int | None

    # Added fields

    iso3: str
    data_source_url: str | None

    @property
    def event_stac_id(self):
        return f"{STAC_EVENT_ID_PREFIX}{self.iso3}-{self.serial}"

    @property
    def event_title(self):
        return f"{self.event} in {self.location} on {self.event_start_date}"

    @property
    def event_description(self):
        return f"{self.event} in {self.location}: {self.comment}"

    @property
    def event_start_date(self):
        if self.year is None:
            return

        start_year = self.year
        start_month = self.month or 1
        start_day = self.day or 1

        try:
            start_dt = datetime(start_year, start_month, start_day)
            return pytz.utc.localize(start_dt)
        except Exception:
            return None

    @property
    def lowest_level(self):
        if self.level2 is not None:
            return "level2"
        if self.level1 is not None:
            return "level1"
        if self.level0 is not None:
            return "level0"
        return None
