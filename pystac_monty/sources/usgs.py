"""USGS data transformer for STAC Items."""

import json
import logging
from datetime import datetime
import typing
import pytz
from pystac import Asset, Item, Link
from shapely.geometry import Point, mapping, shape

from pystac_monty.extension import (
    HazardDetail,
    ImpactDetail,
    MontyEstimateType,
    MontyExtension,
    MontyImpactExposureCategory,
    MontyImpactType,
)
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import MontyDataSource, MontyDataTransformer
from pystac_monty.validators.usgs import USGSValidator, EmpiricalValidator

logger = logging.getLogger(__name__)

STAC_EVENT_ID_PREFIX = "usgs-event-"
STAC_HAZARD_ID_PREFIX = "usgs-hazard-"
STAC_IMPACT_ID_PREFIX = "usgs-impact-"


class USGSDataSource(MontyDataSource):
    """USGS data source that can handle both event detail and losses data."""

    def __init__(self, source_url: str, data: str, losses_data: typing.Optional[str] = None):
        """Initialize USGS data source.

        Args:
            source_url: URL where the data was retrieved from
            data: Event detail data as JSON string
            losses_data: Optional PAGER losses data as JSON string
        """
        super().__init__(source_url, data)
        self.data = json.loads(data)
        self.losses_data = json.loads(losses_data) if losses_data else None

    def get_data(self) -> dict[str, typing.Any]:
        """Get the event detail data."""
        return self.data

    def get_losses_data(self) -> list[dict[str, typing.Any]] | None:
        """Get the PAGER losses data if available."""
        return self.losses_data or []


class USGSTransformer(MontyDataTransformer[USGSDataSource]):
    """Transforms USGS earthquake event data into STAC Items."""

    hazard_profiles = MontyHazardProfiles()
    source_name = 'usgs'

    @staticmethod
    def iso2_to_iso3(iso2: str) -> str:
        """Convert ISO 2-letter country code to ISO 3-letter country code.

        Args:
            iso2: ISO 2-letter country code

        Returns:
            ISO 3-letter country code
        """
        # Common ISO2 to ISO3 mappings
        iso_mappings = {
            "AF": "AFG",
            "AL": "ALB",
            "DZ": "DZA",
            "AS": "ASM",
            "AD": "AND",
            "AO": "AGO",
            "AI": "AIA",
            "AQ": "ATA",
            "AG": "ATG",
            "AR": "ARG",
            "AM": "ARM",
            "AW": "ABW",
            "AU": "AUS",
            "AT": "AUT",
            "AZ": "AZE",
            "BS": "BHS",
            "BH": "BHR",
            "BD": "BGD",
            "BB": "BRB",
            "BY": "BLR",
            "BE": "BEL",
            "BZ": "BLZ",
            "BJ": "BEN",
            "BM": "BMU",
            "BT": "BTN",
            "BO": "BOL",
            "BA": "BIH",
            "BW": "BWA",
            "BV": "BVT",
            "BR": "BRA",
            "IO": "IOT",
            "BN": "BRN",
            "BG": "BGR",
            "BF": "BFA",
            "BI": "BDI",
            "KH": "KHM",
            "CM": "CMR",
            "CA": "CAN",
            "CV": "CPV",
            "KY": "CYM",
            "CF": "CAF",
            "TD": "TCD",
            "CL": "CHL",
            "CN": "CHN",
            "CX": "CXR",
            "CC": "CCK",
            "CO": "COL",
            "KM": "COM",
            "CG": "COG",
            "CD": "COD",
            "CK": "COK",
            "CR": "CRI",
            "CI": "CIV",
            "HR": "HRV",
            "CU": "CUB",
            "CY": "CYP",
            "CZ": "CZE",
            "DK": "DNK",
            "DJ": "DJI",
            "DM": "DMA",
            "DO": "DOM",
            "EC": "ECU",
            "EG": "EGY",
            "SV": "SLV",
            "GQ": "GNQ",
            "ER": "ERI",
            "EE": "EST",
            "ET": "ETH",
            "FK": "FLK",
            "FO": "FRO",
            "FJ": "FJI",
            "FI": "FIN",
            "FR": "FRA",
            "GF": "GUF",
            "PF": "PYF",
            "TF": "ATF",
            "GA": "GAB",
            "GM": "GMB",
            "GE": "GEO",
            "DE": "DEU",
            "GH": "GHA",
            "GI": "GIB",
            "GR": "GRC",
            "GL": "GRL",
            "GD": "GRD",
            "GP": "GLP",
            "GU": "GUM",
            "GT": "GTM",
            "GN": "GIN",
            "GW": "GNB",
            "GY": "GUY",
            "HT": "HTI",
            "HM": "HMD",
            "VA": "VAT",
            "HN": "HND",
            "HK": "HKG",
            "HU": "HUN",
            "IS": "ISL",
            "IN": "IND",
            "ID": "IDN",
            "IR": "IRN",
            "IQ": "IRQ",
            "IE": "IRL",
            "IL": "ISR",
            "IT": "ITA",
            "JM": "JAM",
            "JP": "JPN",
            "JO": "JOR",
            "KZ": "KAZ",
            "KE": "KEN",
            "KI": "KIR",
            "KP": "PRK",
            "KR": "KOR",
            "KW": "KWT",
            "KG": "KGZ",
            "LA": "LAO",
            "LV": "LVA",
            "LB": "LBN",
            "LS": "LSO",
            "LR": "LBR",
            "LY": "LBY",
            "LI": "LIE",
            "LT": "LTU",
            "LU": "LUX",
            "MO": "MAC",
            "MK": "MKD",
            "MG": "MDG",
            "MW": "MWI",
            "MY": "MYS",
            "MV": "MDV",
            "ML": "MLI",
            "MT": "MLT",
            "MH": "MHL",
            "MQ": "MTQ",
            "MR": "MRT",
            "MU": "MUS",
            "YT": "MYT",
            "MX": "MEX",
            "FM": "FSM",
            "MD": "MDA",
            "MC": "MCO",
            "MN": "MNG",
            "MS": "MSR",
            "MA": "MAR",
            "MZ": "MOZ",
            "MM": "MMR",
            "NA": "NAM",
            "NR": "NRU",
            "NP": "NPL",
            "NL": "NLD",
            "NC": "NCL",
            "NZ": "NZL",
            "NI": "NIC",
            "NE": "NER",
            "NG": "NGA",
            "NU": "NIU",
            "NF": "NFK",
            "MP": "MNP",
            "NO": "NOR",
            "OM": "OMN",
            "PK": "PAK",
            "PW": "PLW",
            "PS": "PSE",
            "PA": "PAN",
            "PG": "PNG",
            "PY": "PRY",
            "PE": "PER",
            "PH": "PHL",
            "PN": "PCN",
            "PL": "POL",
            "PT": "PRT",
            "PR": "PRI",
            "QA": "QAT",
            "RE": "REU",
            "RO": "ROU",
            "RU": "RUS",
            "RW": "RWA",
            "SH": "SHN",
            "KN": "KNA",
            "LC": "LCA",
            "PM": "SPM",
            "VC": "VCT",
            "WS": "WSM",
            "SM": "SMR",
            "ST": "STP",
            "SA": "SAU",
            "SN": "SEN",
            "SC": "SYC",
            "SL": "SLE",
            "SG": "SGP",
            "SK": "SVK",
            "SI": "SVN",
            "SB": "SLB",
            "SO": "SOM",
            "ZA": "ZAF",
            "GS": "SGS",
            "ES": "ESP",
            "LK": "LKA",
            "SD": "SDN",
            "SR": "SUR",
            "SJ": "SJM",
            "SZ": "SWZ",
            "SE": "SWE",
            "CH": "CHE",
            "SY": "SYR",
            "TW": "TWN",
            "TJ": "TJK",
            "TZ": "TZA",
            "TH": "THA",
            "TL": "TLS",
            "TG": "TGO",
            "TK": "TKL",
            "TO": "TON",
            "TT": "TTO",
            "TN": "TUN",
            "TR": "TUR",
            "TM": "TKM",
            "TC": "TCA",
            "TV": "TUV",
            "UG": "UGA",
            "UA": "UKR",
            "AE": "ARE",
            "GB": "GBR",
            "US": "USA",
            "UM": "UMI",
            "UY": "URY",
            "UZ": "UZB",
            "VU": "VUT",
            "VE": "VEN",
            "VN": "VNM",
            "VG": "VGB",
            "VI": "VIR",
            "WF": "WLF",
            "EH": "ESH",
            "YE": "YEM",
            "ZM": "ZMB",
            "ZW": "ZWE",
        }
        return iso_mappings[iso2.upper()]

    def get_stac_items(self) -> typing.Generator[Item, None, None]:
        """Creates the STAC Items"""
        self.transform_summary.mark_as_started()

        item_data = self.data_source.get_data()
        losspager_data = self.data_source.get_losses_data()

        # Note that only one datapoint is sent
        self.transform_summary.increment_rows(1)
        try:
            def get_validated_data(items: list[dict[str, typing.Any]]) -> typing.List[EmpiricalValidator]:
                validated_losspager_data: list[EmpiricalValidator] = []
                for item in items:
                    obj = EmpiricalValidator(**item)
                    validated_losspager_data.append(obj)
                return validated_losspager_data

            validated_item = USGSValidator(**item_data)

            if event_item := self.make_source_event_item(item_data=validated_item):
                yield event_item
                losspager_validated_items = get_validated_data(losspager_data)
                hazard_item = self.make_hazard_event_item(event_item=event_item, data_item=validated_item)
                yield hazard_item
                yield from self.make_impact_items(event_item=event_item, hazard_item=hazard_item, losspager_items=losspager_validated_items)
            else:
                self.transform_summary.increment_failed_rows(1)
        except Exception:
            self.transform_summary.increment_failed_rows(1)
            logger.warning("Failed to process the USGS data.", exc_info=True)

        self.transform_summary.mark_as_complete()

    # TODO This method is deprecated
    def make_items(self) -> typing.List[Item]:
        return list(self.get_stac_items())

    def make_source_event_item(self, item_data: USGSValidator) -> Item:
        """Create source event item from USGS data."""

        # Create geometry from coordinates
        longitude = item_data.geometry.coordinates[0]
        latitude = item_data.geometry.coordinates[1]
        point = Point(longitude, latitude)

        event_datetime = datetime.fromtimestamp(item_data.properties.time / 1_000, pytz.UTC)

        # TODO Verify the logic for depth
        if item_data.properties.products.shakemap:
            eq_depth = item_data.properties.products.shakemap[0].properties.depth
        else:
            eq_depth = "-"

        item = Item(
            id=f"{STAC_EVENT_ID_PREFIX}{item_data.id}",
            geometry=mapping(point),
            bbox=[longitude, latitude, longitude, latitude],
            datetime=event_datetime,
            properties={
                "title": item_data.properties.title,
                "description": item_data.properties.place,
                "eq:magnitude": item_data.properties.mag,
                "eq:magnitude_type": item_data.properties.magType,
                "eq:status": item_data.properties.status,
                "eq:tsunami": bool(item_data.properties.tsunami),
                "eq:felt": item_data.properties.felt,
                "eq:depth": eq_depth
            },
        )

        # Add Monty extension
        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = 1
        monty.hazard_codes = ["GH0004"]  # Earthquake surface rupture code

        # TODO Get country code from event data or geometry
        iso3 = self.geocoder.get_iso3_from_point(point) or 'UNK'
        country_codes = [iso3]

        monty.country_codes = country_codes

        # Compute correlation ID
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        # Set collection and roles
        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        # Add source link and assets
        item.add_link(Link("via", self.data_source.get_source_url(), "application/json", "USGS Event Data"))
        item.add_asset(
            "source",
            Asset(
                href=self.data_source.get_source_url(),
                media_type="application/geo+json",
                title="USGS GeoJSON Source",
                roles=["source"],
            ),
        )

        return item

    def make_hazard_event_item(self, event_item: Item, data_item: USGSValidator) -> Item:
        """Create hazard item (ShakeMap) from USGS data."""

        hazard_item = event_item.clone()
        hazard_item.id = f"{STAC_HAZARD_ID_PREFIX}{hazard_item.id.replace(STAC_EVENT_ID_PREFIX, '')}-shakemap"

        # extent the hazard zone with the shakemap extent
        shakemap = None
        shakemaps = data_item.properties.products.shakemap or []
        if shakemaps:
            shakemap = shakemaps[0]

        if shakemap:
            extent = [
                float(shakemap.properties.minimum_longitude or 0.0),
                float(shakemap.properties.minimum_latitude or 0.0),
                float(shakemap.properties.maximum_longitude or 0.0),
                float(shakemap.properties.maximum_latitude or 0.0),
            ]
        else:
            extent = [0.0, 0.0, 0.0, 0.0]

        hazard_item.bbox = extent
        # polygon from extent
        hazard_item.geometry = mapping(
            shape(
                {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [extent[0], extent[1]],
                            [extent[2], extent[1]],
                            [extent[2], extent[3]],
                            [extent[0], extent[3]],
                            [extent[0], extent[1]],
                        ]
                    ],
                }
            )
        )

        # Set collection and roles
        hazard_item.set_collection(self.get_hazard_collection())
        hazard_item.properties["roles"] = ["source", "hazard"]

        # Add hazard detail
        monty = MontyExtension.ext(hazard_item)
        monty.hazard_detail = HazardDetail(
            cluster="GEO-SEIS",
            severity_value=float(event_item.properties.get("eq:magnitude", 0)),
            severity_unit=event_item.properties.get("eq:magnitude_type", ""),
            estimate_type=MontyEstimateType.PRIMARY,
        )

        # Add shakemap assets
        # download/pin-thumbnail.png
        if shakemap:
            pin_thumbnail = shakemap.contents.download_pin_thumbnail
            if pin_thumbnail:
                shakemap_assets: dict[str, typing.Any] = {
                    "intensity_map": {
                        "href": pin_thumbnail.url,
                        "media_type": "image/png",
                        "title": "Intensity Map",
                        "roles": ["overview"],
                    }
                }
            else:
                shakemap_assets = {}

            for key, asset_info in shakemap_assets.items():
                hazard_item.add_asset(key, Asset(**asset_info))

        return hazard_item

    def make_impact_items(self, event_item: Item, hazard_item: Item, losspager_items: typing.List[EmpiricalValidator]) -> typing.List[Item]:
        """Create impact items (PAGER) from USGS data."""
        if not losspager_items:
            return []

        impact_items = []

        # Create fatalities impact item
        for loss_data in losspager_items:
            if loss_data.empirical_fatality:
                for country in loss_data.empirical_fatality.country_fatalities:
                    if not country.fatalities:
                        continue
                    fatalities_item = self._create_impact_item_from_losses(
                        "fatalities",
                        MontyImpactExposureCategory.ALL_PEOPLE,
                        MontyImpactType.DEATH,
                        country.fatalities,
                        "people",
                        country.country_code,
                        hazard_item,
                        event_item,
                    )
                    impact_items.append(fatalities_item)

            # Create economic losses impact item
            if loss_data.empirical_economic:
                for country in loss_data.empirical_economic.country_dollars:
                    if not country.us_dollars:
                        continue
                    economic_item = self._create_impact_item_from_losses(
                        "economic",
                        MontyImpactExposureCategory.BUILDINGS,
                        MontyImpactType.LOSS_COST,
                        country.us_dollars,
                        "usd",
                        country.country_code,
                        hazard_item,
                        event_item,
                    )
                    impact_items.append(economic_item)

        return impact_items

    def _create_impact_item_from_losses(
        self,
        impact_type: str,
        category: MontyImpactExposureCategory,
        imp_type: MontyImpactType,
        value: float,
        unit: str,
        iso2: str,
        hazard_item: Item,
        event_item: Item,
    ) -> Item:
        """Helper method to create impact items from PAGER losses data."""

        impact_item = event_item.clone()
        impact_item.id = f"{STAC_IMPACT_ID_PREFIX}{impact_item.id.replace(STAC_EVENT_ID_PREFIX, '')}-{impact_type}-{iso2}"

        # Set title and description
        title_prefix = "Estimated Fatalities" if impact_type == "fatalities" else "Estimated Economic Losses"
        impact_item.properties["title"] = f"{title_prefix} for {event_item.properties['title']}"
        impact_item.properties["description"] = f"PAGER {title_prefix.lower()} for {event_item.common_metadata.title}"

        # Set collection and roles
        impact_item.set_collection(self.get_impact_collection())
        impact_item.properties["roles"] = ["source", "impact"]

        # Add impact detail
        monty = MontyExtension.ext(impact_item)
        monty.country_codes = [self.iso2_to_iso3(iso2)]
        monty.impact_detail = ImpactDetail(
            category=category,
            type=imp_type,
            value=value,
            unit=unit,
            estimate_type=MontyEstimateType.MODELLED,
        )
        geom = self.geocoder.get_geometry_from_iso3(monty.country_codes[0])
        if geom:
            # intersect with the hazard geometry
            geom = shape(geom["geometry"]).intersection(shape(hazard_item.geometry))
        impact_item.geometry = mapping(geom)
        impact_item.bbox = geom.bounds

        # Add PAGER assets
        pager_assets: dict[str, typing.Any] = {
            "pager_onepager": {
                "href": f"{self.data_source.get_source_url()}/onepager.pdf",
                "media_type": "application/pdf",
                "title": "PAGER One-Pager Report",
                "roles": ["data"],
            },
            "pager_exposure": {
                "href": f"{self.data_source.get_source_url()}/json/exposures.json",
                "media_type": "application/json",
                "title": "PAGER Exposure Data",
                "roles": ["data"],
            },
            "pager_alert": {
                "href": f"{self.data_source.get_source_url()}/alert{impact_type}.pdf",
                "media_type": "application/pdf",
                "title": f"PAGER {impact_type.title()} Alert",
                "roles": ["data"],
            },
        }

        for key, asset_info in pager_assets.items():
            impact_item.add_asset(key, Asset(**asset_info))

        return impact_item
