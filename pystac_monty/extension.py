"""Implements the :stac-ext:`Monty Extension <monty>`."""

from __future__ import annotations

from abc import ABC
from collections.abc import Mapping
from typing import Any, Generic, Iterator, Literal, TypeVar, Union, cast

import pystac
import pystac.extensions
import pystac.extensions.ext
from pystac.extensions import item_assets
from pystac.extensions.base import ExtensionManagementMixin, PropertiesExtension
from pystac.extensions.hooks import ExtensionHooks
from pystac.utils import StringEnum, get_opt, get_required, map_opt

from pystac_monty.hazard_profiles import HazardProfiles
from pystac_monty.paring import Pairing

__version__ = "0.1.0"

T = TypeVar("T", pystac.Collection, pystac.Item, pystac.Asset, item_assets.AssetDefinition)

SCHEMA_URI = "https://ifrcgo.github.io/monty/v0.1.0/schema.json"

PREFIX: str = "monty:"

# Item properties
ITEM_COUNTRY_CODES_PROP = PREFIX + "country_codes"
ITEM_CORR_ID_PROP = PREFIX + "corr_id"
ITEM_HAZARD_CODES_PROP = PREFIX + "hazard_codes"
ITEM_HAZARD_DETAIL_PROP = PREFIX + "hazard_detail"
ITEM_IMPACT_DETAIL_PROP = PREFIX + "impact_detail"
ITEM_EPISODE_NUMBER_PROP = PREFIX + "episode_number"

# Hazard Detail properties
HAZDET_CLUSTER_PROP = "cluster"
HAZDET_SEV_VALUE_PROP = "severity_value"
HAZDET_SEV_UNIT_PROP = "severity_unit"
HAZDET_SEV_LABEL_PROP = "severity_label"
HAZDET_ESTIMATE_TYPE_PROP = "estimate_type"

# Impact Detail properties
IMPDET_CATEGORY_PROP = "category"
IMPDET_TYPE_PROP = "type"
IMPDET_VALUE_PROP = "value"
IMPDET_UNIT_PROP = "unit"
IMPDET_ESTIMATE_TYPE_PROP = "estimate_type"

# Link attributes
LINK_ATTRS_OCCURRENCE_TYPE_PROP = "occ_type"
LINK_ATTRS_OCCURRENCE_PROBABILITY_PROP = "occ_prob"
LINK_ATTRS_OCCURRENCE_PROBABILITY_DEF_PROP = "occ_probdef"


class MontyRoles(StringEnum):
    """A set of roles are defined to describe the type of the data."""

    EVENT = "event"
    REFERENCE = "reference"
    SOURCE = "source"
    HAZARD = "hazard"
    IMPACT = "impact"
    RESPONSE = "response"


class MontyOccurenceTypeLinkAttributes(StringEnum):
    """Allowed values for ``axis`` field of :class:`HorizontalSpatialDimension`
    object."""

    KNOWN = "known"
    POTENTIAL = "potential"


class MontyEstimateType(StringEnum):
    """Allowed values for ``estimate_type`` field of :class:`HazardDetail`
    object."""

    PRIMARY = "primary"
    SECONDARY = "secondary"
    MODELLED = "modelled"


class MontyImpactExposureCategory(StringEnum):
    ALL_PEOPLE = "allpeop"
    CROP = "crop"
    WOMEN = "women"
    MEN = "men"
    ELDERLY = "elder"
    CHILDREN_UNDER_14 = "chld14"
    CHILDREN_UNDER_18 = "chld18"
    WHEELCHAIR_USERS = "wheelch"
    ROAD = "road"
    TRAIN_LINES = "trainlin"
    VULNERABLE_EMPLOYMENT = "vulempl"
    BUILDINGS = "build"
    RECONSTRUCTION_COSTS = "reccost"
    HOSPITALS = "hosp"
    EDUCATION_CENTERS = "educ"
    LOCAL_CURRENCY = "loccur"
    GLOBAL_CURRENCY = "globdate"
    INFLATION_ADJUSTED_LOCAL_CURRENCY = "infloccur"
    INFLATION_ADJUSTED_GLOBAL_CURRENCY = "infglobdate"
    USD_UNSURE = "usdunsure"
    AID_CONTRIBUTIONS_INFLATION_ADJUSTED = "aidinf"
    AID_CONTRIBUTIONS_NON_INFLATION_ADJUSTED = "aidnoninf"
    AID_CONTRIBUTIONS_UNSPECIFIED = "aidunkinf"
    RECONSTRUCTION_COSTS_INFLATION_ADJUSTED = "ecorecinf"
    RECONSTRUCTION_COSTS_NON_INFLATION_ADJUSTED = "ecorecnoninf"
    RECONSTRUCTION_COSTS_UNSPECIFIED = "ecorecunkinf"
    INSURED_COSTS_INFLATION_ADJUSTED = "ecoinsinf"
    INSURED_COSTS_NON_INFLATION_ADJUSTED = "ecoinsnoninf"
    INSURED_COSTS_UNSPECIFIED = "ecoinsunkinf"
    UNINSURED_COSTS_INFLATION_ADJUSTED = "ecouninsinf"
    UNINSURED_COSTS_NON_INFLATION_ADJUSTED = "ecouninsnoninf"
    UNINSURED_COSTS_UNSPECIFIED = "ecouninsunkinf"
    TOTAL_COST_INFLATION_ADJUSTED = "ecototinf"
    TOTAL_COST_NON_INFLATION_ADJUSTED = "ecototnoninf"
    TOTAL_COST_UNSPECIFIED = "ecototunkinf"
    TOTAL_DIRECT_COSTS_INFLATION_ADJUSTED = "ecodirtotinf"
    TOTAL_DIRECT_COSTS_NON_INFLATION_ADJUSTED = "ecodirtotnoninf"
    TOTAL_DIRECT_COSTS_UNSPECIFIED = "ecodirtotunkinf"
    TOTAL_INDIRECT_COSTS_INFLATION_ADJUSTED = "ecoindirtotinf"
    TOTAL_INDIRECT_COSTS_NON_INFLATION_ADJUSTED = "ecoindirtotnoninf"
    TOTAL_INDIRECT_COSTS_UNSPECIFIED = "ecoindirtotunkinf"
    CATTLE = "cattle"
    ALERTSCORE = "alert"
    IFRC_AID_CONTRIBUTIONS_UNSPECIFIED = "ecoifrcall"


class MontyImpactExposureCatgoryLabel(Mapping):
    def __init__(self) -> None:
        self._data = {
            MontyImpactExposureCategory.ALL_PEOPLE: "People (All Demographics)",
            MontyImpactExposureCategory.CROP: "Crops",
            MontyImpactExposureCategory.WOMEN: "Women",
            MontyImpactExposureCategory.MEN: "Men",
            MontyImpactExposureCategory.ELDERLY: "Elderly (Over 65)",
            MontyImpactExposureCategory.CHILDREN_UNDER_14: "Children (Under 14)",
            MontyImpactExposureCategory.CHILDREN_UNDER_18: "Children (Under 18)",
            MontyImpactExposureCategory.WHEELCHAIR_USERS: "Wheelchair Users",
            MontyImpactExposureCategory.ROAD: "Road",
            MontyImpactExposureCategory.TRAIN_LINES: "Train-lines",
            MontyImpactExposureCategory.VULNERABLE_EMPLOYMENT: "Population in Vulnerable Employment",
            MontyImpactExposureCategory.BUILDINGS: "Buildings",
            MontyImpactExposureCategory.RECONSTRUCTION_COSTS: "Reconstruction Costs",
            MontyImpactExposureCategory.HOSPITALS: "Hospitals",
            MontyImpactExposureCategory.EDUCATION_CENTERS: "Education Centers",
            MontyImpactExposureCategory.LOCAL_CURRENCY: "Local Currency [Date of Event]",
            MontyImpactExposureCategory.GLOBAL_CURRENCY: "Global/Regional Currency (e.g. USD)",
            MontyImpactExposureCategory.INFLATION_ADJUSTED_LOCAL_CURRENCY: "Inflation-Adjusted Local Currency [Date of Event]",
            MontyImpactExposureCategory.INFLATION_ADJUSTED_GLOBAL_CURRENCY: "Inflation-Adjusted Global/Regional Currency (USD)",
            MontyImpactExposureCategory.USD_UNSURE: "USD [Unsure]",
            MontyImpactExposureCategory.AID_CONTRIBUTIONS_INFLATION_ADJUSTED: "Aid Contributions Inflation-Adjusted",
            MontyImpactExposureCategory.AID_CONTRIBUTIONS_NON_INFLATION_ADJUSTED: "Aid Contributions Non-Inflation-Adjusted",
            MontyImpactExposureCategory.AID_CONTRIBUTIONS_UNSPECIFIED: "Aid Contributions (Unspecified-Inflation-Adjustment)",
            MontyImpactExposureCategory.RECONSTRUCTION_COSTS_INFLATION_ADJUSTED: "Reconstruction Costs Inflation-Adjusted",
            MontyImpactExposureCategory.RECONSTRUCTION_COSTS_NON_INFLATION_ADJUSTED: "Reconstruction Costs Non-Inflation-Adjusted",  # noqa: E501
            MontyImpactExposureCategory.RECONSTRUCTION_COSTS_UNSPECIFIED: "Reconstruction Costs (Unspecified-Inflation-Adjustment)",  # noqa: E501
            MontyImpactExposureCategory.INSURED_COSTS_INFLATION_ADJUSTED: "Insured Costs Inflation-Adjusted",
            MontyImpactExposureCategory.INSURED_COSTS_NON_INFLATION_ADJUSTED: "Insured Costs Non-Inflation-Adjusted",
            MontyImpactExposureCategory.INSURED_COSTS_UNSPECIFIED: "Insured Costs (Unspecified-Inflation-Adjustment)",
            MontyImpactExposureCategory.UNINSURED_COSTS_INFLATION_ADJUSTED: "Uninsured Costs Inflation-Adjusted",
            MontyImpactExposureCategory.UNINSURED_COSTS_NON_INFLATION_ADJUSTED: "Uninsured Costs Non-Inflation-Adjusted",
            MontyImpactExposureCategory.UNINSURED_COSTS_UNSPECIFIED: "Uninsured Costs (Unspecified-Inflation-Adjustment)",
            MontyImpactExposureCategory.TOTAL_COST_INFLATION_ADJUSTED: "Total Cost Inflation-Adjusted",
            MontyImpactExposureCategory.TOTAL_COST_NON_INFLATION_ADJUSTED: "Total Cost Non-Inflation-Adjusted",
            MontyImpactExposureCategory.TOTAL_COST_UNSPECIFIED: "Total Cost (Unspecified-Inflation-Adjustment)",
            MontyImpactExposureCategory.TOTAL_DIRECT_COSTS_INFLATION_ADJUSTED: "Total Direct Costs Inflation-Adjusted",
            MontyImpactExposureCategory.TOTAL_DIRECT_COSTS_NON_INFLATION_ADJUSTED: "Total Direct Costs Non-Inflation-Adjusted",
            MontyImpactExposureCategory.TOTAL_DIRECT_COSTS_UNSPECIFIED: "Total Direct Costs (Unspecified-Inflation-Adjustment)",
            MontyImpactExposureCategory.TOTAL_INDIRECT_COSTS_INFLATION_ADJUSTED: "Total Indirect Costs Inflation-Adjusted",
            MontyImpactExposureCategory.TOTAL_INDIRECT_COSTS_NON_INFLATION_ADJUSTED: "Total Indirect Costs Non-Inflation-Adjusted",  # noqa: E501
            MontyImpactExposureCategory.TOTAL_INDIRECT_COSTS_UNSPECIFIED: "Total Indirect Costs (Unspecified-Inflation-Adjustment)",  # noqa: E501
            MontyImpactExposureCategory.CATTLE: "Cattle",
            MontyImpactExposureCategory.ALERTSCORE: "Alertscore",
            MontyImpactExposureCategory.IFRC_AID_CONTRIBUTIONS_UNSPECIFIED: "IFRC Aid Contributions (Unspecified-Inflation-Adjustment)",  # noqa: E501
        }

    def __getitem__(self, key: MontyImpactExposureCategory) -> str:
        return self._data[key]

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self) -> Iterator:
        return iter(self._data)


class MontyImpactType(StringEnum):
    UNDEFINED = "unspec"
    UNAFFECTED = "unaff"
    DAMAGED = "dama"
    DESTROYED = "dest"
    POTENTIALLY_DAMAGED = "potdam"
    TOTAL_AFFECTED = "affe"
    DIRECTLY_AFFECTED = "diraffe"
    INDIRECTLY_AFFECTED = "indaffe"
    DEATHS = "deat"
    MISSING = "miss"
    INJURED = "inju"
    EVACUATED = "vac"
    RELOCATED = "reloc"
    ASSISTED = "assist"
    EMERGENCY_SHELTERED = "emshel"
    TEMPORARY_ACCOMMODATED = "tempacc"
    LONG_TERM_ACCOMMODATED = "longacc"
    IN_NEED = "need"
    TARGETED = "targ"
    DISRUPTED = "disr"
    LOSS_COST = "cost"
    HOMELESS = "homles"
    INTERNALLY_DISPLACED_PERSONS = "idp"
    REFUGEES_ASYLUM_SEEKERS_EXTERNALLY_DISPLACED_PERSONS = "extdisp"
    DISPLACED_PERSONS = "disp"
    ALERTSCORE = "alert"


class MontyImpactTypeLabel(Mapping):
    def __init__(self) -> None:
        self._data = {
            MontyImpactType.UNDEFINED: "Unspecified",
            MontyImpactType.UNAFFECTED: "Unaffected",
            MontyImpactType.DAMAGED: "Damaged",
            MontyImpactType.DESTROYED: "Destroyed",
            MontyImpactType.POTENTIALLY_DAMAGED: "Potentially Damaged",
            MontyImpactType.TOTAL_AFFECTED: "Total Affected",
            MontyImpactType.DIRECTLY_AFFECTED: "Directly Affected",
            MontyImpactType.INDIRECTLY_AFFECTED: "Indirectly Affected",
            MontyImpactType.DEATHS: "Deaths",
            MontyImpactType.MISSING: "Missing",
            MontyImpactType.INJURED: "Injured",
            MontyImpactType.EVACUATED: "Evacuated",
            MontyImpactType.RELOCATED: "Relocated",
            MontyImpactType.ASSISTED: "Assisted (Recieved Aid/Support)",
            MontyImpactType.EMERGENCY_SHELTERED: "Emergency Sheltered",
            MontyImpactType.TEMPORARY_ACCOMMODATED: "Temporary Accommodated",
            MontyImpactType.LONG_TERM_ACCOMMODATED: "Long-Term Accommodated",
            MontyImpactType.IN_NEED: "In Need",
            MontyImpactType.TARGETED: "Targeted",
            MontyImpactType.DISRUPTED: "Disrupted",
            MontyImpactType.LOSS_COST: "Loss (Cost)",
            MontyImpactType.HOMELESS: "Homeless",
            MontyImpactType.INTERNALLY_DISPLACED_PERSONS: "Internally Displaced Persons (IDPs)",
            MontyImpactType.REFUGEES_ASYLUM_SEEKERS_EXTERNALLY_DISPLACED_PERSONS: "Refugees, Asylum Seekers and Externally Displaced Persons",  # noqa: E501
            MontyImpactType.DISPLACED_PERSONS: "Displaced Persons (Internal & External)",
            MontyImpactType.ALERTSCORE: "Alertscore",
        }

    def __getitem__(self, key: MontyImpactType) -> str:
        return self._data[key]

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self) -> Iterator:
        return iter(self._data)


class HazardDetail(ABC):
    """Object that contains the details of the hazard.
    Preferably used only in a Hazard item. See the :stac-ext:`Monty Hazard Detail Object
    <monty#montyhazard_detail>` docs for details.
    """

    properties: dict[str, Any]

    def __init__(
        self,
        cluster: str,
        severity_value: float | None = None,
        severity_unit: str | None = None,
        severity_label: str | None = None,
        estimate_type: MontyEstimateType | None = None,
    ) -> None:
        self.properties = {}
        self.cluster = cluster
        if severity_value:
            self.severity_value = severity_value
        if severity_unit:
            self.severity_unit = severity_unit
        if severity_label:
            self.severity_label = severity_label
        if estimate_type:
            self.estimate_type = estimate_type

    @property
    def cluster(self) -> str:
        """The cluster of the hazard."""
        return get_required(
            self.properties.get(HAZDET_CLUSTER_PROP),
            ITEM_HAZARD_DETAIL_PROP,
            HAZDET_CLUSTER_PROP,
        )

    @cluster.setter
    def cluster(self, v: str) -> None:
        self.properties[HAZDET_CLUSTER_PROP] = v

    @property
    def severity_value(self) -> float:
        """The maximum value of the hazard."""
        return get_opt(self.properties.get(HAZDET_SEV_VALUE_PROP))

    @severity_value.setter
    def severity_value(self, v: float) -> None:
        self.properties[HAZDET_SEV_VALUE_PROP] = v

    @property
    def severity_unit(self) -> str:
        """The unit of the maximum value of the hazard."""
        return get_opt(self.properties.get(HAZDET_SEV_UNIT_PROP))

    @severity_unit.setter
    def severity_unit(self, v: str) -> None:
        self.properties[HAZDET_SEV_UNIT_PROP] = v

    @property
    def severity_label(self) -> str:
        """The label of the severity."""
        return get_opt(self.properties.get(HAZDET_SEV_LABEL_PROP))

    @severity_label.setter
    def severity_label(self, v: str) -> None:
        self.properties[HAZDET_SEV_LABEL_PROP] = v

    @property
    def estimate_type(self) -> MontyEstimateType:
        """The type of the estimate."""
        return get_opt(self.properties.get(HAZDET_ESTIMATE_TYPE_PROP))

    @estimate_type.setter
    def estimate_type(self, v: MontyEstimateType) -> None:
        self.properties[HAZDET_ESTIMATE_TYPE_PROP] = v

    def to_dict(self) -> dict[str, Any]:
        return self.properties

    @staticmethod
    def from_dict(d: dict[str, Any]) -> HazardDetail:
        cluster: str = get_required(d.get(HAZDET_CLUSTER_PROP), "hazard_detail", HAZDET_CLUSTER_PROP)

        return HazardDetail(cluster)


class ImpactDetail(ABC):
    """Object that contains the details of the impact.
    Preferably used only in a Impact item. See the :stac-ext:`Monty Impact Detail Object
    <monty#montyimpact_detail>` docs for details.
    """
    
    properties: dict[str, Any]

    def __init__(
        self,
        category: MontyImpactExposureCategory,
        type: MontyImpactType,
        value: float,
        unit: str | None = None,
        estimate_type: MontyEstimateType = None,
    ) -> None:
        self.properties = {}
        self.category = category
        self.type = type
        self.value = value
        if unit:
            self.unit = unit
        if estimate_type:
            self.estimate_type = estimate_type

    @property
    def category(self) -> str:
        """The cluster of the hazard."""
        return get_required(
            self.properties.get(IMPDET_CATEGORY_PROP),
            ITEM_IMPACT_DETAIL_PROP,
            IMPDET_CATEGORY_PROP,
        )

    @category.setter
    def category(self, v: str) -> None:
        self.properties[IMPDET_CATEGORY_PROP] = v

    @property
    def type(self) -> str:
        """The cluster of the impact."""
        return get_required(
            self.properties.get(IMPDET_TYPE_PROP),
            ITEM_IMPACT_DETAIL_PROP,
            IMPDET_TYPE_PROP,
        )

    @type.setter
    def type(self, v: str) -> None:
        self.properties[IMPDET_TYPE_PROP] = v

    @property
    def value(self) -> float:
        """The maximum value of the impact."""
        return get_required(
            self.properties.get(IMPDET_VALUE_PROP),
            ITEM_IMPACT_DETAIL_PROP,
            IMPDET_VALUE_PROP,
        )

    @value.setter
    def value(self, v: float) -> None:
        self.properties[IMPDET_VALUE_PROP] = v

    @property
    def unit(self) -> str:
        """The unit of the maximum value of the impact."""
        return get_opt(self.properties.get(IMPDET_UNIT_PROP))

    @unit.setter
    def unit(self, v: str) -> None:
        self.properties[IMPDET_UNIT_PROP] = v

    @property
    def estimate_type(self) -> MontyEstimateType:
        """The type of the estimate."""
        return get_opt(self.properties.get(IMPDET_ESTIMATE_TYPE_PROP))

    @estimate_type.setter
    def estimate_type(self, v: MontyEstimateType) -> None:
        self.properties[IMPDET_ESTIMATE_TYPE_PROP] = v

    def to_dict(self) -> dict[str, Any]:
        return self.properties

    @staticmethod
    def from_dict(d: dict[str, Any]) -> ImpactDetail:
        category: str = get_required(d.get(IMPDET_CATEGORY_PROP), "impact_detail", IMPDET_CATEGORY_PROP)
        type: str = get_required(d.get(IMPDET_TYPE_PROP), "impact_detail", IMPDET_TYPE_PROP)
        value: float = get_required(d.get(IMPDET_VALUE_PROP), "impact_detail", IMPDET_VALUE_PROP)

        return ImpactDetail(category, type, value)


class MontyExtension(
    Generic[T],
    PropertiesExtension,
    ExtensionManagementMixin[Union[pystac.Collection, pystac.Item]],
):
    """An abstract class that can be used to extend the properties of a
    :class:`~pystac.Collection`, :class:`~pystac.Item`, or :class:`~pystac.Asset` with
    properties from the :stac-ext:`Monty Extension <monty>`. This class is
    generic over the type of STAC Object to be extended (e.g. :class:`~pystac.Item`,
    :class:`~pystac.Asset`).

    To create a concrete instance of :class:`MontyExtension`, use the
    :meth:`MontyExtension.ext` method. For example:

    .. code-block:: python

       >>> item: pystac.Item = ...
       >>> dc_ext = MontyExtension.ext(item)
    """

    name: Literal["monty"] = "monty"
    pairing: Pairing = Pairing()

    def apply(
        self,
        correlation_id: str,
        country_codes: list[str],
        hazard_codes: list[str] | None = None,
    ) -> None:
        """Applies Monty Extension properties to the extended
        :class:`~pystac.Collection`, :class:`~pystac.Item`.

        Args:
            correlation_id : str
                The correlation ID.
            country_codes : list[str]
                The country codes.
            hazard_codes : list[str] | None
                The hazard codes.
        """
        self.correlation_id = correlation_id
        self.country_codes = country_codes
        self.hazard_codes = hazard_codes

    @property
    def correlation_id(self) -> str:
        """A unique correlation identifier for the event of the data."""
        result = get_required(self._get_property(ITEM_CORR_ID_PROP, str), self, ITEM_CORR_ID_PROP)
        return result

    @correlation_id.setter
    def correlation_id(self, v: str) -> None:
        self._set_property(ITEM_CORR_ID_PROP, v)

    @property
    def country_codes(self) -> list[str]:
        """A unique correlation identifier for the event of the data."""
        result = get_required(
            self._get_property(ITEM_COUNTRY_CODES_PROP, list[str]),
            self,
            ITEM_COUNTRY_CODES_PROP,
        )
        return result

    @country_codes.setter
    def country_codes(self, v: str) -> None:
        self._set_property(ITEM_COUNTRY_CODES_PROP, v)

    @property
    def hazard_codes(self) -> list[str] | None:
        """A list of hazard codes."""
        result = self._get_property(ITEM_HAZARD_CODES_PROP, list[str])

        return result

    @hazard_codes.setter
    def hazard_codes(self, v: list[str] | None) -> None:
        self._set_property(ITEM_HAZARD_CODES_PROP, v)

    @property
    def hazard_detail(self) -> HazardDetail | None:
        """The details of the hazard."""
        result = map_opt(self._get_property(ITEM_HAZARD_DETAIL_PROP, dict), HazardDetail)
        return result

    @hazard_detail.setter
    def hazard_detail(self, v: HazardDetail | None) -> None:
        self._set_property(ITEM_HAZARD_DETAIL_PROP, map_opt(lambda x: x.to_dict(), v))

    @property
    def impact_detail(self) -> ImpactDetail | None:
        """The details of the impact."""
        result = map_opt(self._get_property(ITEM_IMPACT_DETAIL_PROP, dict), ImpactDetail)
        return result

    @impact_detail.setter
    def impact_detail(self, v: ImpactDetail | None) -> None:
        self._set_property(ITEM_IMPACT_DETAIL_PROP, map_opt(lambda x: x.to_dict(), v))

    @property
    def episode_number(self) -> int:
        """The episode number."""
        return self.properties.get(ITEM_EPISODE_NUMBER_PROP, 0)

    @episode_number.setter
    def episode_number(self, v: int) -> None:
        self.properties[ITEM_EPISODE_NUMBER_PROP] = v

    def compute_and_set_correlation_id(self, hazard_profiles: HazardProfiles) -> None:
        correlation_id = self.pairing.generate_correlation_id(self.item, hazard_profiles)
        self.correlation_id = correlation_id

    @classmethod
    def get_schema_uri(cls) -> str:
        return SCHEMA_URI

    @classmethod
    def ext(cls, obj: T, add_if_missing: bool = False) -> MontyExtension[T]:
        """Extends the given STAC Object with properties from the :stac-ext:`Monty
        Extension <monty>`.

        This extension can be applied to instances of :class:`~pystac.Collection`,
        :class:`~pystac.Item`.

        Raises:

            pystac.ExtensionTypeError : If an invalid object type is passed.
        """
        if isinstance(obj, pystac.Collection):
            cls.ensure_has_extension(obj, add_if_missing)
            return cast(MontyExtension[T], CollectionMontyExtension(obj))
        if isinstance(obj, pystac.Item):
            cls.ensure_has_extension(obj, add_if_missing)
            return cast(MontyExtension[T], ItemMontyExtension(obj))
        elif isinstance(obj, item_assets.AssetDefinition):
            cls.ensure_owner_has_extension(obj, add_if_missing)
            return cast(MontyExtension[T], ItemAssetsMontyExtension(obj))
        else:
            raise pystac.ExtensionTypeError(cls._ext_error_message(obj))

    @staticmethod
    def enable_extension() -> None:
        pystac.extensions.ext.ItemExt.monty = property(lambda self: MontyExtension.ext(self))


class CollectionMontyExtension(MontyExtension[pystac.Collection]):
    """A concrete implementation of :class:`MontyExtension` on an
    :class:`~pystac.Collection` that extends the properties of the Item to include
    properties defined in the :stac-ext:`Monty Extension <monty>`.

    This class should generally not be instantiated directly. Instead, call
    :meth:`MontyExtension.ext` on an :class:`~pystac.Collection` to extend it.
    """

    collection: pystac.Collection
    properties: dict[str, Any]

    def __init__(self, collection: pystac.Collection):
        self.collection = collection
        self.properties = collection.extra_fields

    def __repr__(self) -> str:
        return f"<CollectionMontyExtension Item id={self.collection.id}>"


class ItemMontyExtension(MontyExtension[pystac.Item]):
    """A concrete implementation of :class:`MontyExtension` on an
    :class:`~pystac.Item` that extends the properties of the Item to include properties
    defined in the :stac-ext:`Monty Extension <monty>`.

    This class should generally not be instantiated directly. Instead, call
    :meth:`MontyExtension.ext` on an :class:`~pystac.Item` to extend it.
    """

    item: pystac.Item
    properties: dict[str, Any]

    def __init__(self, item: pystac.Item):
        self.item = item
        self.properties = item.properties

    def is_source_event(self) -> bool:
        """Indicates if the item is a source event."""
        return MontyRoles.SOURCE in self.item.properties["roles"] and MontyRoles.EVENT in self.item.properties["roles"]

    def is_source_hazard(self) -> bool:
        """Indicates if the item is a source hazard."""
        return MontyRoles.SOURCE in self.item.properties["roles"] and MontyRoles.HAZARD in self.item.properties["roles"]

    def is_source_impact(self) -> bool:
        """Indicates if the item is a source impact."""
        return MontyRoles.SOURCE in self.item.properties["roles"] and MontyRoles.IMPACT in self.item.properties["roles"]

    def __repr__(self) -> str:
        return f"<ItemMontyExtension Item id={self.item.id}>"


class ItemAssetsMontyExtension(MontyExtension[item_assets.AssetDefinition]):
    properties: dict[str, Any]
    asset_defn: item_assets.AssetDefinition

    def __init__(self, item_asset: item_assets.AssetDefinition):
        self.asset_defn = item_asset
        self.properties = item_asset.properties


class MontyExtensionHooks(ExtensionHooks):
    schema_uri: str = SCHEMA_URI
    prev_extension_ids = {
        "monty",
        # "https://stac-extensions.github.io/monty/v1.0.0/schema.json",
        # "https://stac-extensions.github.io/monty/v2.0.0/schema.json",
        # "https://stac-extensions.github.io/monty/v2.1.0/schema.json",
    }
    stac_object_types = {
        pystac.STACObjectType.COLLECTION,
        pystac.STACObjectType.ITEM,
    }


MONTY_EXTENSION_HOOKS: ExtensionHooks = MontyExtensionHooks()
