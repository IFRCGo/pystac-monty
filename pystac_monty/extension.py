"""Implements the :stac-ext:`Monty Extension <monty>`."""

from __future__ import annotations

from abc import ABC
from typing import Any, Generic, Literal, TypeVar, Union, cast

import pystac
import pystac.extensions
import pystac.extensions.ext
from pystac.extensions import item_assets
from pystac.extensions.base import ExtensionManagementMixin, PropertiesExtension
from pystac.extensions.hooks import ExtensionHooks
from pystac.utils import StringEnum, get_opt, get_required, map_opt

from pystac_monty.paring import Pairing

__version__ = "0.1.0"

T = TypeVar(
    "T", pystac.Collection, pystac.Item, pystac.Asset, item_assets.AssetDefinition
)

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
HAZDET_CLUSTER_PROP = PREFIX + "cluster"
HAZDET_MAX_VALUE_PROP = PREFIX + "max_value"
HAZDET_MAX_UNIT_PROP = PREFIX + "max_unit"
HAZDET_ESTIMATE_TYPE_PROP = PREFIX + "estimate_type"

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


class HazardDetail(ABC):
    """Object that contains the details of the hazard.
    Preferably used only in a Hazard item. See the :stac-ext:`Monty Hazard Detail Object
    <monty#montyhazard_detail>` docs for details.
    """

    properties: dict[str, Any]

    def __init__(self, properties: dict[str, Any]) -> None:
        self.properties = properties

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
    def max_value(self) -> float:
        """The maximum value of the hazard."""
        return get_opt(self.properties.get(HAZDET_MAX_VALUE_PROP))

    @max_value.setter
    def max_value(self, v: float) -> None:
        self.properties[HAZDET_MAX_VALUE_PROP] = v

    @property
    def max_unit(self) -> str:
        """The unit of the maximum value of the hazard."""
        return get_opt(self.properties.get(HAZDET_MAX_UNIT_PROP))

    @max_unit.setter
    def max_unit(self, v: str) -> None:
        self.properties[HAZDET_MAX_UNIT_PROP] = v

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
        cluster: str = get_required(
            d.get(HAZDET_CLUSTER_PROP), "hazard_detail", HAZDET_CLUSTER_PROP
        )

        return HazardDetail(d)


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
        result = get_required(
            self._get_property(ITEM_CORR_ID_PROP, str), self, ITEM_CORR_ID_PROP
        )
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
        result = map_opt(
            self._get_property(ITEM_HAZARD_DETAIL_PROP, dict), HazardDetail
        )
        return result

    @hazard_detail.setter
    def hazard_detail(self, v: HazardDetail | None) -> None:
        self._set_property(ITEM_HAZARD_DETAIL_PROP, map_opt(v, lambda x: x.to_dict()))
        
    @property
    def episode_number(self) -> int:
        """The episode number."""
        return self.properties.get(ITEM_EPISODE_NUMBER_PROP, 0)

    @episode_number.setter
    def episode_number(self, v: int) -> None:
        self.properties[ITEM_EPISODE_NUMBER_PROP] = v

    def compute_and_set_correlation_id(self) -> None:
        correlation_id = self.pairing.generate_correlation_id(self.item)
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
        pystac.extensions.ext.ItemExt.monty = property(
            lambda self: MontyExtension.ext(self)
        )


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
        return (
            MontyRoles.SOURCE in self.item.properties["roles"] and
            MontyRoles.EVENT in self.item.properties["roles"]
        )

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
