"""Builder helpers for pairing Monty Impact STAC items with a Response item."""

from __future__ import annotations

from typing import Any, Mapping

from pystac import Item, Link

from pystac_monty.extension import (
    ImpactDetail,
    MontyEstimateType,
    MontyExtension,
    MontyImpactExposureCategory,
    MontyImpactType,
    MontyRoles,
)


def build_impact_from_response(
    response_item: Item,
    thematic: MontyImpactExposureCategory,
    type: MontyImpactType,
    value: float,
    unit: str | None = None,
    estimate_type: MontyEstimateType | None = None,
    properties: dict[str, Any] | None = None,
    id: str | None = None,
) -> Item:
    """Builds a Monty Impact item carrying a single thematic figure, paired to
    ``response_item``.
    """
    monty_response = MontyExtension.ext(response_item)

    item_properties = dict(properties or {})
    roles = list(item_properties.get("roles", []))
    if MontyRoles.IMPACT not in roles:
        roles.append(MontyRoles.IMPACT)
    item_properties["roles"] = roles

    if id is None:
        response_detail = monty_response.response_detail
        source_id = response_detail.properties.get("source_id") if response_detail else None
        if not source_id:
            try:
                base_id = "-".join(response_item.id.split("-")[1:-1])
            except (IndexError, ValueError):
                base_id = response_item.id
        id = f"impact-{source_id or base_id}-{thematic}-{type}"

    item = Item(
        id=id,
        geometry=response_item.geometry,
        bbox=response_item.bbox,
        datetime=response_item.datetime,
        properties=item_properties,
    )

    MontyExtension.add_to(item)
    monty = MontyExtension.ext(item)
    monty.correlation_id = monty_response.correlation_id
    monty.country_codes = list(monty_response.country_codes)
    if monty_response.hazard_codes:
        monty.hazard_codes = list(monty_response.hazard_codes)
    monty.impact_detail = ImpactDetail(
        category=thematic,
        type=type,
        value=value,
        unit=unit,
        estimate_type=estimate_type,
    )

    item.set_self_href(f"./{item.id}.json")

    link_derived_from_response(item, response_item)
    link_related_for_response(response_item, item)

    return item


def link_derived_from_response(item: Item, response_item: Item) -> None:
    """Adds a ``rel: derived_from`` link (``roles: ["response"]``) from an Impact item to
    the Response item it was derived from."""
    item.add_link(
        Link(
            rel="derived_from",
            target=response_item,
            media_type="application/geo+json",
            extra_fields={"roles": [MontyRoles.RESPONSE]},
        )
    )


def link_related_for_response(response_item: Item, item: Item) -> None:
    """Adds a ``rel: related`` link (``roles: ["impact"]``) for the response item"""
    response_item.add_link(
        Link(
            rel="related",
            target=item,
            media_type="application/geo+json",
            extra_fields={"roles": [MontyRoles.IMPACT]},
        )
    )


def build_impacts_from_response(
    response_item: Item,
    thematics: Mapping[MontyImpactExposureCategory, float],
    type: MontyImpactType,
    unit: str | None = None,
    estimate_type: MontyEstimateType | None = None,
) -> list[Item]:
    """Build impact item per thematic category."""
    return [
        build_impact_from_response(
            response_item=response_item,
            thematic=thematic,
            type=type,
            value=value,
            unit=unit,
            estimate_type=estimate_type,
        )
        for thematic, value in thematics.items()
    ]
