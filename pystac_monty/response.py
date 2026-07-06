"""Builder and query helpers for Monty Response STAC items.

See the `Response best practices
<https://github.com/IFRCGo/monty-stac-extension/blob/main/docs/model/response-best-practices.md>`_
doc in the vendored `monty-stac-extension` submodule for the extension-layering guidance
(``disaster:`` for Charter VAP items, ``processing:`` for value-added products) that this
module does not attempt to replicate: layer those extensions directly on the item returned
by :func:`build_response_item` instead of adding fields to ``response_detail``.
"""

from __future__ import annotations

from datetime import datetime as Datetime
from typing import Any, Iterable

from pystac import Item, Link

from pystac_monty.extension import (
    MontyExtension,
    MontyMethodology,
    MontyResponseStatus,
    MontyRoles,
    ResponseDetail,
)
from pystac_monty.validators.response import ResponseDetailValidator


def build_response_item(
    id: str,
    geometry: dict[str, Any] | None,
    bbox: list[float] | None,
    datetime: Datetime | None,
    correlation_id: str,
    country_codes: list[str],
    type: str,
    *,
    hazard_codes: list[str] | None = None,
    source_id: str | None = None,
    status: MontyResponseStatus | None = None,
    monitoring_number: int | None = None,
    producer: str | None = None,
    methodology: MontyMethodology | None = None,
    sendai_targets: list[str] | None = None,
    sectors: list[str] | None = None,
    properties: dict[str, Any] | None = None,
    prev_response_item: Item | None = None,
    related_response_items: Iterable[Item] | None = None,
) -> Item:
    """Builds a Monty Response STAC item.

    Validates the ``response_detail`` fields against the v1.3.0 constraints (mandatory
    ``type`` + regex, ``status``/``methodology`` enums, unique ``sendai_targets`` subset,
    unknown-key rejection) before wiring ``roles: ["response"]`` and the
    ``monty:corr_id``/``country_codes``/``hazard_codes`` properties onto the item.

    ``prev_response_item`` adds a ``rel: prev`` link to the prior iteration this item
    monitors. ``related_response_items`` adds bidirectional ``rel: related``
    (``roles: ["response"]``) links, e.g. for CEMS<->Charter co-activation.
    """
    response_detail_fields = ResponseDetailValidator(
        type=type,
        source_id=source_id,
        status=status,
        monitoring_number=monitoring_number,
        producer=producer,
        methodology=methodology,
        sendai_targets=sendai_targets,
        sectors=sectors,
    ).model_dump(exclude_none=True)

    item_properties = dict(properties or {})
    roles = list(item_properties.get("roles", []))
    if MontyRoles.RESPONSE not in roles:
        roles.append(MontyRoles.RESPONSE)
    item_properties["roles"] = roles

    item = Item(id=id, geometry=geometry, bbox=bbox, datetime=datetime, properties=item_properties)

    MontyExtension.add_to(item)
    monty = MontyExtension.ext(item)
    monty.correlation_id = correlation_id
    monty.country_codes = country_codes
    if hazard_codes:
        monty.hazard_codes = hazard_codes
    monty.response_detail = ResponseDetail(**response_detail_fields)

    if prev_response_item is not None:
        link_monitoring_update(item, prev_response_item)

    for related_item in related_response_items or []:
        link_related_response(item, related_item)

    return item


def link_related_response(item: Item, related_item: Item) -> None:
    """Adds a bidirectional ``rel: related`` link (``roles: ["response"]``) between two
    Response items, e.g. a CEMS activation and its corresponding Charter VAP co-activation,
    or two sibling response products on the same activation."""
    item.add_link(
        Link(
            rel="related",
            target=related_item,
            media_type="application/geo+json",
            extra_fields={"roles": [MontyRoles.RESPONSE]},
        )
    )
    related_item.add_link(
        Link(
            rel="related",
            target=item,
            media_type="application/geo+json",
            extra_fields={"roles": [MontyRoles.RESPONSE]},
        )
    )


def link_monitoring_update(item: Item, prev_item: Item) -> None:
    """Adds a ``rel: prev`` link from a monitoring-update Response item to the prior
    iteration it monitors."""
    item.add_link(Link(rel="prev", target=prev_item, media_type="application/geo+json"))


def filter_response_items(
    items: Iterable[Item],
    *,
    type: str | None = None,
    producer: str | None = None,
    methodology: str | None = None,
    status: str | None = None,
) -> list[Item]:
    """Filters Response items by their ``monty:response_detail`` fields. Items without a
    ``response_detail`` are excluded."""
    result = []
    for item in items:
        detail = MontyExtension.ext(item).response_detail
        if detail is None:
            continue
        if type is not None and detail.type != type:
            continue
        if producer is not None and detail.producer != producer:
            continue
        if methodology is not None and detail.methodology != methodology:
            continue
        if status is not None and detail.status != status:
            continue
        result.append(item)
    return result
