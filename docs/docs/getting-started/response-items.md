# Response Items

Response items (`roles: ["response"]`) describe a response product — an EO delineation/grading
product, a humanitarian sector response, or a financial allocation. See the [Response best
practices](https://github.com/IFRCGo/monty-stac-extension/blob/main/docs/model/response-best-practices.md)
doc in the vendored `monty-stac-extension` submodule for the full extension-layering rationale.

The governing principle: **layer other STAC extensions instead of copying their fields into
`monty:response_detail`**. `monty:response_detail` only carries the fields that no other declared
extension already covers.

## (a) A bare Response item

```python
from datetime import datetime, timezone

from pystac_monty.extension import MontyMethodology, MontyResponseStatus
from pystac_monty.response import build_response_item

item = build_response_item(
    id="EMSR744-GRA",
    geometry={"type": "Polygon", "coordinates": [[[-3.8, 40.3], [-3.6, 40.3], [-3.6, 40.5], [-3.8, 40.5], [-3.8, 40.3]]]},
    bbox=[-3.8, 40.3, -3.6, 40.5],
    datetime=datetime(2026, 6, 15, tzinfo=timezone.utc),
    correlation_id="20260615T000000Z-ESP-FL-001-GCDB",
    country_codes=["ESP"],
    hazard_codes=["FL"],
    type="eo-gra",
    source_id="EMSR744",
    status=MontyResponseStatus.PUBLISHED,
    producer="JRC",
    methodology=MontyMethodology.HUMAN_INTERPRETED,
    sendai_targets=["C", "D"],
)
```

`build_response_item` validates `response_detail` (mandatory `type` + regex, `status`/`methodology`
enums, unique Sendai targets, unknown-key rejection) before setting it, and wires
`roles: ["response"]`, `monty:corr_id`, `monty:country_codes`, `monty:hazard_codes`.

### Monitoring updates and Charter co-activation links

```python
# Monitoring update: rel: prev to the prior iteration
monitoring_item = build_response_item(
    ..., monitoring_number=2, prev_response_item=item,
)

# CEMS <-> Charter co-activation: bidirectional rel: related, roles: ["response"]
charter_item = build_response_item(..., type="eo-vap", producer="Airbus")
cems_item = build_response_item(..., related_response_items=[charter_item])
```

## (b) Response + `disaster:` layering, for a Charter VAP

International Charter VAP items MUST declare the [`disaster:`
extension](https://terradue.github.io/stac-extensions-disaster/) alongside `monty:` rather than
duplicating its fields under `response_detail`. In particular, `disaster:activation_status` replaces
`response_detail.status`, and `disaster:activation_id`/`disaster:resolution_class` are not mirrored
into `monty:response_detail` either.

```python
charter_item = build_response_item(
    id="ACT-849-VAP",
    geometry=...,
    bbox=...,
    datetime=...,
    correlation_id="20260615T000000Z-ESP-FL-001-GCDB",
    country_codes=["ESP"],
    hazard_codes=["FL"],
    type="eo-vap",
    source_id="ACT-849",
    producer="Airbus",
    methodology=MontyMethodology.HUMAN_INTERPRETED,
    sendai_targets=["D", "G"],
    # status is intentionally omitted: carried via disaster:activation_status instead
)

charter_item.stac_extensions.append("https://terradue.github.io/stac-extensions-disaster/v1.1.0/schema.json")
charter_item.properties.update(
    {
        "disaster:class": "vap",
        "disaster:activation_id": 849,
        "disaster:call_ids": [1421],
        "disaster:activation_status": "open",
        "disaster:resolution_class": "VHR",
        "disaster:types": ["flood"],
        "disaster:country": "ESP",
    }
)
```

## (c) Response + `processing:` layering, for a value-added product

CEMS/UNOSAT-sourced EO products SHOULD declare the
[`processing:`](https://github.com/stac-extensions/processing) extension to describe their
processing chain — this stays on the item alongside `monty:response_detail`, it is not a
`response_detail` field.

```python
item.stac_extensions.append("https://stac-extensions.github.io/processing/v1.2.0/schema.json")
item.properties.update(
    {
        "processing:level": "L3",
        "processing:lineage": "Sentinel-1 GRD -> flood-mask classifier -> vector cleanup",
    }
)
```

## Filtering Response items

```python
from pystac_monty.response import filter_response_items

grading_products = filter_response_items(all_response_items, type="eo-gra", producer="JRC")
```
