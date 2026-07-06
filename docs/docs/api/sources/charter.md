# Charter

The Charter module provides functionality for working with International Charter on Space and Major Disasters activation data.

## Usage Example

Here's a complete example showing how to transform Charter activation data into STAC Items:

```python
import requests
from pystac_monty.sources.charter import CharterTransformer, CharterDataSource
from pystac_monty.sources.common import GenericDataSource, Memory, DataType
from pystac_monty.extension import MontyExtension

# 1. Fetch Charter activation data (includes areas)
activation_url = "https://supervisor.disasterscharter.org/api/activations/act-123"
activation_data = requests.get(activation_url).json()

# 2. Create Charter data source
charter_source = CharterDataSource(
    data=GenericDataSource(
        source_url=activation_url,
        input_data=Memory(content=activation_data, data_type=DataType.MEMORY)
    )
)

# 3. Create transformer and generate items
transformer = CharterTransformer(charter_source)
items = list(transformer.get_stac_items())

# 4. The transformer creates up to three types of STAC items:
# - Event item (one per activation)
# - Hazard items (one per disaster type per area)
# - Response items (one per Value Added Product, when VAP sidecars are present)

# Example: Print item details
for item in items:
    print(f"\nItem ID: {item.id}")
    print(f"Roles: {item.properties['roles']}")

    monty = MontyExtension.ext(item)
    if monty.is_source_event():
        print("Event Details:")
        print(f"  Hazard Codes: {monty.hazard_codes}")
        print(f"  Country: {monty.country_codes}")
    elif monty.is_source_hazard():
        print("Hazard Details:")
        print(f"  Estimate Type: {monty.hazard_detail.estimate_type}")
        if monty.hazard_detail.severity_value:
            print(f"  Severity: {monty.hazard_detail.severity_value} {monty.hazard_detail.severity_unit}")
```

## Multi-Hazard Support

Charter activations can involve multiple disaster types. The transformer creates separate hazard items for each type:

```python
# Activation with disaster:type = ["flood", "landslide"]
# Creates:
# - 1 event item
# - 2 hazard items per area (one for flood, one for landslide)
```

## Example Output

### Event Item

The transformed event item will look like this:

```json
{
  "type": "Feature",
  "stac_version": "1.0.0",
  "id": "charter-event-123",
  "properties": {
    "title": "Charter Activation 123",
    "description": "Emergency activation for flood event",
    "datetime": "2024-01-15T00:00:00Z",
    "roles": ["event", "source"],
    "monty:hazard_codes": ["MH0600", "FL", "nat-hyd-flo-flo"],
    "monty:country_codes": ["PAK"],
    "monty:corr_id": "...",
    "keywords": ["flood", "PAK"]
  },
  "geometry": {
    "type": "Point",
    "coordinates": [71.5, 34.0]
  },
  "links": [
    {
      "rel": "via",
      "href": "https://supervisor.disasterscharter.org/api/activations/act-123",
      "type": "application/json"
    },
    {
      "rel": "related",
      "href": "...",
      "type": "application/geo+json",
      "roles": ["hazard"]
    }
  ]
}
```

### Hazard Item

Hazard items include `derived_from` links to their parent event item:

```json
{
  "type": "Feature",
  "stac_version": "1.0.0",
  "id": "charter-hazard-123-area1-flood",
  "properties": {
    "title": "Affected Area 1",
    "description": "Radius (km): 10.0\nPriority: 1",
    "datetime": "2024-01-15T00:00:00Z",
    "roles": ["hazard", "source"],
    "monty:hazard_codes": ["MH0600", "FL", "nat-hyd-flo-flo"],
    "monty:country_codes": ["PAK"],
    "monty:corr_id": "...",
    "monty:hazard_detail": {
      "estimate_type": "primary",
      "severity_value": 10.0,
      "severity_unit": "km",
      "severity_label": "Area radius"
    },
    "keywords": ["flood", "PAK"]
  },
  "geometry": {
    "type": "Polygon",
    "coordinates": [[[71.0, 33.5], [72.0, 33.5], [72.0, 34.5], [71.0, 34.5], [71.0, 33.5]]]
  },
  "links": [
    {
      "rel": "derived_from",
      "href": "../charter-events/charter-event-123.json",
      "type": "application/json",
      "title": "Parent Charter Event"
    },
    {
      "rel": "related",
      "href": "...",
      "type": "application/geo+json",
      "roles": ["event"]
    }
  ]
}
```

### Response Item

When Value Added Product (VAP) sidecars (`act-*-vap-*.json`) accompany an activation, the
transformer also emits one response item per VAP, carrying `monty:response_detail`:

```json
{
  "type": "Feature",
  "stac_version": "1.0.0",
  "id": "charter-response-123-1144-1",
  "stac_extensions": [
    "https://ifrcgo.org/monty-stac-extension/v1.3.0/schema.json",
    "https://terradue.github.io/stac-extensions-disaster/v1.1.0/schema.json"
  ],
  "properties": {
    "title": "Preliminary satellite-derived damage assessment ...",
    "disaster:class": "vap",
    "disaster:resolution_class": "VHR",
    "roles": ["response", "source"],
    "monty:hazard_codes": ["GH0101", "EQ", "nat-geo-ear-gro"],
    "monty:country_codes": ["AFG"],
    "monty:corr_id": "...",
    "monty:response_detail": {
      "type": "eo-gra",
      "source_id": "1144-1",
      "methodology": "human_interpreted",
      "sendai_targets": ["C", "D"],
      "producer": "Airbus"
    },
    "keywords": ["AFG", "Earthquake", "ValueAddedProduct"]
  },
  "links": [
    {"rel": "related", "href": "...", "type": "application/geo+json", "roles": ["event"]},
    {"rel": "related", "href": "...", "type": "application/geo+json", "roles": ["hazard"]},
    {"rel": "derived_from", "href": "https://disasterscharter.org/activations/...", "type": "text/html"}
  ]
}
```

The `type`, `producer`, and `disaster:resolution_class` fields are inferred heuristically from
the VAP title/description and copyright text (interim mapping).

## Item Relationships

The transformer creates relationships between items using STAC links:

- **Event → Hazard**: Event items have `related` links to their hazard items
- **Hazard → Event**: Hazard items have:
  - `derived_from` link to their parent event item (indicates the hazard was derived from the event)
  - `related` link back to the event item (bidirectional relationship)
- **Event → Response** / **Response → Event/Hazard**: Response items have `related` links back to
  the event and each hazard item, plus a `derived_from` link to the activation web page.
- **Correlation ID**: All items from the same activation share the same `monty:corr_id`

### Hazard Code Canonicalization

Hazard codes are automatically canonicalized to follow the standard order:
1. UNDRR-ISC 2025 code (e.g., `MH0600`)
2. GLIDE code (e.g., `FL`)
3. EM-DAT code (e.g., `nat-hyd-flo-flo`)

This ensures consistency across all Event and Hazard items.

## CharterTransformer

The CharterTransformer class handles the transformation of Charter activation data into STAC Items with the Monty extension.

```python
class CharterTransformer:
    """
    Transforms Charter activation data into STAC Items.
    Creates one event item, multiple hazard items (one per disaster type per area),
    and one response item per VAP sidecar when present.
    """
    def __init__(self, data_source: CharterDataSource, geocoder: MontyGeoCoder | None = None) -> None:
        """
        Initialize transformer with Charter data source.

        Args:
            data_source: CharterDataSource containing activation, area, and optional VAP data
            geocoder: Optional MontyGeoCoder (unused by Charter today)
        """

    def get_stac_items(self) -> Generator[Item, None, None]:
        """
        Generate STAC items from Charter activation data.
        Yields the event item, then hazard items, then response items.

        Yields:
            Item: STAC Items with Monty extension
        """
```

## CharterDataSource

The CharterDataSource class encapsulates Charter activation and area data.

```python
class CharterDataSource:
    """
    Charter data source containing activation and area data.
    Supports both in-memory and file-based data.
    """
    def __init__(self, data: GenericDataSource, eoapi_url: Optional[str] = None):
        """
        Initialize Charter data source.

        Args:
            data: GenericDataSource with activation data (including areas)
            eoapi_url: Optional EOAPI URL for item hrefs
        """
```

## Hazard Code Mapping

Charter disaster types are mapped to UNDRR-ISC 2025, EM-DAT, and GLIDE codes:

| Charter Type | UNDRR-2025 | EM-DAT | GLIDE |
|--------------|------------|--------|-------|
| flood | MH0600 | nat-hyd-flo-flo | FL |
| earthquake | GH0101 | nat-geo-ear-gro | EQ |
| cyclone | MH0403 | nat-met-sto-tro | TC |
| fire | MH1301 | nat-cli-wil-for | WF |

See `CHARTER_HAZARD_CODES` in the source for the complete mapping.

## Iterating a directory of activations

`iter_charter_stac_items(source_dir)` yields the full STAC item graph for every activation in a
directory, wiring up areas and VAP sidecars automatically. Items are registered on the
``charter-events`` / ``charter-hazards`` / ``charter-response`` collections via PySTAC
``set_collection`` like other Monty sources.

Input layout: a directory containing ``act-*-activation.json`` files and matching
``act-*-area-*.json`` and optional ``act-*-vap-*.json`` sidecars, e.g.
``monty-stac-extension/docs/model/sources/Charter`` in a full submodule checkout.

```python
from pathlib import Path
from pystac_monty.sources.charter import iter_charter_stac_items

items = list(iter_charter_stac_items(Path("monty-stac-extension/docs/model/sources/Charter")))
```

## CPE Status Mapping

Charter CPE (Common Processing Environment) status is mapped to estimate types:

| CPE Status | Estimate Type |
|------------|---------------|
| notificationNew | primary |
| readyToDeliver | secondary |
| readyToArchive | secondary |
