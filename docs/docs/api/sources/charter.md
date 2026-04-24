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

# 4. The transformer creates two types of STAC items:
# - Event item (one per activation)
# - Hazard items (one per disaster type per area)

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
    "roles": ["source", "event"],
    "monty:hazard_codes": ["MH0600", "FL", "nat-hyd-flo-flo"],
    "monty:country_codes": ["PAK"],
    "monty:correlation_id": "...",
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
    "roles": ["source", "hazard"],
    "monty:hazard_codes": ["MH0600", "FL", "nat-hyd-flo-flo"],
    "monty:country_codes": ["PAK"],
    "monty:correlation_id": "...",
    "monty:hazard_detail": {
      "estimate_type": "primary",
      "severity_value": 10.0,
      "severity_unit": "km",
      "severity_label": "Area radius"
    },
    "charter:area_priority": 1,
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

## Item Relationships

The transformer creates relationships between items using STAC links:

- **Event → Hazard**: Event items have `related` links to their hazard items
- **Hazard → Event**: Hazard items have:
  - `derived_from` link to their parent event item (indicates the hazard was derived from the event)
  - `related` link back to the event item (bidirectional relationship)
- **Correlation ID**: All items from the same activation share the same `monty:correlation_id`

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
    Creates one event item and multiple hazard items (one per disaster type per area).
    """
    def __init__(self, data_source: CharterDataSource) -> None:
        """
        Initialize transformer with Charter data source.

        Args:
            data_source: CharterDataSource containing activation and areas data
        """

    def get_stac_items(self) -> Generator[Item, None, None]:
        """
        Generate STAC items from Charter activation data.
        Yields event item followed by hazard items.

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

## Static batch export

`convert_charter_activations` writes ``charter-events``, ``charter-hazards``, and an empty
``charter-response`` collection (no response items yet — acquisitions/datasets/VAPs need
partner data access). Items are registered on ``charter-events`` / ``charter-hazards`` via
PySTAC ``set_collection`` like other Monty sources.

Input layout: any directory containing ``act-*-activation.json`` files and matching
``act-*-area-*.json`` sidecars, including
``monty-stac-extension/docs/model/sources/Charter`` in a full submodule checkout.

## CPE Status Mapping

Charter CPE (Common Processing Environment) status is mapped to estimate types:

| CPE Status | Estimate Type |
|------------|---------------|
| notificationNew | primary |
| readyToDeliver | secondary |
| readyToArchive | secondary |
