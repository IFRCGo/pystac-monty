# GDACS

The GDACS (Global Disaster Alert and Coordination System) module provides functionality for working with GDACS data.

## Usage Example

Here's a complete example showing how to transform GDACS data into STAC Items:

```python
import requests
from pystac_monty.sources.gdacs import GDACSTransformer, GDACSDataSource, GDACSDataSourceType

# 1. Fetch GDACS data
# We need both event data and geometry data
event_url = "https://www.gdacs.org/gdacsapi/api/events/geteventdata?eventtype=FL&eventid=1102983&episodeid=1"
geometry_url = "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=FL&eventid=1102983&episodeid=1"

# Create GDACS data sources
gdacs_data_sources = [
    GDACSDataSource(
        event_url,
        requests.get(event_url).text,
        GDACSDataSourceType.EVENT
    ),
    GDACSDataSource(
        geometry_url,
        requests.get(geometry_url).text,
        GDACSDataSourceType.GEOMETRY
    )
]

# 2. Create transformer and transform data
transformer = GDACSTransformer(gdacs_data_sources)
items = transformer.make_items()

# 3. The transformer creates three types of STAC items:
# - Source event item
# - Hazard event item
# - Impact items (from Sendai indicators if present)

# Example: Print details of each item
for item in items:
    print(f"\nItem ID: {item.id}")
    print(f"Type: {item.properties['roles']}")

    # Access Monty extension fields
    monty = MontyExtension.ext(item)
    if monty.is_source_event():
        print("Event Details:")
        print(f"  Episode Number: {monty.episode_number}")
        print(f"  Hazard Codes: {monty.hazard_codes}")
        print(f"  Country Codes: {monty.country_codes}")
    elif monty.is_source_hazard():
        print("Hazard Details:")
        print(f"  Severity: {monty.hazard_detail.severity_value} {monty.hazard_detail.severity_unit}")
        print(f"  Label: {monty.hazard_detail.severity_label}")
    elif monty.is_source_impact():
        print("Impact Details:")
        print(f"  Category: {monty.impact_detail.category}")
        print(f"  Type: {monty.impact_detail.type}")
        print(f"  Value: {monty.impact_detail.value} {monty.impact_detail.unit}")
```

## Example Output

The transformed items will look like this:

```json
{
  "type": "Feature",
  "stac_version": "1.0.0",
  "id": "gdacs-event-1102983-1",
  "properties": {
    "title": "Flood in Spain",
    "description": "A severe flood event affected parts of Spain...",
    "start_datetime": "2023-09-03T00:00:00Z",
    "end_datetime": "2023-09-04T00:00:00Z",
    "roles": ["source", "event"],
    "monty:episode_number": 1,
    "monty:hazard_codes": ["FL"],
    "monty:country_codes": ["ESP"]
  },
  "geometry": {
    "type": "Point",
    "coordinates": [-0.48, 38.34]
  },
  "links": [...],
  "assets": {
    "icon": {
      "href": "https://www.gdacs.org/images/gdacs_icons/FL_red_arrow.png",
      "title": "Icon"
    },
    "report": {
      "href": "https://www.gdacs.org/report.aspx?eventtype=FL&eventid=1102983",
      "title": "Report"
    }
  }
}
```

## GDACSTransformer

The GDACSTransformer class handles the transformation of GDACS data into STAC Items with the Monty extension.

```python
class GDACSTransformer:
    """
    Transforms GDACS event data into STAC Items.
    Requires both event data and geometry data from GDACS API.
    """
    def __init__(self, data: list[GDACSDataSource]) -> None:
        """
        Initialize transformer with GDACS data sources.

        Args:
            data: List of GDACSDataSource objects containing event and geometry data
        """

    def make_items(self) -> list[Item]:
        """
        Transform GDACS data into STAC Items.
        Creates source event, hazard, and impact items.

        Returns:
            list[Item]: List of STAC Items with Monty extension
        """
```

The transformer requires two types of GDACS data:

1. Event data (`GDACSDataSourceType.EVENT`): Basic event information including location, dates, and impact data
2. Geometry data (`GDACSDataSourceType.GEOMETRY`): Detailed geometry information for hazard extent

For each GDACS event, the transformer creates:

1. A source event item containing basic event information
2. A hazard item containing severity and extent information
3. Zero or more impact items containing Sendai indicator data if available

Each item includes the Monty extension fields appropriate for its type.
