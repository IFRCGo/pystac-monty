# USGS

The USGS (United States Geological Survey) module provides functionality for working with [USGS Earthquake Catalog](https://earthquake.usgs.gov/) data. The module handles event details, ShakeMap hazard data, and PAGER impact estimates.

## Usage Example

Here's a complete example showing how to transform USGS earthquake data into STAC Items:

```python
import requests
from pystac_monty.sources.usgs import USGSTransformer, USGSDataSource

# 1. Fetch USGS data
# We need both event data and optional losses data
event_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/detail/us6000pi9w.geojson"
losses_url = "https://earthquake.usgs.gov/product/losspager/us6000pi9w/json/losses.json"

event_data = requests.get(event_url).text
losses_data = requests.get(losses_url).text  # Optional

# 2. Create USGS data source
data_source = USGSDataSource(
    source_url=event_url,
    data=event_data,
    losses_data=losses_data  # Optional - if not provided, no impact items created
)

# 3. Create transformer and transform data
transformer = USGSTransformer(data_source)
items = transformer.make_items()

# 4. The transformer creates three types of STAC items:
# - Source event item
# - Hazard item from ShakeMap 
# - Impact items from PAGER data (if losses data provided)

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
        print(f"  Earthquake Magnitude: {item.properties['eq:magnitude']} {item.properties['eq:magnitude_type']}")
    elif monty.is_source_hazard():
        print("Hazard Details:")
        print(f"  Severity: {monty.hazard_detail.severity_value} {monty.hazard_detail.severity_unit}")
    elif monty.is_source_impact():
        print("Impact Details:")
        print(f"  Category: {monty.impact_detail.category}")
        print(f"  Type: {monty.impact_detail.type}")
        print(f"  Value: {monty.impact_detail.value} {monty.impact_detail.unit}")
```

## Example Output

The transformer will create STAC items like this:

```json
{
  "type": "Feature",
  "stac_version": "1.0.0",
  "stac_extensions": [
    "https://ifrcgo.github.io/monty/v0.1.0/schema.json",
    "https://stac-extensions.github.io/earthquake/v1.0.0/schema.json"
  ],
  "id": "usgs-event-us6000pi9w",
  "properties": {
    "title": "M 7.1 - Southern Tibetan Plateau",
    "description": "Ground motion and intensity map for M7.1 earthquake",
    "datetime": "2025-01-07T01:05:16Z",
    "roles": ["source", "event"],
    "monty:episode_number": 1,
    "monty:hazard_codes": ["GH0004"],
    "monty:country_codes": ["CHN"],
    "eq:magnitude": 7.1,
    "eq:magnitude_type": "mww",
    "eq:depth": 10,
    "eq:status": "reviewed",
    "eq:tsunami": false
  },
  "geometry": {
    "type": "Point",
    "coordinates": [87.3608, 28.639]
  },
  "links": [...],
  "assets": {
    "source": {
      "href": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/detail/us6000pi9w.geojson",
      "type": "application/geo+json",
      "title": "USGS GeoJSON Source"
    }
  }
}
```

## USGSTransformer

The USGSTransformer class handles transformation of USGS earthquake data into STAC Items with the Monty extension.

```python
class USGSTransformer:
    """
    Transforms USGS earthquake data into STAC Items.
    Creates event, hazard (ShakeMap), and impact (PAGER) items.
    """
    def __init__(self, data: USGSDataSource) -> None:
        """
        Initialize transformer with USGS data source.
        
        Args:
            data: USGSDataSource containing event data and optional losses data
        """
        
    def make_items(self) -> list[Item]:
        """
        Transform USGS data into STAC Items.
        Creates source event, hazard, and impact items if losses data available.
        
        Returns:
            list[Item]: List of STAC Items with Monty extension
        """
```

The transformer creates three types of items:

1. Event item from basic earthquake data including:
   - Location, magnitude, depth
   - Event metadata and timestamps
   - Links to additional USGS resources
   - Earthquake extension fields (eq:magnitude, eq:depth, etc.)

2. Hazard item from ShakeMap data including:
   - Ground motion intensity information
   - ShakeMap assets (intensity maps, contours, grids)
   - Hazard severity details

3. Impact items from PAGER data (if provided) including:
   - Estimated fatalities
   - Estimated economic losses
   - Impact type, category and value
   - PAGER-specific assets

## USGSDataSource

Wrapper class for USGS data that handles both event data and optional losses data.

```python
class USGSDataSource(MontyDataSource):
    """
    USGS data source that can handle both event detail and losses data.
    
    Args:
        source_url: URL where the event data was retrieved from
        data: Event detail data as JSON string
        losses_data: Optional PAGER losses data as JSON string
    """
    def __init__(self, source_url: str, data: str, losses_data: Optional[str] = None):
        super().__init__(source_url, data)
        self.data = json.loads(data)
        self.losses_data = json.loads(losses_data) if losses_data else None
        
    def get_data(self) -> dict:
        """Get the event detail data."""
        return self.data
        
    def get_losses_data(self) -> Optional[dict]:
        """Get the PAGER losses data if available."""
        return self.losses_data
```

The data source handles:
- GeoJSON event data from USGS detail API
- Optional PAGER losses data in JSON format
- Automatic JSON parsing and validation
- Access to both event and losses data through clean interface