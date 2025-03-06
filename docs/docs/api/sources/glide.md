# GLIDE

The GLIDE (GLobal IDEntifier) module provides functionality for working with GLIDE numbers and transforming GLIDE event data into STAC Items.

## Overview

GLIDE numbers are unique identifiers for disasters that follow the format:
`[XX]-[YYYY]-[NNNNN]-[CCC]` where:

- `XX` is the disaster type (e.g., FL for flood)
- `YYYY` is the year
- `NNNNN` is a sequence number
- `CCC` is the ISO country code

## Usage

```python
import requests
from pystac_monty.sources.glide import GlideTransformer, GlideDataSource

# Fetch GLIDE data
url = "https://www.glidenumber.net/glide/jsonglideset.jsp?level1=ESP&fromyear=2024&toyear=2024&events=FL&number=2024-000199"
response = requests.get(url)
data = GlideDataSource(url, response.text)

# Transform to STAC Items
transformer = GlideTransformer(data)
items = transformer.make_items()

# The transformer creates two types of items:
# 1. Source event items (with roles=['source', 'event'])
# 2. Hazard event items (with roles=['source', 'hazard'])

# Access the items
for item in items:
    print(f"Item ID: {item.id}")
    print(f"Roles: {item.properties['roles']}")
    print(f"Datetime: {item.datetime}")
    print(f"Location: {item.geometry}")

    # Access Monty extension data
    monty = MontyExtension.ext(item)
    print(f"Hazard codes: {monty.hazard_codes}")
    print(f"Country codes: {monty.country_codes}")

    if 'hazard' in item.properties['roles']:
        print(f"Hazard detail: {monty.hazard_detail}")
```

## Example Output

For a flood event in Spain (FL-2024-000199-ESP), the transformer creates:

```python
# Source Event Item
{
    "type": "Feature",
    "stac_version": "1.0.0",
    "id": "glide-event-FL-2024-000199-ESP",
    "properties": {
        "datetime": "2024-10-27T00:00:00Z",
        "title": "",
        "description": "GDACS - Medium humanitarian impact in for Spain.According to the authorities (CENEM), as of 30 October at 14.00 UTC , the number of deaths stands at 62 in the Autonomous Region of Valencia (eastern Spain), with an undetermined number of missing in Autonomous Region of Valencia, in Albacete province in Castilla La Mancha autonomous Region. Dozens have been rescued and several have been displaced in Valencia province. In addition, a bridge collapsed in Ribarroja del Turia (Valencia province), several roads have been closed, including some in Granada and Malaga cities in the Andalucia region, and train disruptions have been recorded in the affected area.",
        "magnitude": "0",
        "source": "GDACS",
        "docid": 23388,
        "status": "A",
        "roles": ["source", "event"],
        "monty:hazard_codes": ["FL"],
        "monty:country_codes": ["ESP"],
        "monty:episode_number": 1,
        "monty:corr_id": "20241027-ESP-FL-1-GCDB"
    },
    "geometry": {
        "type": "Point",
        "coordinates": [-3.41102534556838, 38.6013316868745]
    },
    "links": [
        {
            "rel": "via",
            "href": "https://www.glidenumber.net/glide/jsonglideset.jsp?...",
            "type": "application/json",
            "title": "Glide Event Data"
        }
    ],
    "assets": {
        "report": {
            "href": "https://www.glidenumber.net/glide/public/search/details.jsp?glide=23388",
            "type": "application/json",
            "title": "Report"
        }
    }
}

# Hazard Event Item
{
    "type": "Feature",
    "stac_version": "1.0.0",
    "stac_extensions": [
        "https://ifrcgo.github.io/monty/v0.1.0/schema.json"
    ],
    "id": "glide-hazard-FL-2024-000199-ESP",
    "geometry": {
        "type": "Point",
        "coordinates": [-3.41102534556838, 38.6013316868745]
    },
    "properties": {
        "datetime": "2024-10-27T00:00:00Z",
        "title": "",
        "description": "GDACS - Medium humanitarian impact in for Spain.According to the authorities (CENEM), as of 30 October at 14.00 UTC , the number of deaths stands at 62 in the Autonomous Region of Valencia (eastern Spain), with an undetermined number of missing in Autonomous Region of Valencia, in Albacete province in Castilla La Mancha autonomous Region...",
        "magnitude": "0",
        "source": "GDACS",
        "docid": 23388,
        "status": "A",
        "roles": ["source", "hazard"],
        "monty:episode_number": 1,
        "monty:hazard_codes": ["FL"],
        "monty:country_codes": ["ESP"],
        "monty:corr_id": "20241027-ESP-FL-1-GCDB",
        "monty:hazard_detail": {
            "cluster": "FL",
            "severity_unit": "glide",
            "estimate_type": "primary"
        }
    },
    "links": [
        {
            "rel": "via",
            "href": "https://www.glidenumber.net/glide/jsonglideset.jsp?level1=ESP&fromyear=2024&toyear=2024&events=FL&number=2024-000199",
            "type": "application/json",
            "title": "Glide Event Data"
        }
    ],
    "assets": {
        "report": {
            "href": "https://www.glidenumber.net/glide/public/search/details.jsp?glide=23388",
            "type": "application/json",
            "title": "Report"
        }
    }
}
```

## API Reference

### GlideTransformer

```python
class GlideTransformer:
    """
    Transforms GLIDE event data into STAC Items with Monty extension.

    The transformer creates two types of items for each GLIDE event:
    1. Source event items - Basic event information
    2. Hazard event items - Event information with hazard details
    """

    def make_items(self) -> list[Item]:
        """
        Create both source and hazard items for the GLIDE event.

        Returns:
            list[Item]: List of STAC Items with Monty extension
        """

    def make_source_event_items(self) -> List[Item]:
        """
        Create source event items.

        Returns:
            List[Item]: List of source event STAC Items
        """

    def make_hazard_event_items(self) -> List[Item]:
        """
        Create hazard event items.

        Returns:
            List[Item]: List of hazard event STAC Items
        """
```

### GlideDataSource

```python
class GlideDataSource(MontyDataSource):
    """
    Wrapper for GLIDE JSON data from glidenumber.net

    Args:
        source_url: URL of the GLIDE data source
        data: JSON response from the GLIDE API
    """
```
