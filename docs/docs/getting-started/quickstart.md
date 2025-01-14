# Quick Start Guide

This guide will help you get started with PySTAC Monty, showing you how to use the extension with STAC Items and Collections.

## Basic Usage

```python
import pystac
from pystac_monty import MontyExtension, MontyRoles, HazardDetail, ImpactDetail
from pystac_monty.hazard_profiles import HazardProfiles

# Enable the extension
MontyExtension.enable_extension()

# Create a STAC Item
item = pystac.Item(
    id="example-event",
    geometry=None,
    bbox=None,
    datetime=None,
    properties={
        "roles": [MontyRoles.EVENT, MontyRoles.SOURCE]
    }
)

# Add the extension to the item
monty_ext = MontyExtension.ext(item, add_if_missing=True)

# Set basic properties
monty_ext.apply(
    correlation_id="2024-cyclone-01",
    country_codes=["FJI"],  # Fiji
    hazard_codes=["TC"]     # Tropical Cyclone
)

# Add hazard details
hazard_detail = HazardDetail(
    cluster="TC",
    severity_value=150.0,
    severity_unit="km/h",
    severity_label="Category 4"
)
monty_ext.hazard_detail = hazard_detail

# Add impact details
impact_detail = ImpactDetail(
    category="allpeop",  # All People
    type="affe",        # Affected
    value=10000,
    unit="people"
)
monty_ext.impact_detail = impact_detail

# Compute correlation ID using hazard profiles
hazard_profiles = HazardProfiles()
monty_ext.compute_and_set_correlation_id(hazard_profiles)

# Check if item is a source event
is_source = monty_ext.is_source_event()  # True
```

## Working with Collections

You can also use the extension with STAC Collections:

```python
# Create a STAC Collection
collection = pystac.Collection(
    id="disaster-events",
    description="Collection of disaster events",
    extent=pystac.Extent(
        spatial=pystac.SpatialExtent([[0, 0, 1, 1]]),
        temporal=pystac.TemporalExtent([[None, None]])
    )
)

# Add the extension to the collection
monty_ext = MontyExtension.ext(collection, add_if_missing=True)

# Set collection properties
monty_ext.apply(
    correlation_id="2024-events",
    country_codes=["FJI", "VUT"],  # Fiji and Vanuatu
    hazard_codes=["TC", "FL"]      # Tropical Cyclone and Flood
)
```

## Extension Properties

The Monty extension provides several properties:

- `correlation_id`: Unique identifier for the event
- `country_codes`: List of ISO 3166-1 alpha-3 country codes
- `hazard_codes`: List of hazard codes
- `hazard_detail`: Details about the hazard (severity, units, etc.)
- `impact_detail`: Details about the impact (category, type, value, etc.)
- `episode_number`: Episode number for the event

## Roles

The extension defines several roles that can be used to classify items:

- `MontyRoles.EVENT`: Represents an event
- `MontyRoles.REFERENCE`: Reference data
- `MontyRoles.SOURCE`: Source data
- `MontyRoles.HAZARD`: Hazard data
- `MontyRoles.IMPACT`: Impact data
- `MontyRoles.RESPONSE`: Response data

## Helper Methods

The extension provides helper methods to check item types:

```python
# Check if an item is a source event
is_source_event = monty_ext.is_source_event()

# Check if an item is a source hazard
is_source_hazard = monty_ext.is_source_hazard()

# Check if an item is a source impact
is_source_impact = monty_ext.is_source_impact()
