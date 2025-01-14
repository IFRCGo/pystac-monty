# EMDAT

The EMDAT (Emergency Events Database) module provides functionality for working with [EM-DAT](https://public.emdat.be) data.
Full Specification of the transformation can be found [here](https://github.com/IFRCGo/monty-stac-extension/tree/main/model/sources/EMDAT).

```python
from pystac_monty.sources.emdat import EMDATTransformer, EMDATDataSource
```

## EMDATDataSource

The `EMDATDataSource` class handles EM-DAT data input in either Excel format or as a pandas DataFrame.

```python
class EMDATDataSource(MontyDataSource):
    """
    EM-DAT data source that can handle both Excel files and pandas DataFrames.
    
    Args:
        source_url: URL of the data source
        data: Either Excel content as string or pandas DataFrame containing EM-DAT data
    """
```

## EMDATTransformer

The `EMDATTransformer` class transforms EM-DAT data into STAC Items with the Monty extension.

```python
class EMDATTransformer:
    """
    Transforms EM-DAT data into STAC Items with Monty extension.
    
    Args:
        data: EMDATDataSource containing the EM-DAT data
        geocoder: Optional GAULGeocoder instance for enhanced location handling
    """
```

A geocoder is needed to handle location data in the EM-DAT data. The `GAULGeocoder` class is provided for this purpose. It is designed to work with the Global Administrative Unit Layers (GAUL) dataset as described in the EMDAT tutorial: [Making Maps](https://doc.emdat.be/docs/additional-resources-and-tutorials/tutorials/python_tutorial_2/)

```python
from pystac_monty.geocoding import GAULGeocoder

geocoder = GAULGeocoder("path/to/gaul.gpkg")
```

### Output STAC Items

The transformer creates three types of STAC Items:

1. Event Items (`emdat-event-*`)
   - Basic event information including location and dates
   - Monty extension with hazard codes and country codes
   
2. Hazard Items (`emdat-hazard-*`)
   - Derived from event items
   - Additional hazard details including severity and classification
   
3. Impact Items (`emdat-impact-*`)
   - Created for each impact metric
   - Includes detailed impact information with type, value, and units

### Example Usage

```python
from pystac_monty.sources.emdat import EMDATTransformer, EMDATDataSource
from pystac_monty.geocoding import GAULGeocoder

# Create data source from Excel file
data = EMDATDataSource(
    source_url="https://public.emdat.be",
    data="path/to/emdat_data.xlsx"
)

# Initialize geocoder (optional)
geocoder = GAULGeocoder("path/to/gaul.gpkg")

# Create transformer
transformer = EMDATTransformer(data, geocoder)

# Generate STAC items
items = transformer.make_items()

# Or generate specific types of items
event_items = transformer.make_source_event_items()
hazard_items = transformer.make_hazard_event_items()
impact_items = transformer.make_impact_items()
