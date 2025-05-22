# Common

The common module provides shared functionality for all sources.

```python
from pystac_monty.sources.common import BaseTransformer, MontyDataSource
```

## BaseTransformer

Base class for all source transformers. Transformers are responsible for converting source data into STAC Items. Each data source (like GLIDE, GDACS, etc.) should implement its own transformer by inheriting from this base class.

### Implementing a New Transformer

To implement a new transformer for a data source:

1. Create a new class that inherits from `BaseTransformer`
2. Implement the `transform` method that converts source data into STAC Items
3. Handle data validation and transformation according to your source's format

Example implementation:

```python
class MySourceTransformer(BaseTransformer):
    def __init__(self, data: MontyDataSource):
        self.data = data

    def transform(self, data: Any) -> pystac.Item:
        # Validate input data
        if not self._validate_data(data):
            raise ValueError("Invalid data format")

        # Transform data into STAC Item
        item = pystac.Item(
            id="unique-id",
            geometry=self._extract_geometry(data),
            bbox=self._extract_bbox(data),
            datetime=self._extract_datetime(data),
            properties=self._extract_properties(data)
        )

        # Add any extensions or additional metadata
        return item
```

### Data Input Pattern

Data is passed to transformers using the `MontyDataSource` class, which encapsulates:

- `source_url`: The URL or identifier of where the data came from
- `data`: The actual data to be transformed (can be any type)

Example usage:

```python
source = MontyDataSource(
    source_url="https://api.example.com/data",
    data=raw_data
)
transformer = MySourceTransformer(source)
stac_items = transformer.transform(source.get_data())
```

The `MontyDataSource` class provides a consistent interface for handling different types of input data while maintaining source information. This pattern allows transformers to:

1. Access both the raw data and its source information
2. Track data provenance
3. Handle different data formats consistently

### Key Methods

When implementing a transformer, consider implementing these key methods:

- `transform()`: The main method that converts source data into STAC Items
- Data validation methods to ensure input data meets requirements
- Helper methods for extracting specific STAC Item components (geometry, datetime, properties)
- Methods for handling source-specific data formats or requirements

The transformer should handle:

- Data validation and error checking
- Conversion of source data format to STAC Items
- Addition of any required extensions or metadata
- Proper handling of source URLs and data provenance
