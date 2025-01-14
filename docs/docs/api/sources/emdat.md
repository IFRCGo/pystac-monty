# EMDAT

The EMDAT (Emergency Events Database) module provides functionality for working with EM-DAT data.

```python
from pystac_monty.sources.emdat import EMDATTransformer
```

## EMDATTransformer

Transforms EM-DAT data into STAC Items.

```python
class EMDATTransformer(BaseTransformer):
    """Transforms EM-DAT data into STAC Items with Monty extension."""
    def transform(self, emdat_data: dict) -> pystac.Item:
        """
        Transform EM-DAT data into a STAC Item.
        
        Args:
            emdat_data: Dictionary containing EM-DAT event data
            
        Returns:
            pystac.Item: A STAC Item with Monty extension
        """
