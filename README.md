# Montandon Extension for PySTAC

This project provides a Python library for working with the SpatioTemporal Asset Catalog (STAC) extension for Montandon. It extends the capabilities of PySTAC to handle disaster and hazard-related data using the Montandon schema.

## Features

- Extend STAC Collections and Items with Montandon-specific properties.
- Support for hazard detail objects.
- Integration with GDACS data sources.
- Utilities for pairing and correlation ID generation.

## Installation

To install the library, use pip:

```sh
pip install pystac_monty
```

To install the tool `jq`, use the following:

Debian/Ubuntu/Mint environment
```sh
sudo apt-get update
sudo apt-get install jq
```

MacOS environment
```sh
brew install jq
```

To check if it is install correctly

```sh
jq --version
```

## Usage

## Extending a STAC Item

The library provides classes and functions to work with Montandon STAC objects. Here is an example of how to create a Montandon Item:

```python
import pystac
from pystac_monty.extension import MontyExtension

item = pystac.Item(...)  # Create or load a STAC Item
monty_ext = MontyExtension.ext(item)
monty_ext.episode_number = 1
print(monty_ext.episode_number)
```

### Working with GDACS data

To transform GDACS event data into STAC Items:

```python
from pystac_monty.sources.gdacs import GDACSTransformer, GDACSDataSource

data_source = GDACSDataSource(source_url="...", data="...", type=GDACSDataSourceType.EVENT)
transformer = GDACSTransformer(data=[data_source])
items = transformer.make_items()
```

## Development

To set up the development environment:

```sh
pip install uv
uv sync
```

To run the tests:

1. Make the test with actual calls to http and write them in the cassette files:

```sh
uv run pytest -v -s --record-mode rewrite
```

2. Run the tests with the recorded calls:

```sh
uv run pytest -v -s
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for more details.

### Links

- [Documentation](https://pystac.readthedocs.io)
- [Repository](https://github.com/IFRCGo/monty-stac-extension)
- [Issues](https://github.com/IFRCGo/monty-stac-extension/issues)
