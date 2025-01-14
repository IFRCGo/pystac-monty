# Installation

## Requirements

- Python >= 3.10

## Installation Methods

### Using pip

```bash
pip install pystac-monty
```

### From Source

To install the latest development version directly from GitHub:

```bash
git clone https://github.com/IFRCGo/monty-stac-extension.git
cd monty-stac-extension
pip install -e .
```

## Dependencies

PySTAC Monty requires the following Python packages:

- python-dateutil >= 2.7.0
- pystac >= 1.11.0
- geojson >= 2.5.0
- markdownify >= 0.14.1
- pytz >= 2021.1
- pandas >= 2.2.0
- shapely >= 2.0.0

These dependencies will be automatically installed when you install PySTAC Monty.

## Development Installation

For development, you might want to install additional dependencies:

```bash
pip install -e ".[dev]"
```

This will install development dependencies including:
- Testing tools (pytest, coverage)
- Code quality tools (ruff, mypy)
- Documentation tools (mkdocs)
