# Changelog

## [Unreleased]

### Added

- Static Monty STAC batch export (`pystac_monty/exporter.py`) with role collections on disk.
- CLI entrypoint `pystac-monty` for GLIDE, GFD, and GDACS sources (`pystac_monty/sources/batch_export.py`). GDACS accepts a native `GdacsDataSourceType` bundle JSON produced upstream (e.g. montandon-etl).
- `ItemMontyExtension.is_source_response()` for partitioning response items.

[Unreleased]: <https://github.com/stac-utils/pystac/compare/v1.11.0..main>

<!-- markdownlint-disable-file MD024 -->
