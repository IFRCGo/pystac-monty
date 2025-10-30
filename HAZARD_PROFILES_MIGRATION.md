# HIPs 2025 Migration Guide for Hazard Profiles

## Overview

This document provides guidance for migrating from UNDRR-ISC 2020 Hazard Information Profiles to the 2025 update in the `pystac_monty` project.

## Key Changes in HIPs 2025

### New Code Format
- **2025 Format**: 2 letters + 4 digits (e.g., `MH0600`, `GH0101`)
- **Organized Structure**: 281 hazards across 8 hazard types and 39 clusters
- **Chapeau HIPs**: General umbrella hazards (e.g., `MH0600` for "Flooding")

### Hazard Code Requirements

According to the [Monty STAC Extension PR #32](https://github.com/IFRCGo/monty-stac-extension/pull/32):

> Hazard items **MUST** have **exactly one UNDRR-ISC 2025 code** (format: 2 letters + 4 digits).
> Optionally, the array may also include **at most one GLIDE code** and **at most one EM-DAT code**.

**Valid examples:**
- `["MH0600"]` - Only UNDRR 2025 code
- `["FL", "MH0600"]` - GLIDE + UNDRR 2025
- `["FL", "nat-hyd-flo-flo", "MH0600"]` - GLIDE + EM-DAT + UNDRR 2025

**Invalid examples:**
- `["FL"]` - Missing UNDRR 2025 code
- `["MH0600", "MH0601"]` - Multiple UNDRR codes
- `["FL", "TC", "MH0600"]` - Multiple GLIDE codes

## Updated HazardProfiles.csv Structure

The CSV now includes the following columns:

| Column | Description |
|--------|-------------|
| `undrr_2025_key` | **Primary**: UNDRR-ISC 2025 code (e.g., MH0600) |
| `undrr_key` | UNDRR-ISC 2020 code for backward compatibility (e.g., MH0004) |
| `label` | Human-readable hazard name |
| `cluster_label` | Cluster name (e.g., "Water-related", "Seismic") |
| `family_label` | Family name (e.g., "Meteorological & Hydrological") |
| `link_group` | Internal cluster grouping |
| `link_maingroup` | Internal family grouping |
| `glide_code` | GLIDE classification code (e.g., FL, EQ) |
| `emdat_key` | EM-DAT classification key (e.g., nat-hyd-flo-flo) |

## New Functions in `hazard_profiles.py`

### `get_undrr_2025_code(hazard_codes: List[str]) -> Optional[str]`

Extracts the UNDRR-ISC 2025 code from a list of hazard codes.

```python
from pystac_monty.hazard_profiles import MontyHazardProfiles

hazard_profiles = MontyHazardProfiles()
codes = ["FL", "nat-hyd-flo-flo", "MH0600"]
undrr_2025 = hazard_profiles.get_undrr_2025_code(codes)
# Returns: "MH0600"
```

### `get_keywords(hazard_codes: List[str]) -> List[str]`

Generates human-readable keywords from hazard codes for use in STAC item `keywords` field.

```python
from pystac_monty.hazard_profiles import MontyHazardProfiles

hazard_profiles = MontyHazardProfiles()
codes = ["MH0600", "FL"]
keywords = hazard_profiles.get_keywords(codes)
# Returns: ["Flooding", "Meteorological & Hydrological", "Water-related"]
```

## Migration Steps for Source Transformers

### 1. Update Hazard Code Mappings

Each source transformer needs to:

1. **Add HIPs 2025 codes** to existing mapping functions
2. **Ensure UNDRR 2025 code is first** in the hazard_codes array
3. **Include cross-classification codes** (GLIDE, EM-DAT) for interoperability

#### Example: Before (GDACS)

```python
def get_hazard_codes(self, hazard: str) -> List[str]:
    hazard_mapping = {
        "EQ": ["nat-geo-ear-gro", "EQ"],
        "TC": ["nat-met-sto-tro", "TC"],
        "FL": ["nat-hyd-flo-flo", "FL"],
        # ...
    }
    return hazard_mapping.get(hazard, [])
```

#### Example: After (GDACS)

```python
def get_hazard_codes(self, hazard: str) -> List[str]:
    hazard_mapping = {
        "EQ": ["GH0101", "EQ", "nat-geo-ear-gro"],  # UNDRR 2025 first!
        "TC": ["MH0309", "TC", "nat-met-sto-tro"],  # Tropical Cyclone
        "FL": ["MH0600", "FL", "nat-hyd-flo-flo"],  # General Flooding
        # ...
    }
    return hazard_mapping.get(hazard, [])
```

### 2. Use `get_keywords()` for STAC Items

Update item creation to populate keywords:

```python
from pystac_monty.hazard_profiles import MontyHazardProfiles

hazard_profiles = MontyHazardProfiles()

# In your make_event_item or similar method:
monty.hazard_codes = self.get_hazard_codes(source_hazard_type)

# Generate and add keywords
hazard_keywords = hazard_profiles.get_keywords(monty.hazard_codes)
country_keywords = [get_country_name(code) for code in monty.country_codes]
item.properties["keywords"] = hazard_keywords + country_keywords
```

## Common HIPs 2025 Mappings

### Meteorological & Hydrological Hazards

| 2020 Code | 2025 Code | Label | GLIDE | EM-DAT |
|-----------|-----------|-------|-------|--------|
| MH0004-MH0013 | MH0600 | Flooding (Chapeau) | FL | nat-hyd-flo-flo |
| MH0006 | MH0603 | Flash Flooding | FF | nat-hyd-flo-fla |
| MH0007 | MH0604 | Fluvial (Riverine) Flooding | FL | nat-hyd-flo-riv |
| MH0035 | MH0401 | Drought | DR | nat-cli-dro-dro |
| MH0047 | MH0501 | Heatwave | HT | nat-met-ext-hea |
| MH0040 | MH0502 | Cold Wave | CW | nat-met-ext-col |
| MH0057-MH0058 | MH0309 | Tropical Cyclone | TC | nat-met-sto-tro |
| MH0029 | MH0705 | Tsunami | TS | nat-geo-ear-tsu |

### Geological Hazards

| 2020 Code | 2025 Code | Label | GLIDE | EM-DAT |
|-----------|-----------|-------|-------|--------|
| GH0001-GH0008 | GH0101 | Earthquake | EQ | nat-geo-ear-gro |
| GH0009-GH0020 | GH0201-GH0205 | Volcanic (various) | VO | nat-geo-vol-* |
| GH0007,GH0014,GH0031 | GH0300 | Gravitational Mass Movement (Chapeau) | LS | nat-geo-mmd-* |

### Environmental Hazards

| 2020 Code | 2025 Code | Label | GLIDE | EM-DAT |
|-----------|-----------|-------|-------|--------|
| EN0013 | EN0205 | Wildfires | WF | nat-cli-wil-wil |

## HazardProfiles.csv Migration Status

The `HazardProfiles.csv` file has been updated with:

- ✅ New column structure with `undrr_2025_key`, `cluster_label`, and `family_label`
- ✅ Mappings for key meteorological & hydrological hazards
- ⚠️ Partial mappings for geological, environmental, and technological hazards
- ⚠️ Biological and societal hazards need completion

### Contributing Mappings

To add or update mappings:

1. Check the [HIPs 2025 documentation](https://www.undrr.org/media/107380/download?startDownload=20250613)
2. Find the corresponding 2025 code for your hazard
3. Update the CSV row with:
   - `undrr_2025_key`: The new 2025 code
   - `cluster_label`: From the HIPs 2025 cluster name
   - `family_label`: From the HIPs 2025 family name
4. Test with your source transformer

## Source-Specific Issues

As you update each source transformer, please create GitHub issues to track:

1. **Issue Title**: "Update [SOURCE] transformer for HIPs 2025"
2. **Issue Body**: Should include:
   - Source-specific hazard mappings needed
   - Any ambiguous cases requiring clarification
   - Testing plan for the migration

## References

- [Monty STAC Extension PR #32](https://github.com/IFRCGo/monty-stac-extension/pull/32/files)
- [HIPs 2025 Report](https://www.undrr.org/media/107380/download?startDownload=20250613)
- [Monty Extension Taxonomy Documentation](https://ifrcgo.org/monty-stac-extension/model/taxonomy.md)

## Questions?

If you have questions about the migration, please:
1. Check the [Monty Extension documentation](https://ifrcgo.org/monty-stac-extension/)
2. Review examples in the [monty-stac-extension examples folder](https://github.com/IFRCGo/monty-stac-extension/tree/main/examples)
3. Open a discussion in the pystac-monty repository
