# Extension API Reference

## MontyExtension

The main extension class that provides Monty-specific functionality to STAC objects.

```python
class MontyExtension(Generic[T], PropertiesExtension, ExtensionManagementMixin[Union[pystac.Collection, pystac.Item]])
```

### Properties

- `correlation_id` (str): A unique correlation identifier for the event
- `country_codes` (list[str]): List of ISO 3166-1 alpha-3 country codes
- `hazard_codes` (list[str] | None): List of hazard codes
- `hazard_detail` (HazardDetail | None): Details about the hazard
- `impact_detail` (ImpactDetail | None): Details about the impact
- `episode_number` (int): Episode number for the event

### Methods

#### apply

```python
def apply(
    self,
    correlation_id: str,
    country_codes: list[str],
    hazard_codes: list[str] | None = None,
) -> None
```

Applies Monty Extension properties to the extended STAC object.

#### compute_and_set_correlation_id

```python
def compute_and_set_correlation_id(self, hazard_profiles: HazardProfiles) -> None
```

Computes and sets the correlation ID using the provided hazard profiles.

#### ext

```python
@classmethod
def ext(cls, obj: T, add_if_missing: bool = False) -> MontyExtension[T]
```

Creates an extension instance from a STAC object.

## HazardDetail

Class representing hazard details.

```python
class HazardDetail
```

### Properties

- `cluster` (str): The cluster of the hazard
- `severity_value` (float): The maximum value of the hazard
- `severity_unit` (str): The unit of the maximum value
- `severity_label` (str): The label of the severity
- `estimate_type` (MontyEstimateType): The type of estimate

## ImpactDetail

Class representing impact details.

```python
class ImpactDetail
```

### Properties

- `category` (MontyImpactExposureCategory): The impact category
- `type` (MontyImpactType): The type of impact
- `value` (float): The impact value
- `unit` (str): The unit of measurement
- `estimate_type` (MontyEstimateType): The type of estimate

## Enums

### MontyRoles

```python
class MontyRoles(StringEnum):
    EVENT = "event"
    REFERENCE = "reference"
    SOURCE = "source"
    HAZARD = "hazard"
    IMPACT = "impact"
    RESPONSE = "response"
```

### MontyEstimateType

```python
class MontyEstimateType(StringEnum):
    PRIMARY = "primary"
    SECONDARY = "secondary"
    MODELLED = "modelled"
```

### MontyImpactExposureCategory

Defines categories for impact exposure. Some key values include:

```python
class MontyImpactExposureCategory(StringEnum):
    ALL_PEOPLE = "allpeop"
    CROP = "crop"
    BUILDINGS = "build"
    HOSPITALS = "hosp"
    EDUCATION_CENTERS = "educ"
    # ... and many more
```

### MontyImpactType

Defines types of impacts. Some key values include:

```python
class MontyImpactType(StringEnum):
    UNDEFINED = "unspec"
    DAMAGED = "dama"
    DESTROYED = "dest"
    DEATHS = "deat"
    MISSING = "miss"
    INJURED = "inju"
    # ... and many more
```

## Constants

- `SCHEMA_URI`: The URI of the Monty extension schema
- `PREFIX`: The prefix used for Monty extension properties ("monty:")
