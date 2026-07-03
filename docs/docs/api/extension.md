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
- `response_detail` (ResponseDetail | None): Details about the response
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

## ResponseDetail

Class representing the details of a response product (`monty:response_detail`).

```python
class ResponseDetail
```

### Properties

- `type` (str): Response type code, e.g. `eo-del`, `hum-shelter`, `fin-dref` (see `MontyResponseType`)
- `source_id` (str | None): Native identifier in the source system (CEMS activation code, Charter call id, ...)
- `status` (MontyResponseStatus | None): Lifecycle status of the response product
- `monitoring_number` (int | None): Iteration number for monitoring updates; its presence marks the item as a monitoring update
- `producer` (str | None): Organisation that produced the response
- `methodology` (MontyMethodology | None): Type of analysis used to produce the response
- `sendai_targets` (list[str] | None): Unique subset of the Sendai Framework targets `A`-`G`
- `sectors` (list[str] | None): IASC clusters / IFRC EPoA sectors covered, for humanitarian (`hum-*`) items

### Methods

- `is_monitoring_update() -> bool`: Whether `monitoring_number` is set
- `sendai_targets_set() -> set[str]`: `sendai_targets` as a set

## Response builder (`pystac_monty.response`)

Helpers for constructing and querying Response items, mirroring the `HazardDetail`/`ImpactDetail` accessor
pattern without a source-specific ETL transformer:

- `build_response_item(...)`: builds a Response `pystac.Item`, validating `response_detail` (via
  `pystac_monty.validators.response.ResponseDetailValidator`) and wiring `roles: ["response"]`,
  `monty:corr_id`, `monty:country_codes`, `monty:hazard_codes`. Accepts `prev_response_item` (adds a
  `rel: prev` link for monitoring updates) and `related_response_items` (adds bidirectional
  `rel: related` links with `roles: ["response"]`, e.g. for CEMS<->Charter co-activation).
- `link_related_response(item, related_item)`: adds the bidirectional `rel: related` (`roles: ["response"]`) link directly.
- `link_monitoring_update(item, prev_item)`: adds the `rel: prev` link directly.
- `filter_response_items(items, *, type=None, producer=None, methodology=None, status=None)`: filters an
  iterable of items by their `response_detail` fields.

See [Response Items](../getting-started/response-items.md) for usage examples, including layering
`disaster:` and `processing:` extensions alongside `monty:response_detail`.

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

### MontyResponseType

Response type codes, `{domain}-{type}` with `domain` in `eo`, `hum`, `fin`:

```python
class MontyResponseType(StringEnum):
    EO_REFERENCE = "eo-ref"
    EO_FIRST_ESTIMATE = "eo-fep"
    EO_DELINEATION = "eo-del"
    EO_GRADING = "eo-gra"
    EO_POPULATION_EXPOSURE = "eo-pop"
    EO_MONITORING = "eo-mon"
    EO_SITUATIONAL_REPORT = "eo-sr"
    EO_VALUE_ADDED_PRODUCT = "eo-vap"
    HUM_SHELTER = "hum-shelter"
    # ... and other hum-*/fin-* codes, see the response taxonomy doc
```

### MontyResponseStatus

```python
class MontyResponseStatus(StringEnum):
    PLANNED = "planned"
    IN_PRODUCTION = "in-production"
    PUBLISHED = "published"
    FINISHED = "finished"
    NO_IMPACT = "no-impact"
    WITHDRAWN = "withdrawn"
```

### MontyMethodology

```python
class MontyMethodology(StringEnum):
    HUMAN_INTERPRETED = "human_interpreted"
    SEMI_AUTOMATED = "semi_automated"
    AUTOMATED = "automated"
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
