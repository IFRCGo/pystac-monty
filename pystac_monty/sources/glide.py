import json
import mimetypes
from datetime import datetime
from typing import Any, List

from pystac import Asset, Item, Link
from shapely.geometry import Point, mapping

from pystac_monty.extension import HazardDetail, MontyEstimateType, MontyExtension
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import MontyDataSource, MontyDataTransformer

STAC_EVENT_ID_PREFIX = "glide-event-"
STAC_HAZARD_ID_PREFIX = "glide-hazard-"


class GlideDataSource(MontyDataSource):
    def __init__(self, source_url: str, data: Any):
        super().__init__(source_url, data)
        self.data = json.loads(data)


class GlideTransformer(MontyDataTransformer):
    """
    Transforms Glide event data into STAC Items
    """

    hazard_profiles = MontyHazardProfiles()

    def __init__(self, data: GlideDataSource) -> None:
        super().__init__("glide")
        self.data = data

    def make_items(self) -> list[Item]:
        """Create Glide Items"""
        items = []

        glide_events = self.make_source_event_items()
        items.extend(glide_events)

        glide_hazards = self.make_hazard_event_items()
        items.extend(glide_hazards)

        return items

    def get_hazard_codes(self, hazard: str) -> List[str]:
        hazard_mapping = {
            "EQ": ["nat-gem-ear-gro", "EQ"],
            "TC": ["nat-met-sto-tro", "TC"],
            "FL": ["nat-hyd-flo-flo", "FL"],
            "DR": ["nat-cli-dro-dro", "DR"],
            "WF": ["nat-cli-wil-wil", "WF"],
            "VO": ["nat-geo-vol-vol", "VO"],
            "TS": ["nat-geo-ear-tsu", "TS"],
            "CW": ["nat-met-ext-col", "CW"],
            "EP": ["nat-bio-epi-dis", "EP"],
            "EC": ["nat-met-sto-ext", "EC"],
            "ET": ["nat-met-ext-col", "ET"],
            "FR": ["tec-ind-fir-fir", "FR"],
            "FF": ["nat-hyd-flo-fla", "FF"],
            "HT": ["nat-met-ext-hea", "HT"],
            "IN": ["nat-bio-inf-inf", "IN"],
            "LS": ["nat-hyd-mmw-lan", "LS"],
            "MS": ["nat-hyd-mmw-mud", "MS"],
            "ST": ["nat-met-sto-sto", "ST"],
            "SL": ["nat-hyd-mmw-lan", "SL"],
            "AV": ["nat-geo-mmd-ava", "AV"],
            "SS": ["nat-met-sto-sur", "SS"],
            "AC": ["AC"],
            "TO": ["nat-met-sto-tor", "TO"],
            "VW": ["nat-met-sto-tor", "VW"],
            "WV": ["nat-hyd-wav-rog", "WV"],
            "OT": ["OT"],
            "CE": ["CE"],
        }
        if hazard not in hazard_mapping:
            print(f"Warning: Hazard {hazard} not found in mapping.")
            return ["OT"]  # Return a default value instead of raising an exception
        return hazard_mapping.get(hazard)

    def make_source_event_items(self) -> List[Item]:
        """Create source event items"""
        event_items = []
        # validate data for glide transformation
        try:
            glide_events = self.check_and_get_glide_events()
        except Exception as e:
            print(f"Error getting glide events: {str(e)}")
            return []

        if not glide_events == []:
            for data in glide_events:
                try:
                    glide_id = STAC_EVENT_ID_PREFIX + data.get("event") + "-" + data.get("number") + "-" + data.get("geocode")
                    latitude = float(data.get("latitude"))
                    longitude = float(data.get("longitude"))
                    event_date = {
                        "year": abs(int(data.get("year"))),
                        "month": abs(int(data.get("month"))),
                        "day": abs(int(data.get("day"))),
                    }  # abs is used to ignore negative sign

                    point = Point(longitude, latitude)
                    geometry = mapping(point)
                    bbox = [longitude, latitude, longitude, latitude]

                    item = Item(
                        id=glide_id,
                        geometry=geometry,
                        bbox=bbox,
                        datetime=self.make_date(event_date),
                        properties={
                            "title": data.get("title", ""),
                            "description": data.get("comments", ""),
                            "magnitude": data.get("magnitude", ""),
                            "source": data.get("source", ""),
                            "docid": data.get("docid", ""),
                            "status": data.get("status", ""),
                        },
                    )

                    # Add keywords
                    keywords = [data.get("event", ""), "glide"]
                    if data.get("geocode"):
                        keywords.append(data.get("geocode"))
                    if data.get("title"):
                        keywords.append(data.get("title"))
                    item.properties["keywords"] = keywords

                    item.set_collection(self.get_event_collection())
                    item.properties["roles"] = ["source", "event"]

                    MontyExtension.add_to(item)
                    monty = MontyExtension.ext(item)
                    # Since there is no episode_number in glide data,
                    # we set it to 1 as it is required to create the correlation id
                    # in the method monty.compute_and_set_correlation_id(..)
                    monty.episode_number = 1
                    monty.hazard_codes = self.get_hazard_codes(data.get("event"))
                    monty.country_codes = [data.get("geocode")]

                    monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

                    item.add_link(Link("via", self.data.get_source_url(), "application/json", "Glide Event Data"))
                    item.add_asset(
                        "report",
                        Asset(
                            href=f"https://www.glidenumber.net/glide/public/search/details.jsp?glide={data.get('docid')}",
                            media_type=mimetypes.types_map[".json"],
                            title="Report",
                        ),
                    )

                    event_items.append(item)
                except Exception as e:
                    print(f"Error processing glide event {data.get('number', 'unknown')}: {str(e)}")
                    continue
        return event_items

    def make_hazard_event_items(self) -> List[Item]:
        """Create hazard event items"""
        hazard_items = []
        items = self.make_source_event_items()

        for item in items:
            try:
                item_id = item.id
                item.id = item.id.replace(STAC_EVENT_ID_PREFIX, STAC_HAZARD_ID_PREFIX)
                item.set_collection(self.get_hazard_collection())
                item.properties["roles"] = ["source", "hazard"]

                monty = MontyExtension.ext(item)
                monty.hazard_detail = self.get_hazard_detail(item)
                monty.hazard_codes = [monty.hazard_detail.cluster]

                # Add hazard-specific keywords
                keywords = item.properties.get("keywords", [])
                keywords.append("hazard")
                if monty.hazard_detail and monty.hazard_detail.cluster:
                    keywords.append(monty.hazard_detail.cluster)
                if monty.hazard_detail and monty.hazard_detail.severity_unit:
                    keywords.append(monty.hazard_detail.severity_unit)
                item.properties["keywords"] = keywords

                hazard_items.append(item)
            except Exception as e:
                print(f"Error processing hazard item {item.id if hasattr(item, 'id') else 'unknown'}: {str(e)}")
                continue

        return hazard_items

    def _handle_ef_range(self, magnitude_str: str) -> tuple[float, str]:
        """
        Special handler for EF tornado scale ranges.
        
        Args:
            magnitude_str: The magnitude string to parse
            
        Returns:
            tuple: (numerical_value, unit)
        """
        # Exact string matches for common problematic cases
        if magnitude_str == "EF-0 to EF-4" or magnitude_str == "EF-0 - EF-4":
            return 4.0, "EF"
            
        # Extract numbers from EF range pattern
        import re
        numbers = re.findall(r'EF-?(\d+)', magnitude_str)
        if len(numbers) >= 2:
            values = [float(n) for n in numbers]
            return max(values), "EF"
            
        return 0.0, "glide"
    
    def extract_magnitude_info(self, magnitude_str: str) -> tuple[float, str]:
        """
        Extract numerical value and unit from magnitude string on a best-effort basis.
        
        Args:
            magnitude_str: The magnitude string to parse
            
        Returns:
            tuple: (numerical_value, unit)
        """
        import re
        
        # Default values
        value = 0.0
        unit = "glide"
        
        if not magnitude_str or magnitude_str.strip() == "N/A":
            return value, unit
            
        # Special case for EF ranges which need custom handling
        if "EF" in magnitude_str and ("to" in magnitude_str or (" - " in magnitude_str)):
            return self._handle_ef_range(magnitude_str)
            
        # Try to extract patterns
        
        # Pattern: "Category X" or "Cat X" or "Category X cyclone"
        category_match = re.search(r'Cat(?:egory)?\s+([IVX0-9]+)', magnitude_str, re.IGNORECASE)
        if category_match:
            cat_value = category_match.group(1)
            # Convert Roman numerals if needed
            if re.match(r'^[IVX]+$', cat_value):
                roman_map = {'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'V': 5}
                value = float(roman_map.get(cat_value, 0))
            else:
                value = float(cat_value)
            unit = "category"
            return value, unit
            
        # Pattern: "X mm" or "X cm. snow" or similar measurements
        measurement_match = re.search(r'(\d+(?:\.\d+)?)\s*(mm|cm|m|km|KM/h)', magnitude_str)
        if measurement_match:
            value = float(measurement_match.group(1))
            unit = measurement_match.group(2)
            return value, unit
            
        # Pattern: "Wind speed: X KM/h"
        wind_match = re.search(r'Wind speed:\s*(\d+(?:\.\d+)?)\s*(KM/h)', magnitude_str, re.IGNORECASE)
        if wind_match:
            value = float(wind_match.group(1))
            unit = wind_match.group(2)
            return value, unit
            
        # Pattern: "MX.X" (earthquake magnitude)
        eq_match = re.search(r'M(\d+(?:\.\d+)?)', magnitude_str)
        if eq_match:
            value = float(eq_match.group(1))
            unit = "magnitude"
            return value, unit
            
        # Pattern: "EF-X" or "EFX" (tornado scale)
        ef_match = re.search(r'EF-?(\d+)', magnitude_str)
        if ef_match:
            value = float(ef_match.group(1))
            unit = "EF"
            return value, unit
            
        # Pattern: "D4" (drought scale)
        d_scale_match = re.search(r'D(\d+)', magnitude_str)
        if d_scale_match:
            value = float(d_scale_match.group(1))
            unit = "drought_scale"
            return value, unit
            
        # Pattern: "X.X and Y.Y" (multiple values - take the highest)
        multiple_values_match = re.findall(r'(\d+(?:\.\d+)?)', magnitude_str)
        if multiple_values_match:
            values = [float(v) for v in multiple_values_match]
            if values:
                value = max(values)
                return value, unit
            
        # Pattern: "Yellow", "Medium", etc. (qualitative descriptions)
        qualitative_map = {
            "yellow": 1.0,
            "medium": 2.0,
            "red": 3.0,
            "high": 3.0,
            "tropical storm": 1.0,
            "tropical depression": 0.5,
        }
        
        for term, mapped_value in qualitative_map.items():
            if term.lower() in magnitude_str.lower():
                value = mapped_value
                unit = term
                return value, unit
                
        # Last resort: try to find any number in the string
        any_number = re.search(r'(\d+(?:\.\d+)?)', magnitude_str)
        if any_number:
            value = float(any_number.group(1))
            
        return value, unit
        
    def get_hazard_detail(self, item: Item) -> HazardDetail:
        """Get hazard detail"""
        try:
            magnitude = item.properties.get("magnitude", "").strip()
            severity_label = magnitude
            severity_value = 0
            severity_unit = "glide"
            
            if magnitude:
                try:
                    severity_value, severity_unit = self.extract_magnitude_info(magnitude)
                    severity_value = int(severity_value) if severity_value.is_integer() else severity_value
                except (ValueError, TypeError) as e:
                    print(f"Warning: Could not convert magnitude '{magnitude}' to number for item {item.id}: {str(e)}")
            
            return HazardDetail(
                cluster=self.hazard_profiles.get_cluster_code(item),
                severity_value=severity_value,
                severity_unit=severity_unit,
                severity_label=severity_label,
                estimate_type=MontyEstimateType.PRIMARY,
            )
        except Exception as e:
            print(f"Error getting hazard detail for item {item.id}: {str(e)}")
            # Return a default hazard detail
            return HazardDetail(
                cluster="OT",
                severity_value=0,
                severity_unit="glide",
                estimate_type=MontyEstimateType.PRIMARY,
            )

    def make_date(self, event_date: dict) -> datetime:
        """Generate a datetime object"""
        try:
            # sometimes the day is not the day in the month but the day in the year
            # so we need to check if the day is greater than 31 if the datetime parsing fails
            year = event_date.get('year', 1970)
            month = event_date.get('month', 1)
            day = event_date.get('day', 1)
            
            # Validate and adjust the date if needed
            if month < 1 or month > 12:
                print(f"Warning: Invalid month {month}, setting to 1")
                month = 1
                
            # Check if day is valid for the month
            import calendar
            max_days = calendar.monthrange(year, month)[1]
            if day < 1 or day > max_days:
                print(f"Warning: Day {day} is out of range for month {month}, setting to 1")
                day = 1
                
            dt = datetime(year=year, month=month, day=day)
            formatted_date = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

            date = datetime.fromisoformat(formatted_date.replace("Z", "+00:00"))
            return date
        except (ValueError, TypeError) as e:
            print(f"Error creating date from {event_date}: {str(e)}")
            # Return a default date if there's an error
            return datetime(year=1970, month=1, day=1)  # Use a fixed default date instead of now()

    def check_and_get_glide_events(self) -> list[Any]:
        """Validate the source fields"""
        valid_events = []
        try:
            glideset: list[Any] = self.data.get_data()["glideset"]
            if glideset == []:
                print(f"No Glide data found in {self.data.get_source_url()}")
                return []
                
            for obj in glideset:
                try:
                    required_fields = ["latitude", "longitude", "event", "number", "geocode"]
                    missing_fields = [field for field in required_fields if field not in obj]

                    if missing_fields:
                        print(f"Warning: Missing required fields {missing_fields} in glide number {obj.get('number')}. Skipping this event.")
                        continue
                    
                    valid_events.append(obj)
                except Exception as e:
                    print(f"Error validating glide event {obj.get('number', 'unknown')}: {str(e)}")
                    continue
                    
            return valid_events
        except Exception as e:
            print(f"Error getting glide events: {str(e)}")
            return []
