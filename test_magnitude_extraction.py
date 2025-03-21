#!/usr/bin/env python3
"""
Test script for the magnitude extraction function in glide.py
"""
import sys
from pystac_monty.sources.glide import GlideTransformer

# Create a minimal instance of GlideTransformer
transformer = GlideTransformer(None)

# Example magnitude strings from the user's input
test_cases = [
    "Affected the entire country",
    "Category 4",
    "Category 3",
    "Medium",
    "150 mm",
    "f the flood is expected  to last 7-10 days after t",
    "Yellow",
    "Wind speed: 130 KM/h",
    "M6.3",
    "EF-0 to EF-4",
    "Tropical Cyclone - tropical depression",
    "Govt. PNG, UNCT PNG",
    "Cat IV cyclone",
    "Post tropical cyclone Cat IV",
    "Category 1",
    "EF-0 - EF-4",
    "N/A",
    "EF-4",
    "EF4",
    "D4",
    "85 cm. snow",
    "5.4 and 5.9",
    "Red Epidemiological Alert"
]

# Print the exact strings for the problematic cases
print("Debugging problematic cases:")
print(f"Case 1: '{test_cases[9]}'")  # EF-0 to EF-4
print(f"Case 2: '{test_cases[15]}'")  # EF-0 - EF-4
print()

print("Testing magnitude extraction function:")
print("-" * 60)
print(f"{'Magnitude String':<40} | {'Value':<10} | {'Unit':<15}")
print("-" * 60)

for magnitude in test_cases:
    value, unit = transformer.extract_magnitude_info(magnitude)
    print(f"{magnitude[:38]:<40} | {value:<10} | {unit:<15}")

print("-" * 60)
