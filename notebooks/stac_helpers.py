"""
STAC Helper Functions

This module contains helper functions for working with STAC APIs and creating STAC items.
"""

import json
import os

import pystac
import requests

from pystac_monty.extension import MontyExtension


def check_stac_api_availability(api_url) -> bool:
    """
    Check if a STAC API is available at the given URL

    Parameters:
    - api_url: URL of the STAC API

    Returns:
    - Boolean indicating if the API is available
    """
    try:
        response = requests.get(f"{api_url}/")
        if response.status_code == 200:
            print(f"STAC API is available at {api_url}")
            return True
        else:
            print(f"STAC API returned status code {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to STAC API: {e}")
        return False


def check_collection_exists(api_url, collection_id) -> bool:
    """
    Check if a collection exists in the STAC API

    Parameters:
    - api_url: URL of the STAC API
    - collection_id: ID of the collection to check

    Returns:
    - Boolean indicating if the collection exists
    """
    try:
        response = requests.get(f"{api_url}/collections/{collection_id}")
        if response.status_code == 200:
            print(f"Collection '{collection_id}' exists in the STAC API")
            return True
        elif response.status_code == 404:
            print(f"Collection '{collection_id}' does not exist in the STAC API")
            return False
        else:
            print(f"Unexpected status code {response.status_code} when checking collection")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error checking collection: {e}")
        return False


def create_collection_from_file(api_url, collection_path) -> bool:
    """
    Create a collection in the STAC API using a predefined collection definition file

    Parameters:
    - api_url: URL of the STAC API
    - collection_path: Path to the collection definition file

    Returns:
    - Boolean indicating if the collection was created successfully
    """
    try:
        # Check if the collection definition file exists
        if not os.path.exists(collection_path):
            print(f"Collection definition file not found: {collection_path}")
            return False

        # Load the collection definition from the file
        with open(collection_path, "r") as f:
            collection_dict = json.load(f)

        collection_id = collection_dict.get("id")
        if not collection_id:
            print(f"Collection ID not found in {collection_path}")
            return False

        # Create the collection using the transaction API
        response = requests.post(f"{api_url}/collections", json=collection_dict, headers={"Content-Type": "application/json"})

        if response.status_code in [200, 201]:
            print(f"Collection '{collection_id}' created successfully")
            return True
        else:
            print(f"Failed to create collection '{collection_id}'. Status code: {response.status_code}")
            print(f"Response: {response.text}")
            return False
    except Exception as e:
        print(f"Error creating collection from file: {e}")
        return False


def create_collection_fallback(api_url, collection_id, description, roles) -> bool:
    """
    Create a collection in the STAC API using pystac if the predefined collection is not available

    Parameters:
    - api_url: URL of the STAC API
    - collection_id: ID of the collection to create
    - description: Description of the collection
    - roles: List of roles for the collection

    Returns:
    - Boolean indicating if the collection was created successfully
    """
    try:
        # Create a STAC collection
        collection = pystac.Collection(
            id=collection_id,
            description=description,
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]), temporal=pystac.TemporalExtent([[None, None]])
            ),
        )

        # Add the Monty extension to the collection
        monty_ext = MontyExtension.ext(collection, add_if_missing=True)
        monty_ext.apply(
            correlation_id=collection_id,
            hazard_codes=["nat-geo-ear-grd"],  # Earthquake ground shaking
        )

        # Add roles to the collection
        collection.properties["roles"] = roles

        # Convert the collection to a dictionary
        collection_dict = collection.to_dict()

        # Create the collection using the transaction API
        response = requests.post(f"{api_url}/collections", json=collection_dict, headers={"Content-Type": "application/json"})

        if response.status_code in [200, 201]:
            print(f"Collection '{collection_id}' created successfully using fallback method")
            return True
        else:
            print(f"Failed to create collection '{collection_id}' using fallback method. Status code: {response.status_code}")
            print(f"Response: {response.text}")
            return False
    except Exception as e:
        print(f"Error creating collection using fallback method: {e}")
        return False


def add_items_to_collection(api_url, collection_id, items, overwrite=False, batch_size=1000):
    """
    Add items to a collection using the transaction API, with support for bulk loading

    Parameters:
    - api_url: URL of the STAC API
    - collection_id: ID of the collection to add items to
    - items: List of STAC items to add
    - overwrite: Boolean indicating whether to overwrite existing items
    - batch_size: Number of items to process in each batch (default: 1000)

    Returns:
    - Tuple of (successful_items, failed_items) counts
    """
    successful_items = 0
    failed_items = 0

    # Process items in batches
    for i in range(0, len(items), batch_size):
        batch = items[i : i + batch_size]
        print(f"Processing batch {i // batch_size + 1} of {(len(items) + batch_size - 1) // batch_size} ({len(batch)} items)")

        # Try bulk loading first with compression
        try:
            # Convert all items in the batch to dictionaries
            bulk_items = {}
            for item in batch:
                item_dict = item.to_dict()
                bulk_items[item.id] = item_dict
            item_dicts = {"items": bulk_items, "method": "upsert"}

            # Attempt to bulk load items
            response = requests.post(
                f"{api_url}/collections/{collection_id}/bulk_items",
                data=item_dicts,
                headers={
                    "Content-Type": "application/json",
                },
            )

            # Check if bulk loading was successful
            if response.status_code in [200, 201]:
                # If successful, count all items as successful
                successful_items += len(batch)
                print(f"Bulk loaded {len(batch)} items successfully")

                continue

        except requests.exceptions.RequestException as e:
            print(f"Error during bulk loading attempt: {e}. Falling back to individual processing.")

        # Fall back to individual item processing if bulk loading failed
        for item in batch:
            # Convert the item to a dictionary
            item_dict = item.to_dict()

            try:
                # Add the item to the collection using the transaction API
                response = requests.post(
                    f"{api_url}/collections/{collection_id}/items", json=item_dict, headers={"Content-Type": "application/json"}
                )

                if response.status_code in [200, 201]:
                    successful_items += 1
                elif response.status_code == 409:
                    # Item already exists
                    print(f"Item {item.id} already exists in the collection")
                    if overwrite:
                        # Overwrite the existing item
                        response = requests.put(
                            f"{api_url}/collections/{collection_id}/items/{item.id}",
                            json=item_dict,
                            headers={"Content-Type": "application/json"},
                        )
                        if response.status_code in [200, 201]:
                            successful_items += 1
                        else:
                            failed_items += 1
                            print(f"Failed to overwrite item {item.id}. Status code: {response.status_code}")
                            print(f"Response: {response.text}")
                    else:
                        successful_items += 1
                else:
                    failed_items += 1
                    print(f"Failed to add item {item.id}. Status code: {response.status_code}")
                    print(f"Response: {response.text}")
            except requests.exceptions.RequestException as e:
                failed_items += 1
                print(f"Error adding item {item.id}: {e}")

    print(f"Added {successful_items} items successfully, {failed_items} items failed")
    return successful_items, failed_items


def delete_collection(api_url, collection_id) -> bool:
    """
    Delete a collection from the STAC API

    Parameters:
    - api_url: URL of the STAC API
    - collection_id: ID of the collection to delete

    Returns:
    - Boolean indicating if the collection was deleted successfully
    """
    try:
        # Check if the collection exists first
        if not check_collection_exists(api_url, collection_id):
            print(f"Collection '{collection_id}' does not exist, nothing to delete")
            return False

        # Delete the collection using the transaction API
        response = requests.delete(f"{api_url}/collections/{collection_id}", headers={"Content-Type": "application/json"})

        if response.status_code in [200, 202, 204]:
            print(f"Collection '{collection_id}' deleted successfully")
            return True
        else:
            print(f"Failed to delete collection '{collection_id}'. Status code: {response.status_code}")
            print(f"Response: {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error deleting collection: {e}")
        return False
