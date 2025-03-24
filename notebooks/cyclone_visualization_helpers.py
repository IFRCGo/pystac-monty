"""
Helper functions for searching and visualizing tropical cyclones from IBTrACS data.
"""

import folium
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.colors import LinearSegmentedColormap
import numpy as np
import pandas as pd
from datetime import datetime
from IPython.display import display, HTML

import pystac
import pystac_client
from pystac_monty.extension import MontyExtension

def search_cyclones_by_name(name, catalog, collection_id="ibtracs-events", limit=10):
    """
    Search for tropical cyclones by name in the IBTrACS collection using pystac-client
    
    Parameters:
    - name: Name of the cyclone to search for (case-insensitive)
    - catalog: pystac_client.Client instance
    - collection_id: ID of the collection to search in
    - limit: Maximum number of results to return
    
    Returns:
    - List of pystac.Item objects matching the search criteria
    """
    if not catalog:
        print("STAC API client not available")
        return []
    
    try:
        # Create a search with the specified parameters
        search = catalog.search(
            collections=[collection_id],
            filter={"op": "like", "args": [{"property": "title"}, f"%{name.upper()}%"]},
            limit=limit
        )
        
        # Execute the search and get the items
        items = list(search.items())
        return items
    except Exception as e:
        print(f"Error searching for cyclones: {e}")
        return []

def get_cyclone_by_id(cyclone_id, catalog, collection_id="ibtracs-events"):
    """
    Get a specific cyclone by its ID using pystac-client
    
    Parameters:
    - cyclone_id: ID of the cyclone to retrieve
    - catalog: pystac_client.Client instance
    - collection_id: ID of the collection containing the cyclone
    
    Returns:
    - pystac.Item representing the cyclone, or None if not found
    """
    if not catalog:
        print("STAC API client not available")
        return None
    
    try:
        # Get the collection
        collection = catalog.get_collection(collection_id)
        
        # Get the item by ID
        item = collection.get_item(cyclone_id)
        return item
    except Exception as e:
        print(f"Error retrieving cyclone {cyclone_id}: {e}")
        return None

def get_cyclone_hazards(cyclone_event, catalog, hazard_collection_id="ibtracs-hazards"):
    """
    Get all hazard items related to a specific cyclone
    
    Parameters:
    - cyclone_event: pystac.Item representing the cyclone event
    - catalog: pystac_client.Client instance
    - hazard_collection_id: ID of the hazards collection
    
    Returns:
    - List of pystac.Item objects representing the cyclone's hazards, sorted by datetime
    """
    if not catalog or not cyclone_event:
        print("STAC API client or cyclone event not available")
        return []
    
    try:
        # Get the correlation ID from the cyclone event
        monty_ext = MontyExtension.ext(cyclone_event)
        correlation_id = monty_ext.correlation_id
        
        if not correlation_id:
            print("No correlation ID found in cyclone event")
            return []
        
        # Search for hazard items with the same correlation ID
        search = catalog.search(
            collections=[hazard_collection_id],
            filter={"op": "=", "args": [{"property": "monty:corr_id"}, correlation_id]},
            limit=100  # Increase limit to get all hazard points
        )
        
        # Get the hazard items
        hazard_items = list(search.items())
        
        # Sort the hazard items by datetime
        hazard_items.sort(key=lambda item: item.datetime if item.datetime else datetime.min)
        
        return hazard_items
    except Exception as e:
        print(f"Error retrieving hazards for cyclone: {e}")
        return []

def create_cyclone_evolution_map(cyclone_event, hazard_items):
    """
    Create an interactive map showing the evolution of a cyclone over time
    
    Parameters:
    - cyclone_event: pystac.Item representing the cyclone event
    - hazard_items: List of pystac.Item objects representing the cyclone's hazards
    
    Returns:
    - Folium map object
    """
    if not cyclone_event or not hazard_items:
        print("No cyclone or hazard data provided")
        return None
    
    # Get the cyclone title
    title = cyclone_event.properties.get("title", "Unknown Cyclone")
    
    # Create a map centered on the first hazard point
    first_hazard = hazard_items[0]
    first_coords = first_hazard.geometry.get("coordinates")
    m = folium.Map(location=[first_coords[1], first_coords[0]], zoom_start=4)
    
    # Add the cyclone track as a polyline
    track_points = []
    for item in hazard_items:
        coords = item.geometry.get("coordinates")
        # if the coords is a double list, then it is a LineString
        # and we take the last point of the LineString
        if isinstance(coords[0], list):
            coords = coords[-1]
        track_points.append([coords[1], coords[0]])  # [lat, lon] for folium
    
    folium.PolyLine(
        track_points,
        color='blue',
        weight=3,
        opacity=0.8,
        tooltip=f"{title} Track"
    ).add_to(m)
    
    # Add markers for each hazard point with wind speed information
    for item in hazard_items:
        coords = item.geometry.get("coordinates")
        dt = item.datetime.strftime("%Y-%m-%d %H:%M UTC") if item.datetime else "Unknown"
        
        # Get wind speed from Monty extension
        monty_ext = MontyExtension.ext(item)
        hazard_detail = monty_ext.hazard_detail if hasattr(monty_ext, 'hazard_detail') else None
        
        if hazard_detail and hasattr(hazard_detail, 'severity_value'):
            wind_speed = hazard_detail.severity_value
            wind_unit = hazard_detail.severity_unit if hasattr(hazard_detail, 'severity_unit') else "knots"
        else:
            wind_speed = "Unknown"
            wind_unit = ""
        
        # Determine marker color based on wind speed (Saffir-Simpson scale)
        if isinstance(wind_speed, (int, float)):
            if wind_speed >= 137:  # Category 5: >=137 knots
                color = 'darkred'
            elif wind_speed >= 113:  # Category 4: 113-136 knots
                color = 'red'
            elif wind_speed >= 96:  # Category 3: 96-112 knots
                color = 'orange'
            elif wind_speed >= 83:  # Category 2: 83-95 knots
                color = 'yellow'
            elif wind_speed >= 64:  # Category 1: 64-82 knots
                color = 'green'
            elif wind_speed >= 34:  # Tropical Storm: 34-63 knots
                color = 'blue'
            else:  # Tropical Depression: <34 knots
                color = 'lightblue'
        else:
            color = 'gray'
        
        # Create popup content
        popup_content = f"<b>{title}</b><br>"
        popup_content += f"<b>Time:</b> {dt}<br>"
        popup_content += f"<b>Wind Speed:</b> {wind_speed} {wind_unit}<br>"
        
        # if the coords is a double list, then it is a LineString
        # and we take the last point of the LineString
        if isinstance(coords[0], list):
            coords = coords[-1]
        
        # Add marker
        folium.CircleMarker(
            location=[coords[1], coords[0]],
            radius=5,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.8,
            popup=folium.Popup(popup_content, max_width=300),
            tooltip=f"{dt}: {wind_speed} {wind_unit}"
        ).add_to(m)
    
    # Add a simple legend using circle markers
    # Create a feature group for the legend
    legend = folium.FeatureGroup(name="Legend")
    
    # Add a title for the legend
    legend_title = folium.Marker(
        location=[first_coords[1] - 5, first_coords[0] - 5],
        icon=folium.DivIcon(
            icon_size=(150, 36),
            icon_anchor=(0, 0),
            html='<div style="font-size: 12pt; font-weight: bold;">Wind Speed Categories</div>'
        )
    )
    legend.add_child(legend_title)
    
    # Add legend items
    categories = [
        ("Category 5 (≥137 knots)", "darkred"),
        ("Category 4 (113-136 knots)", "red"),
        ("Category 3 (96-112 knots)", "orange"),
        ("Category 2 (83-95 knots)", "yellow"),
        ("Category 1 (64-82 knots)", "green"),
        ("Tropical Storm (34-63 knots)", "blue"),
        ("Tropical Depression (<34 knots)", "lightblue")
    ]
    
    for i, (label, color) in enumerate(categories):
        # Add a circle marker for each category
        folium.CircleMarker(
            location=[first_coords[1] - 5 - (i * 0.5), first_coords[0] - 5],
            radius=5,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.8,
            tooltip=label
        ).add_to(legend)
        
        # Add a label for each category
        folium.Marker(
            location=[first_coords[1] - 5 - (i * 0.5), first_coords[0] - 4.8],
            icon=folium.DivIcon(
                icon_size=(150, 36),
                icon_anchor=(0, 0),
                html=f'<div style="font-size: 10pt;">{label}</div>'
            )
        ).add_to(legend)
    
    # Add the legend to the map
    m.add_child(legend)
    
    return m

def create_cyclone_map(cyclone_item):
    """
    Create an interactive map showing the track of a tropical cyclone
    
    Parameters:
    - cyclone_item: pystac.Item representing a tropical cyclone
    
    Returns:
    - Folium map object
    """
    if not cyclone_item:
        print("No cyclone data provided")
        return None
    
    # Extract the cyclone track coordinates
    geometry = cyclone_item.geometry
    if geometry.get("type") != "LineString":
        print(f"Unexpected geometry type: {geometry.get('type')}")
        return None
    
    coordinates = geometry.get("coordinates", [])
    if not coordinates:
        print("No coordinates found in cyclone data")
        return None
    
    # Get the cyclone properties
    title = cyclone_item.properties.get("title", "Unknown Cyclone")
    description = cyclone_item.properties.get("description", "")
    start_date = cyclone_item.properties.get("start_datetime", "")
    end_date = cyclone_item.properties.get("end_datetime", "")
    
    # Format dates for display
    if start_date:
        start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00')).strftime('%Y-%m-%d')
    if end_date:
        end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00')).strftime('%Y-%m-%d')
    
    # Create a map centered on the middle of the cyclone track
    mid_point_index = len(coordinates) // 2
    mid_point = coordinates[mid_point_index]
    
    # Create the map
    m = folium.Map(location=[mid_point[1], mid_point[0]], zoom_start=4)
    
    # Add the cyclone track as a polyline
    # Convert coordinates from [lon, lat] to [lat, lon] for folium
    track_points = [[coord[1], coord[0]] for coord in coordinates]
    
    # Add the track line with a gradient color to show progression
    folium.PolyLine(
        track_points,
        color='blue',
        weight=3,
        opacity=0.8,
        tooltip=f"{title} ({start_date} to {end_date})"
    ).add_to(m)
    
    # Add markers for the start and end points
    folium.Marker(
        [track_points[0][0], track_points[0][1]],
        popup=f"Start: {start_date}",
        icon=folium.Icon(color='green', icon='play')
    ).add_to(m)
    
    folium.Marker(
        [track_points[-1][0], track_points[-1][1]],
        popup=f"End: {end_date}",
        icon=folium.Icon(color='red', icon='stop')
    ).add_to(m)
    
    # Add markers at regular intervals to show progression
    step = max(1, len(track_points) // 10)  # Add up to 10 markers along the track
    for i in range(step, len(track_points) - 1, step):
        folium.CircleMarker(
            track_points[i],
            radius=3,
            color='orange',
            fill=True,
            fill_color='orange',
            fill_opacity=0.8
        ).add_to(m)
    
    return m

def display_cyclone_info(cyclone_item):
    """
    Display information about a tropical cyclone using pystac and pystac-monty helpers
    
    Parameters:
    - cyclone_item: pystac.Item representing a tropical cyclone
    """
    if not cyclone_item:
        print("No cyclone data provided")
        return
    
    # Extract basic information
    title = cyclone_item.properties.get("title", "Unknown Cyclone")
    description = cyclone_item.properties.get("description", "No description available")
    start_date = cyclone_item.properties.get("start_datetime", "Unknown")
    end_date = cyclone_item.properties.get("end_datetime", "Unknown")
    
    # Format dates for display
    if start_date and start_date != "Unknown":
        start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00')).strftime('%Y-%m-%d')
    if end_date and end_date != "Unknown":
        end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00')).strftime('%Y-%m-%d')
    
    # Extract Monty extension properties using the MontyExtension helper
    monty_ext = MontyExtension.ext(cyclone_item)
    country_codes = monty_ext.country_codes or []
    hazard_codes = monty_ext.hazard_codes or []
    correlation_id = monty_ext.correlation_id or "Unknown"
    
    # Display the information
    print(f"\n{title}")
    print("=" * len(title))
    print(f"\nDescription: {description}")
    print(f"\nDuration: {start_date} to {end_date}")
    print(f"\nAffected Countries: {', '.join(country_codes)}")
    print(f"\nHazard Codes: {', '.join(hazard_codes)}")
    print(f"\nCorrelation ID: {correlation_id}")
    
    # Display any additional keywords
    keywords = cyclone_item.properties.get("keywords", [])
    if keywords:
        print(f"\nKeywords: {', '.join(keywords)}")

def search_and_display_cyclone(cyclone_name, catalog):
    """
    Search for a cyclone by name and display its information and track
    
    Parameters:
    - cyclone_name: Name of the cyclone to search for
    - catalog: pystac_client.Client instance
    """
    # Search for cyclones matching the name
    search_results = search_cyclones_by_name(cyclone_name, catalog)
    
    if not search_results:
        print(f"No cyclones found matching '{cyclone_name}'")
        return
    
    # Display the search results
    print(f"Found {len(search_results)} cyclones matching '{cyclone_name}':\n")
    
    # Create a DataFrame to display the results
    results_df = pd.DataFrame([
        {
            "ID": item.id,
            "Title": item.properties.get("title", "Unknown"),
            "Start Date": item.properties.get("start_datetime", "Unknown"),
            "End Date": item.properties.get("end_datetime", "Unknown"),
            "Countries": ", ".join(MontyExtension.ext(item).country_codes or [])
        }
        for item in search_results
    ])
    
    # Format dates for display
    for col in ["Start Date", "End Date"]:
        results_df[col] = results_df[col].apply(
            lambda x: datetime.fromisoformat(x.replace('Z', '+00:00')).strftime('%Y-%m-%d') if x != "Unknown" else x
        )
    
    display(results_df)
    
    # If there's only one result, display it automatically
    if len(search_results) == 1:
        cyclone_item = search_results[0]
        display_cyclone_info(cyclone_item)
        m = create_cyclone_map(cyclone_item)
        if m:
            display(m)
    else:
        # If there are multiple results, let the user select one
        print("\nEnter the ID of the cyclone you want to visualize in the next cell.")
        
    return search_results

def display_cyclone_by_id(cyclone_id, catalog):
    """
    Display information and track for a specific cyclone by ID
    
    Parameters:
    - cyclone_id: ID of the cyclone to display
    - catalog: pystac_client.Client instance
    """
    # Get the cyclone data
    cyclone_item = get_cyclone_by_id(cyclone_id, catalog)
    
    if not cyclone_item:
        print(f"No cyclone found with ID '{cyclone_id}'")
        return
    
    # Display the cyclone information
    display_cyclone_info(cyclone_item)
    
    # Create and display the map
    m = create_cyclone_map(cyclone_item)
    if m:
        display(m)

def visualize_cyclone_evolution(cyclone_id, catalog):
    """
    Visualize the evolution of a cyclone over time
    
    Parameters:
    - cyclone_id: ID of the cyclone to visualize
    - catalog: pystac_client.Client instance
    """
    # Get the cyclone event
    cyclone_event = get_cyclone_by_id(cyclone_id, catalog)
    if not cyclone_event:
        print(f"No cyclone found with ID '{cyclone_id}'")
        return
    
    # Get the cyclone hazards using the cyclone event object
    hazard_items = get_cyclone_hazards(cyclone_event, catalog)
    if not hazard_items:
        print(f"No hazard data found for cyclone '{cyclone_id}'")
        return
    
    # Display the number of hazard points
    print(f"Found {len(hazard_items)} hazard points for {cyclone_event.properties.get('title')}")
    
    # Create and display the evolution map
    print("\nCreating evolution map...")
    evolution_map = create_cyclone_evolution_map(cyclone_event, hazard_items)
    if evolution_map:
        display(evolution_map)
    
    # Create and display the animation
    print("\nCreating animation...")
    animation_html = create_cyclone_animation(cyclone_event, hazard_items)
    if animation_html:
        display(animation_html)

def create_cyclone_animation(cyclone_event, hazard_items):
    """
    Create an animation showing the evolution of a cyclone over time
    
    Parameters:
    - cyclone_event: pystac.Item representing the cyclone event
    - hazard_items: List of pystac.Item objects representing the cyclone's hazards
    
    Returns:
    - HTML animation
    """
    if not cyclone_event or not hazard_items:
        print("No cyclone or hazard data provided")
        return None
    
    # Get the cyclone title
    title = cyclone_event.properties.get("title", "Unknown Cyclone")
    
    # Extract coordinates and wind speeds
    lons = []
    lats = []
    wind_speeds = []
    dates = []
    
    for item in hazard_items:
        # if the coords is a double list, then it is a LineString
        # and we take the last point of the LineString
        coords = item.geometry.get("coordinates")
        if isinstance(coords[0], list):
            coords = coords[-1]
        
        lons.append(coords[0])
        lats.append(coords[1])
        
        # Get wind speed from Monty extension
        monty_ext = MontyExtension.ext(item)
        hazard_detail = monty_ext.hazard_detail if hasattr(monty_ext, 'hazard_detail') else None
        
        if hazard_detail and hasattr(hazard_detail, 'severity_value'):
            wind_speed = hazard_detail.severity_value
        else:
            wind_speed = 0
        
        wind_speeds.append(wind_speed)
        dates.append(item.datetime)
    
    # Create a custom colormap for wind speeds
    colors = ['lightblue', 'blue', 'green', 'yellow', 'orange', 'red', 'darkred']
    cmap = LinearSegmentedColormap.from_list('wind_speed', colors)
    
    # Create the figure and axis
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # Set the axis limits with some padding
    lon_padding = (max(lons) - min(lons)) * 0.1
    lat_padding = (max(lats) - min(lats)) * 0.1
    ax.set_xlim(min(lons) - lon_padding, max(lons) + lon_padding)
    ax.set_ylim(min(lats) - lat_padding, max(lats) + lat_padding)
    
    # Add coastlines and country borders
    ax.set_title(f"{title} Evolution")
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    
    # Plot the full track
    ax.plot(lons, lats, 'b-', alpha=0.3, linewidth=1)
    
    # Create a scatter plot for the cyclone position
    scatter = ax.scatter([], [], c=[], cmap=cmap, s=100, edgecolor='black')
    
    # Add a colorbar
    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label('Wind Speed (knots)')
    
    # Add a text annotation for the date
    date_text = ax.text(0.02, 0.95, '', transform=ax.transAxes, fontsize=12, 
                        bbox=dict(facecolor='white', alpha=0.8))
    
    # Add a text annotation for the wind speed
    wind_text = ax.text(0.02, 0.90, '', transform=ax.transAxes, fontsize=12,
                        bbox=dict(facecolor='white', alpha=0.8))
    
    # Define the animation update function
    def update(frame):
        # Update the scatter plot data
        scatter.set_offsets(np.column_stack((lons[:frame+1], lats[:frame+1])))
        scatter.set_array(np.array(wind_speeds[:frame+1]))
        
        # Update the date text
        if frame < len(dates):
            date_str = dates[frame].strftime("%Y-%m-%d %H:%M UTC")
            date_text.set_text(f'Date: {date_str}')
            
            # Update the wind speed text and add category
            wind_speed = wind_speeds[frame]
            category = ""
            if wind_speed >= 137:
                category = "Category 5"
            elif wind_speed >= 113:
                category = "Category 4"
            elif wind_speed >= 96:
                category = "Category 3"
            elif wind_speed >= 83:
                category = "Category 2"
            elif wind_speed >= 64:
                category = "Category 1"
            elif wind_speed >= 34:
                category = "Tropical Storm"
            else:
                category = "Tropical Depression"
            
            wind_text.set_text(f'Wind Speed: {wind_speed} knots ({category})')
        
        return scatter, date_text, wind_text
    
    # Create the animation
    anim = animation.FuncAnimation(fig, update, frames=len(lons), interval=200, blit=True)
    
    # Display the animation
    plt.close()  # Prevent the static plot from displaying
    return HTML(anim.to_jshtml())
