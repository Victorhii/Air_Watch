import pandas as pd
import geopandas as gpd
import osmnx as ox
import rasterio
from shapely.ops import nearest_points
from osmnx import _errors
import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# --- Geospatial and Elevation Functions (same as before) ---

def get_altitude(lat, lon):
    """Get altitude from the Open-Meteo Elevation API."""
    url = "https://api.open-meteo.com/v1/elevation"
    params = {
        "latitude": lat,
        "longitude": lon
    }
    try:
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()  # Raise an exception for HTTP errors (e.g., 404, 500)
        
        data = response.json()
        
        # The API returns a list with a single elevation value
        elevation = data.get("elevation")
        if elevation and isinstance(elevation, list):
            return elevation[0]
        else:
            raise ValueError("Invalid data format received from API.")

    except requests.exceptions.RequestException as e:
        print(f"Network error fetching altitude for ({lat}, {lon}): {e}")
        return None
    except (ValueError, KeyError) as e:
        print(f"Error processing response for ({lat}, {lon}): {e}")
        return None

def get_nasa_population_density(lat, lon, tif_path):
    """Fetches population density from a local GeoTIFF file."""
    if not os.path.exists(tif_path):
        # This warning is expected if the file isn't present
        return None
    try:
        with rasterio.open(tif_path) as src:
            value = next(src.sample([(lon, lat)]))[0]
            return round(float(value), 2) if value >= 0 else 0.0
    except (IndexError, rasterio.errors.RasterioIOError) as e:
        print(f"Error reading population data for ({lat}, {lon}): {e}")
        return None

def get_geospatial_features(lat, lon, tif_path):
    """Calculates distances to roads, industrial zones, and gets population/elevation."""
    point_of_interest = gpd.GeoDataFrame(
        [{'geometry': gpd.points_from_xy([lon], [lat])[0]}], crs="EPSG:4326"
    )
    utm_crs = point_of_interest.estimate_utm_crs()
    point_proj = point_of_interest.to_crs(utm_crs)
    
    SEARCH_RADIUS_METERS = 10000
    DEFAULT_DISTANCE_METERS = 12000
    
    
    try:
        G_roads = ox.graph_from_point((lat, lon), dist=SEARCH_RADIUS_METERS, network_type='all')
        gdf_roads_proj = ox.graph_to_gdfs(G_roads, nodes=False, edges=True).to_crs(utm_crs)
        all_roads = gdf_roads_proj.unary_union
        if not all_roads.is_empty:
            nearest_geom = nearest_points(point_proj.geometry.iloc[0], all_roads)
            distance_to_road = nearest_geom[0].distance(nearest_geom[1])
    except _errors.InsufficientResponseError:
        distance_to_road = DEFAULT_DISTANCE_METERS# Silently handle locations with no OSM data

    
    try:
        tags = {"landuse": "industrial"}
        gdf_industrial = ox.features_from_point((lat, lon), tags, dist=SEARCH_RADIUS_METERS)
        if not gdf_industrial.empty:
            gdf_industrial_proj = gdf_industrial.to_crs(utm_crs)
            all_industrial = gdf_industrial_proj.unary_union
            nearest_industrial_geom = nearest_points(point_proj.geometry.iloc[0], all_industrial)
            distance_to_industrial = nearest_industrial_geom[0].distance(nearest_industrial_geom[1])
    except _errors.InsufficientResponseError:
        distance_to_industrial = DEFAULT_DISTANCE_METERS# Silently handle locations with no industrial data

    return {
        'road': round(distance_to_road, 2),
        'industrial': round(distance_to_industrial, 2),
        'population': get_nasa_population_density(lat, lon, tif_path),
        'elev': get_altitude(lat, lon, )
    }

def process_row(index, row, tif_path):
    """Wrapper function for parallel processing."""
    geospatial_data = get_geospatial_features(row['lat'], row['lon'], tif_path)
    return index, geospatial_data

# --- Main Execution ---

if __name__ == "__main__":
    # --- 1. SETUP ---
    INPUT_CSV = 'data.csv'
    OUTPUT_CSV = 'train.csv'
    GPW_DENSITY_GEOTIFF_FILE = 'population.tif'
    MAX_WORKERS = 5
    
    # --- NEW: Checkpointing Setup ---
    CHECKPOINT_INTERVAL = 1 # Save after every 5 completed rows. Adjust as needed!
    completed_count = 0
    lock = threading.Lock()
    
        # --- 2. LOAD DATA & PREPARE COLUMNS ---
    # Load existing output file if it exists to resume progress
    try:
        df = pd.read_csv(OUTPUT_CSV)
        print(f"Resuming from existing file '{OUTPUT_CSV}'.")
    except FileNotFoundError:
        df = pd.read_csv(INPUT_CSV)
        print(f"Starting new processing for '{INPUT_CSV}'.")

    # --- THIS IS THE NEW SECTION TO ADD ---
    # Define the new columns you want to ensure exist
    new_columns = ['industrial', 'road', 'population', 'elev']

    # Find the position right after the 'lon' column to insert them
    try:
        # Get the integer index of the 'lon' column
        insert_location = df.columns.get_loc('lon') + 1
    except KeyError:
        print("Warning: 'lon' column not found. Appending new columns to the end.")
        insert_location = len(df.columns)

    # Iterate through the new columns in reverse to insert them correctly
    # This ensures 'industrial' comes first, then 'road', etc. at the insert_location
    for column in reversed(new_columns):
        # Only add the column if it doesn't already exist (important for resuming)
        if column not in df.columns:
            print(f"Header '{column}' not found. Creating it now.")
            df.insert(loc=insert_location, column=column, value=pd.NA)
    # --- END OF NEW SECTION ---

    # --- 3. PROCESS IN PARALLEL ---
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Create a list of tasks for rows that haven't been processed yet
        # We check if 'elev' is null/empty, assuming it's the last value to be filled
        tasks = {
            executor.submit(process_row, index, row, GPW_DENSITY_GEOTIFF_FILE): index
            for index, row in df.iterrows() if pd.isna(row.get('elev'))
        }
        
        print(f"Submitting {len(tasks)} unprocessed rows for analysis.")

        for future in as_completed(tasks):
            index = tasks[future]
            try:
                _, data = future.result()
                # Update the DataFrame in memory
                df.loc[index, ['industrial', 'road', 'population', 'elev']] = data.values()
                print(f"Successfully processed row {index}.")
                
                # --- NEW: Checkpoint Logic ---
                with lock:
                    completed_count += 1
                    if completed_count % CHECKPOINT_INTERVAL == 0:
                        print(f"--- CHECKPOINT: Saving progress ({completed_count} new rows processed) ---")
                        df.to_csv(OUTPUT_CSV, index=False)

            except Exception as exc:
                print(f"Row {index} generated an exception: {exc}")
    
    # --- 4. FINAL SAVE ---
    # Always save at the very end to capture any remaining rows
    print("\nProcessing complete. Performing final save.")
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Final results saved to '{OUTPUT_CSV}'")