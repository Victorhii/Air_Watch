import pandas as pd
import geopandas as gpd
import osmnx as ox
import rasterio
from shapely.ops import nearest_points
from osmnx import _errors
import os
import requests
import json
from MODEL.predict import predict_single_instance

# --- Geospatial and Elevation Functions ---

def get_altitude(lat, lon):
    """Get altitude from the Open-Meteo Elevation API."""
    url = "https://api.open-meteo.com/v1/elevation"
    params = {"latitude": lat, "longitude": lon}
    try:
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()
        elevation = data.get("elevation")
        if elevation and isinstance(elevation, list):
            return elevation[0]
        else:
            raise ValueError("Invalid data format received from elevation API.")
    except requests.exceptions.RequestException as e:
        print(f"Network error fetching altitude for ({lat}, {lon}): {e}")
        return None
    except (ValueError, KeyError) as e:
        print(f"Error processing elevation response for ({lat}, {lon}): {e}")
        return None

def get_nasa_population_density(lat, lon, tif_path):
    """Fetches population density from a local GeoTIFF file."""
    if not os.path.exists(tif_path):
        print(f"Warning: Population TIF file not found at '{tif_path}'. Population will be None.")
        return None
    try:
        with rasterio.open(tif_path) as src:
            value = next(src.sample([(lon, lat)]))[0]
            return round(float(value), 2) if value >= 0 else 0.0
    except (IndexError, rasterio.errors.RasterioIOError) as e:
        print(f"Error reading population data for ({lat}, {lon}): {e}")
        return None

def get_geospatial_features(lat, lon, tif_path, road_gpkg_path):
    """
    Calculates distances to roads, industrial zones, and gets population/elevation.
    This function is called only once to get static data.
    """
    print("Fetching static geospatial data (roads, industrial, population, elevation)...")
    point_of_interest = gpd.GeoDataFrame(
        [{'geometry': gpd.points_from_xy([lon], [lat])[0]}], crs="EPSG:4326"
    )
    utm_crs = point_of_interest.estimate_utm_crs()
    point_proj = point_of_interest.to_crs(utm_crs)
    
    SEARCH_RADIUS_METERS = 8000
    DEFAULT_DISTANCE_METERS = 12000 # A default large distance if nothing is found
    
    distance_to_road = DEFAULT_DISTANCE_METERS
    distance_to_industrial = DEFAULT_DISTANCE_METERS
    
        # --- New Block for Reading Road GPKG file ---
    try:
        # 1. Read all road data from your local GPKG file
        gdf_roads_all = gpd.read_file(road_gpkg_path)
        
        # 2. Project the road data to the same UTM CRS for accurate distance calculation
        gdf_roads_all_proj = gdf_roads_all.to_crs(utm_crs)
        
        # 3. Create a buffer around your point to define the search area
        search_buffer = point_proj.geometry.buffer(SEARCH_RADIUS_METERS).iloc[0]
        
        # 4. Clip the roads to include only those within the search radius
        gdf_roads_within_radius = gpd.clip(gdf_roads_all_proj, search_buffer)

        if not gdf_roads_within_radius.empty:
            # 5. Merge all road geometries into one for efficient calculation
            all_roads = gdf_roads_within_radius.unary_union
            
            # 6. Find the nearest points and calculate the distance
            nearest_geom = nearest_points(point_proj.geometry.iloc[0], all_roads)
            distance_to_road = nearest_geom[0].distance(nearest_geom[1])
        else:
            print(f"No roads from GPKG file found within {SEARCH_RADIUS_METERS}m. Using default distance.")
            
    except Exception as e:
        print(f"Error processing road GPKG file '{road_gpkg_path}': {e}. Using default distance.")

    try:
        tags = {"landuse": "industrial"}
        gdf_industrial = ox.features_from_point((lat, lon), tags, dist=SEARCH_RADIUS_METERS)
        if not gdf_industrial.empty:
            gdf_industrial_proj = gdf_industrial.to_crs(utm_crs)
            all_industrial = gdf_industrial_proj.unary_union
            nearest_industrial_geom = nearest_points(point_proj.geometry.iloc[0], all_industrial)
            distance_to_industrial = nearest_industrial_geom[0].distance(nearest_industrial_geom[1])
    except _errors.InsufficientResponseError:
        print(f"No OSM industrial data found within {SEARCH_RADIUS_METERS}m. Using default distance.")

    return {
        'road': round(distance_to_road, 2),
        'industrial': round(distance_to_industrial, 2),
        'population': get_nasa_population_density(lat, lon, tif_path),
        'elev': get_altitude(lat, lon)
    }

# --- Weather Function ---

def get_weather_forecast(latitude, longitude):
    """
    Retrieves a 7-day weather forecast from Open-Meteo.
    Returns a Python dictionary, not a JSON string.
    """
    print("Fetching 7-day weather forecast...")
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": "temperature_2m_mean,relative_humidity_2m_mean,precipitation_sum",
        "timezone": "auto",
        "forecast_days": 7
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        forecast_data = {}
        daily_data = data.get("daily", {})
        
        dates = daily_data.get("time", [])
        temps = daily_data.get("temperature_2m_mean", [])
        humidity = daily_data.get("relative_humidity_2m_mean", [])
        precip_sum = daily_data.get("precipitation_sum", [])

        for i, date_str in enumerate(dates):
            try:
                avg_precip_mm_hr = precip_sum[i] / 24.0
                forecast_data[date_str] = {
                    "temp": round(temps[i], 2),
                    "rh": round(humidity[i], 2),
                    "prectot": round(avg_precip_mm_hr, 4)
                }
            except IndexError:
                print(f"Warning: Missing weather data for date {date_str}. Skipping this day.")
        
        return forecast_data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from weather API: {e}")
        return None
    except KeyError:
        print("Error: Unexpected data format received from the weather API.")
        return None

# --- Main Execution Block ---

def generate_combined_json(latitude, longitude, tif_path, road_gpkg_path):
    """
    Orchestrates fetching static and daily data and merges them.
    """
    # 1. Get the static geospatial data ONCE
    static_data = get_geospatial_features(latitude, longitude, tif_path, road_gpkg_path)
    if not static_data:
        print("Could not retrieve geospatial data. Aborting.")
        return None

    # 2. Get the 7-day weather forecast
    daily_weather = get_weather_forecast(latitude, longitude)
    if not daily_weather:
        print("Could not retrieve weather forecast. Aborting.")
        return None

    # 3. Combine the static data with each day's forecast
    final_forecast_list = []
    for date, weather in daily_weather.items():
        # Create a combined dictionary for the day
        daily_record = {
            "date": date,
            "lat": latitude,
            "lon": longitude,
            **static_data,  # Unpack the static data
            **weather     # Unpack the weather data for the specific day
        }
        final_forecast_list.append(daily_record)
        
    return final_forecast_list

import joblib

def predict_data(MODEL_FILE_PATH, SCALER_FILE_PATH, lat, lon, tif_path, road_gpkg_path):
    if not os.path.exists(MODEL_FILE_PATH) or not os.path.exists(SCALER_FILE_PATH):
        print(f"\n❌ Error: Model or scaler file not found. Make sure these paths are correct:\n- {MODEL_FILE_PATH}\n- {SCALER_FILE_PATH}")
    else:
        try:
            # 1. Load the model and scaler ONCE to improve efficiency
            model = joblib.load(MODEL_FILE_PATH)
            scaler = joblib.load(SCALER_FILE_PATH)
            print("✅ Model and scaler loaded successfully.\n")

            # 2. Create an empty dictionary to store the results
            all_predictions = {}
            combined_data = generate_combined_json(lat, lon, tif_path, road_gpkg_path)

            # 3. Loop through each JSON object in the list
            # --- TO THIS ---
            for data_point in combined_data:
                # Get the date to use as the key in our result dictionary
                prediction_date = data_point.get("date", "UnknownDate")
                
                # Call the prediction function for the current data point
                predicted_value = predict_single_instance(data_point, model, scaler)
                
                # Store the result
                all_predictions[prediction_date] = predicted_value
                

            # 4. Convert the final dictionary to a JSON formatted string and print it
            results_json = json.dumps(all_predictions, indent=4)
            return results_json

        except Exception as e:
            print(f"An error occurred during prediction: {e}")

