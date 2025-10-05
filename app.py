from flask import Flask, jsonify, render_template
import json
import random
from datetime import datetime, timedelta
import os
import requests
from datetime import datetime
import pytz
from timezonefinder import TimezoneFinder
from NRT_DATASET.HCHO.data_fetcher import fetch_and_manage_tempo_hcho_granules
from NRT_DATASET.NO2.data_fetcher import fetch_and_manage_tempo_no2_granules
from NRT_DATASET.O3.data_fetcher import fetch_and_manage_tempo_o3_granules
from NRT_DATASET.HCHO.point_value import get_hcho_value
from NRT_DATASET.NO2.point_value import get_no2_value
from NRT_DATASET.O3.point_value import get_o3_value
from NRT_DATASET.PM25.point_value import get_pm25_value
from fetch_forecast.fetch_all_forecast_data import predict_data
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from waitress import serve
from flask import session
import uuid # For generating random unique IDs
import csv # For handling CSV files
import math
import math
from flask import jsonify
app = Flask(__name__)
# This is crucial for Flask sessions to work. 
# In a production environment, use a more complex, securely stored key.
app.secret_key = os.urandom(24)
GEOAPIFY_KEY = os.environ.get("GEOAPIFY_KEY")


LATITUDE = 38.89511 #WASHINGTON DC
LONGITUDE = -77.03637

# Load health guidance data from JSON file
with open('./config/guidance.json') as f:
    guidance_data = json.load(f)


import threading
import time
# --- ADDED: Function to manage user ID and CSV file ---
def manage_user_id():
    """
    Checks for a user ID in the session. If not present, it creates one
    and generates a new CSV file named after the user ID with a specific header
    for storing user data.
    """
    if 'user_id' not in session:
        # Generate a new, unique user ID
        user_id = str(uuid.uuid4())
        session['user_id'] = user_id
        
        # Define the directory to store user files
        user_dir = 'user'
        
        # Create the 'user' directory if it doesn't already exist
        os.makedirs(user_dir, exist_ok=True)
        
        # --- CHANGE: The CSV filename is now the user's ID ---
        csv_file_path = os.path.join(user_dir, f'{user_id}.csv')
        
        # --- CHANGE: Define the required headers ---
        headers = ['date', 'no2', 'o3', 'hcho', 'pm25', 'day']
        
        # Create the new user-specific CSV file and write only the header
        # We use 'w' (write mode) because this file is brand new.
        with open(csv_file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Write the new header row into the file
            writer.writerow(headers)
            
        print(f"New user detected. Assigned ID: {user_id}")
        print(f"Created a new data file for this user at: {csv_file_path}")
    else:
        print(f"Returning user detected. User ID: {session['user_id']}")

# --- ADDED: This function will run before every request ---
@app.before_request
def before_request_handler():
    """Runs before each request to ensure a user ID is managed."""
    manage_user_id()
def background_tasks():
    """A function to run our fetching tasks on a loop."""
    while True:
        time.sleep(1200)
        print("Running background data fetch...")
        fetch_and_manage_tempo_o3_granules()
        fetch_and_manage_tempo_hcho_granules()
        fetch_and_manage_tempo_no2_granules()
        # Wait for an hour (3600 seconds) before running again
        
def convert_coordinates(lat, lon):
    """Converts decimal lat/lon to N/S/E/W format."""

     # Determine latitude direction
    lat_direction = "N" if lat >= 0 else "S"
 # Determine longitude direction
    lon_direction = "E" if lon >= 0 else "W"

# Format the final strings using the absolute values
    formatted_lat = f"{abs(lat):.4f}° {lat_direction}"
    formatted_lon = f"{abs(lon):.4f}° {lon_direction}"
    return formatted_lat, formatted_lon
def parse_coordinate(coord_string):
    """
    Converts a coordinate string in N/S/E/W format to a decimal value.
    
    Args:
        coord_string (str): The coordinate string, e.g., "52.5200° N" or "13.4050° W".

    Returns:
        float: The coordinate as a decimal value.
    """
    # 1. Split the string into the number and the direction
    # Example: "52.5200° N" becomes ["52.5200°", "N"]
    parts = coord_string.split()
    
    # 2. Get the numeric value by removing the degree symbol '°' and converting to a float
    decimal_value = float(parts[0].replace('°', ''))
    
    # 3. Get the direction character
    direction = parts[1].upper() # Use .upper() to handle 'n' or 's'
    
    # 4. If the direction is South or West, make the decimal value negative
    if direction in ['S', 'W']:
        decimal_value *= -1
        
    return decimal_value
def get_aqi_category(pm25):
    """ Get AQI category based on PM2.5 value """
    if 0 <= pm25 <= 12:
        return "Good", "Good"
    elif 12.1 <= pm25 <= 35.4:
        return "Moderate", "Moderate"
    elif 35.5 <= pm25 <= 55.4:
        return "Unhealthy for Sensitive Groups", "Unhealthy for Sensitive Groups"
    elif 55.5 <= pm25 <= 150.4:
        return "Unhealthy", "Unhealthy"
    elif 150.5 <= pm25 <= 250.4:
        return "Very Unhealthy", "Very Unhealthy"
    else:
        return "Hazardous", "Hazardous"
def get_hcho_category(hcho):
    """
    Determines the AQI category based on the Formaldehyde (HCHO) value.
    Unit: x 10¹⁶ molecules/cm²

    Args:
        hcho (float): The HCHO concentration value.

    Returns:
        str: The corresponding AQI category name.
    """
    if 0.0 <= hcho <= 0.4:
        return "Good"
    elif 0.41 <= hcho <= 0.8:
        return "Moderate"
    elif 0.81 <= hcho <= 1.2:
        return "Unhealthy for Sensitive Groups"
    elif 1.21 <= hcho <= 2.0:
        return "Unhealthy"
    elif 2.01 <= hcho <= 3.0:
        return "Very Unhealthy"
    elif hcho > 3.0:
        return "Hazardous"
    else:

        return "Invalid input"

def get_o3_category(o3):
    """
    Determines the AQI category based on the Tropospheric Ozone (O3) value.
    Unit: DU (Dobson Units)

    Args:
        o3 (float): The O3 concentration value.

    Returns:
        str: The corresponding AQI category name.
    """
    if 0 <= o3 <= 54:
        return "Good"
    elif 55 <= o3 <= 70:
        return "Moderate"
    elif 71 <= o3 <= 85:
        return "Unhealthy for Sensitive Groups"
    elif 86 <= o3 <= 105:
        return "Unhealthy"
    elif 106 <= o3 <= 200:
        return "Very Unhealthy"
    elif o3 > 200:
        return "Hazardous"
    else:
        return "Invalid input"

def get_no2_category(no2):
    """
    Determines the AQI category based on the Tropospheric Nitrogen Dioxide (NO2) value.
    Unit: x 10¹⁶ molecules/cm²

    Args:
        no2 (float): The NO2 concentration value.

    Returns:
        str: The corresponding AQI category name.
    """
    if 0.0 <= no2 <= 0.1:
        return "Good"
    elif 0.11 <= no2 <= 0.3:
        return "Moderate"
    elif 0.31 <= no2 <= 0.6:
        return "Unhealthy for Sensitive Groups"
    elif 0.61 <= no2 <= 1.0:
        return "Unhealthy"
    elif 1.01 <= no2 <= 2.0:
        return "Very Unhealthy"
    elif no2 > 2.0:
        return "Hazardous"
    else:
        return "Invalid input"
    
def get_pollutant_level(value, levels_config):

    for level, ranges in levels_config.items():
        max_val = ranges["max"]
        # Use a large number for an unbounded upper limit (Hazardous)
        if max_val is None:
            max_val = float('inf')
        if ranges["min"] <= value <= max_val:
            return level, ranges
    return None, None
    
def get_local_time_short_format_pytz(latitude, longitude):
    """
    Gets the current local time (HH:MM) for a given latitude and longitude 
    using timezonefinder and pytz.
    """
    # 1. Find the Timezone Name
    tf = TimezoneFinder()
    timezone_name = tf.timezone_at(lng=longitude, lat=latitude)

    if timezone_name is None:
        return "Error: Could not determine timezone for these coordinates."

    # 2. Get the current time in that Timezone using pytz
    try:
        # Get the timezone object using pytz
        target_tz = pytz.timezone(timezone_name)

        # Get the current time, making it UTC-aware using pytz.utc
        utc_now = datetime.utcnow().replace(tzinfo=pytz.utc)

        # Convert UTC time to the local time zone
        local_time = utc_now.astimezone(target_tz)

        # Apply the HH:MM format (24-hour time)
        short_time_format = local_time.strftime('%H:%M')
        
        return short_time_format

    except Exception as e:
        # Catch potential errors like an invalid timezone name
        return f"An error occurred during time conversion: {e}"
    
def get_local_date_yyyy_mm_dd(latitude, longitude):
    """
    Gets the current local date (YYYY-MM-DD) for a given latitude and longitude 
    using timezonefinder and pytz.
    """
    # 1. Find the Timezone Name
    tf = TimezoneFinder()
    timezone_name = tf.timezone_at(lng=longitude, lat=latitude)

    if timezone_name is None:
        return "Error: Could not determine timezone for these coordinates."

    # 2. Get the current date in that Timezone using pytz
    try:
        # Get the timezone object using pytz
        target_tz = pytz.timezone(timezone_name)

        # Get the current time, making it UTC-aware using pytz.utc
        utc_now = datetime.utcnow().replace(tzinfo=pytz.utc)

        # Convert UTC time to the local time zone
        local_time = utc_now.astimezone(target_tz)

        # Apply the YYYY-MM-DD format
        date_format = local_time.strftime('%Y-%m-%d')
        
        return date_format

    except Exception as e:
        # Catch potential errors like an invalid timezone name
        return f"An error occurred during time conversion: {e}"



def update_user_forecast_data(pm25_forecast, no2_forecast, o3_forecast, hcho_forecast):
    """
    Updates the user's specific CSV file with the latest forecast data.
    
    This function will completely overwrite any existing data in the file,
    ensuring it always contains the most current forecast.
    """
    # 1. Check for user_id in session and construct the file path
    if 'user_id' not in session:
        print("Error: Could not find user_id in session. Cannot save data.")
        return

    user_id = session['user_id']
    user_dir = 'user'
    csv_file_path = os.path.join(user_dir, f'{user_id}.csv')
    
    # Ensure the directory exists (as a safeguard)
    os.makedirs(user_dir, exist_ok=True)

    # 2. Prepare the data for CSV writing
    # Get the list of dates from one of the forecast dictionaries (they are all the same)
    dates = sorted(pm25_forecast.keys())
    
    # NEW: Added 'day' to the end of the header
    header = ['date', 'no2', 'o3', 'hcho', 'pm25', 'day']
    
    rows_to_write = []
    for date in dates:
        # NEW: Get the day of the week from the date string
        # This assumes your date is in 'YYYY-MM-DD' format.
        date_object = datetime.strptime(date, '%Y-%m-%d')
        day_of_week = date_object.strftime('%A')  # '%A' gives the full day name (e.g., "Monday")
        
        # Create a row for each date with data from all pollutants
        row = [
            date,
            no2_forecast.get(date, ''),   # .get() is safer than direct access
            o3_forecast.get(date, ''),
            hcho_forecast.get(date, ''),
            pm25_forecast.get(date, ''),
            day_of_week  # NEW: Added the day name to the row
        ]
        rows_to_write.append(row)

    # 3. Write the data to the CSV file
    # We use 'w' (write mode) to completely overwrite the file.
    try:
        with open(csv_file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(header)       # First, write the header row
            writer.writerows(rows_to_write) # Then, write all the new data rows
        
        print(f"Successfully updated forecast data in: {csv_file_path}")

    except IOError as e:
        print(f"Error writing to file {csv_file_path}: {e}")


def get_pollutant_data_for_day(user_id, day_of_week):
    """
    Reads a user-specific CSV file to find and return pollutant data for a specific day.

    Args:
        user_id (str): The ID of the user, used to find the correct CSV file.
        day_of_week (str): The day to look for in the CSV (e.g., 'monday').

    Returns:
        dict: A dictionary with pollutant data if the day is found, otherwise an empty dict.
    """
    # Define the path to the folder containing user CSV files.
    # Make sure you have a folder named 'user_data' in the same directory as your app.py.
    data_folder = 'user'
    file_path = os.path.join(data_folder, f'{user_id}.csv')

    # Check if the user's data file actually exists before trying to open it.
    if not os.path.isfile(file_path):
        print(f"Warning: Data file not found for user '{user_id}' at path: {file_path}")
        return {}

    try:
        with open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
            # Use DictReader to easily access columns by their header name.
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Compare the day from the CSV with the requested day (case-insensitive).
                if row.get('day', '').lower() == day_of_week.lower():
                    # If we find a match, create the forecast dictionary.
                    # We convert values to float because they are read as strings from the CSV.
                    forecast = {
                        "pm25": float(row.get('pm25', 0)),
                        "no2": float(row.get('no2', 0)),
                        "o3": float(row.get('o3', 0)),
                        "hcho": float(row.get('hcho', 0))
                    }
                    return forecast # Return the data for the first matching day.
    except (FileNotFoundError, ValueError, KeyError) as e:
        # Handle potential errors: file not found, value can't be converted to float, or a key is missing.
        print(f"Error reading or processing file {file_path}: {e}")
        return {}

    # Return an empty dictionary if the requested day was not found in the CSV.
    return {}
def get_recommendation_for_forecast(forecast):
    pm25 = forecast.get('pm25', 0)
    if pm25 > 40:
        return recommendation_data['poor']
    elif pm25 > 12:
        return recommendation_data['moderate']
    else:
        return recommendation_data['excellent']
    
import pandas as pd
import geopandas as gpd
import osmnx as ox
import rasterio
from shapely.ops import nearest_points
from osmnx import _errors
import os
import requests
import json


# --- Data Loading ---
def load_json_file(filename):
    with open(filename, 'r') as f:
        return json.load(f)

# Load all data at startup
schedule_data = load_json_file('./config/schedule.json')
recommendation_data = load_json_file('./config/recommendations.json')

@app.route('/')
def index():
    """ Renders the main HTML page. """
    return render_template('index.html')

@app.route('/school')
def school():
    # MODIFIED: Now we pass initial forecast data for Monday to the template
    days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    user_id = session.get('user_id')
    initial_day = "Monday" 

    # Construct the path to the user's specific CSV file
    file_path = os.path.join('user', f'{user_id}.csv')
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)
        # Check if the file (dataframe) is not empty
        if not df.empty:
            # Get the value from the first row, first column (iloc[0, 0])
            # .strip() removes whitespace and .lower() ensures consistent casing
            day_from_csv = str(df.loc[0, 'day']).strip()

            # Validate that the value from the CSV is a valid day
            if day_from_csv in [d for d in days_of_week]:
                initial_day = day_from_csv

    except (FileNotFoundError, pd.errors.EmptyDataError, IndexError):
        # If file doesn't exist, is empty, or has no rows, 
        # we'll just use the default 'monday'.
        # You could add logging here for debugging purposes.
        print(f"Info: Could not load schedule for user '{user_id}'. Defaulting to Monday.")
    initial_schedule = schedule_data.get(initial_day.lower(), [])
    initial_forecast = get_pollutant_data_for_day(user_id, initial_day)
    initial_recommendation = get_recommendation_for_forecast(initial_forecast)
    return render_template('school_insights.html', 
                        days=days_of_week,
                        initial_schedule=initial_schedule, 
                        initial_forecast=initial_forecast,
                        initial_day=initial_day,
                        initial_recommendation=initial_recommendation)

# --- MODIFIED FLASK ROUTE ---
@app.route('/schedule/<string:day>')
def get_schedule(day):
    # For demonstration, we'll set a user_id in the session.
    # In a real app, this would happen when the user logs in.
    user_id = session.get('user_id')

    if not user_id:
        return jsonify({"error": "User not logged in or session expired"}), 401
    
    day_lower = day.lower()
    
    # Get schedule data from the mock dictionary
    day_schedule = schedule_data.get(day_lower, [])
    
    # MODIFIED: Fetch forecast by reading the user's CSV file
    day_forecast = get_pollutant_data_for_day(user_id, day_lower)
    day_recommendation = get_recommendation_for_forecast(day_forecast)
    
    # Combine both into a single response object
    response_data = {
        "schedule": day_schedule,
        "forecast": day_forecast,
        "recommendation": day_recommendation
    }
    
    return jsonify(response_data)


@app.route('/api/location-data/<mode>/<latitude>/<longitude>')
def location_data(mode, latitude, longitude):
    """ Provides location and time data. """
    if mode == 'initial':
        latitude = LATITUDE
        longitude = LONGITUDE
        try:
            # server-side request to Geoapify
            resp = requests.get(
                "https://api.geoapify.com/v1/geocode/reverse",
                params={
                    "lat": latitude,
                    "lon": longitude,
                    "apiKey": GEOAPIFY_KEY
                },
                timeout=5
            )
            resp.raise_for_status()
            data = resp.json()
            

            # Grab the first feature (if any) and its properties
            features = data.get("features", [])
            props    = features[0].get("properties", {}) if features else {}

            city    = props.get("city")    or props.get("address_line1") or None
            state   = props.get("state")   or props.get("region")        or None
            country = props.get("country")                                 or None
            time = get_local_time_short_format_pytz(latitude, longitude)
            lat, lon = convert_coordinates(latitude, longitude)

            return jsonify({
                "city": city,
                "state": state,
                'lat': lat,
                'lon': lon,
                'local_time': time
            })
        
        except requests.RequestException as e:
            return jsonify({ "error": str(e) }), 500
    elif mode == 'update' or 'same':
        try:
            # server-side request to Geoapify
            resp = requests.get(
                "https://api.geoapify.com/v1/geocode/reverse",
                params={
                    "lat": latitude,
                    "lon": longitude,
                    "apiKey": GEOAPIFY_KEY
                },
                timeout=5
            )
            resp.raise_for_status()
            data = resp.json()
            latitude = float(latitude)
            longitude = float(longitude)
            

            # Grab the first feature (if any) and its properties
            features = data.get("features", [])
            props    = features[0].get("properties", {}) if features else {}

            city    = props.get("city")    or props.get("address_line1") or None
            state   = props.get("state")   or props.get("region")        or None
            country = props.get("country")                                 or None
            time = get_local_time_short_format_pytz(latitude, longitude)
            print(f'{latitude}, {longitude}')
            lat, lon = convert_coordinates(latitude, longitude)
            
            return jsonify({
                "city": city,
                "state": state,
                'lat': lat,
                'lon': lon,
                'local_time': time
            })
        
        except requests.RequestException as e:
            return jsonify({ "error": str(e) }), 500







#
# Remember to add this import at the top of your file
# from concurrent.futures import ThreadPoolExecutor
#

@app.route('/api/air-quality-data/<mode>/<latitude>/<longitude>')
def air_quality_data(mode, latitude, longitude):
    if mode == 'update' or 'initial':
        """ Provides current air quality and forecast data for different pollutants. """
        lat = parse_coordinate(latitude)
        lon = parse_coordinate(longitude)
        # The 'now' variable is not used in the original code, but I'm leaving it here.
        # now = datetime.now()

        # The with statement ensures threads are properly managed
        with ThreadPoolExecutor() as executor:
            # --- SUBMIT ALL TASKS TO RUN IN PARALLEL ---
            # Instead of calling the function and waiting, executor.submit() starts it
            # and immediately returns a "Future" object, which is a promise of a result.
            
            # PM2.5 tasks
            pm25_data_future = executor.submit(get_pm25_value, lat, lon)
            pm25_forecast_future = executor.submit(predict_data, './MODEL/pm25_model.joblib', './MODEL/pm25_scalar.joblib', lat, lon, './MODEL/pm25.tif', './MODEL/pm25.gpkg')

            # NO2 tasks
            no2_data_future = executor.submit(get_no2_value, lat, lon)
            no2_forecast_future = executor.submit(predict_data, './MODEL/no2_model.joblib', './MODEL/no2_scalar.joblib', lat, lon, './MODEL/no2.tif', './MODEL/no2.gpkg')

            # O3 tasks
            o3_data_future = executor.submit(get_o3_value, lat, lon)
            o3_forecast_future = executor.submit(predict_data, './MODEL/o3_model.joblib', './MODEL/o3_scalar.joblib', lat, lon, './MODEL/o3.tif', './MODEL/o3.gpkg')

            # HCHO tasks
            hcho_data_future = executor.submit(get_hcho_value, lat, lon)
            hcho_forecast_future = executor.submit(predict_data, './MODEL/hcho_model.joblib', './MODEL/hcho_scalar.joblib', lat, lon, './MODEL/hcho.tif', './MODEL/hcho.gpkg')

            # --- RETRIEVE THE RESULTS ---
            # Now, we call .result() on each Future object. This will wait for the
            # specific task to finish and give you its return value. Since they all
            # ran in parallel, you're only waiting for the longest one to complete.

            # PM2.5 results
            pm25_data, pm25_unit = pm25_data_future.result()
            pm25_forecast_data = pm25_forecast_future.result()
            pm25_forecast_data = json.loads(pm25_forecast_data)
            print(pm25_forecast_data)
            
            # NO2 results
            no2_data, no2_instrument, no2_unit = no2_data_future.result()
            no2_forecast_data = no2_forecast_future.result()
            no2_forecast_data = json.loads(no2_forecast_data)

            # O3 results
            o3_data, o3_instrument, o3_unit = o3_data_future.result()
            o3_forecast_data = o3_forecast_future.result()
            o3_forecast_data = json.loads(o3_forecast_data)

            # HCHO results
            hcho_data, hcho_instrument, hcho_unit = hcho_data_future.result()
            hcho_forecast_data = hcho_forecast_future.result()
            hcho_forecast_data = json.loads(hcho_forecast_data)

            update_user_forecast_data(
                pm25_forecast=pm25_forecast_data,
                no2_forecast=no2_forecast_data,
                o3_forecast=o3_forecast_data,
                hcho_forecast=hcho_forecast_data
            )

        # --- ALL DATA PROCESSING REMAINS EXACTLY THE SAME ---
        # This part of your code doesn't need to change at all. It just uses the
        # variables that we've now populated in parallel.

        # --- PM2.5 Data ---
        pm25_current = str(pm25_data) + pm25_unit
        
        pm25_forecast = [
            {"time": datetime.strptime(date_str, "%Y-%m-%d").strftime("%b %d"), "value": value}
            for date_str, value in pm25_forecast_data.items()
        ]
            
        # --- NO2 Data ---
        no2_current = str(no2_data) + no2_unit
        
        no2_forecast = [
            {"time": datetime.strptime(date_str, "%Y-%m-%d").strftime("%b %d"), "value": value}
            for date_str, value in no2_forecast_data.items()
        ]

        # --- O3 Data ---
        o3_current = str(o3_data) + o3_unit
        
        o3_forecast = [
            {"time": datetime.strptime(date_str, "%Y-%m-%d").strftime("%b %d"), "value": value}
            for date_str, value in o3_forecast_data.items()
        ]
        
        # --- HCHO Data ---
        hcho_current = str(hcho_data) + hcho_unit
        
        hcho_forecast = [
            {"time": datetime.strptime(date_str, "%Y-%m-%d").strftime("%b %d"), "value": value}
            for date_str, value in hcho_forecast_data.items()
        ]

        aqi_level_text, aqi_category = get_aqi_category(pm25_data)
        no2_category = get_no2_category(no2_data)
        o3_category = get_o3_category(o3_data)
        hcho_category = get_hcho_category(hcho_data)
        
        return jsonify({
            "pollutants": {
                "PM2.5": {"current": pm25_current, "forecast": pm25_forecast, "level": aqi_level_text},
                "NO2": {"current": no2_current, "forecast": no2_forecast, "level": no2_category},
                "O3": {"current": o3_current, "forecast": o3_forecast, "level": o3_category},
                "HCHO": {"current": hcho_current, "forecast": hcho_forecast, "level": hcho_category}
            },
            "guidance": guidance_data.get(aqi_category, [])
        })
    elif mode == 'same':
        user_id = session.get('user_id')

        # Define the path to the user's CSV file
        # This looks for a file like 'user/some_user_id.csv'
        file_path = os.path.join('user', f'{user_id}.csv')
        with ThreadPoolExecutor() as executor:
            # --- SUBMIT ALL TASKS TO RUN IN PARALLEL ---
            # Instead of calling the function and waiting, executor.submit() starts it
            # and immediately returns a "Future" object, which is a promise of a result.
            
            # PM2.5 tasks
            pm25_data_future = executor.submit(get_pm25_value, lat, lon)

            # NO2 tasks
            no2_data_future = executor.submit(get_no2_value, lat, lon)

            # O3 tasks
            o3_data_future = executor.submit(get_o3_value, lat, lon)

            # HCHO tasks
            hcho_data_future = executor.submit(get_hcho_value, lat, lon)

            # --- RETRIEVE THE RESULTS ---
            # Now, we call .result() on each Future object. This will wait for the
            # specific task to finish and give you its return value. Since they all
            # ran in parallel, you're only waiting for the longest one to complete.

            # PM2.5 results
            pm25_data, pm25_unit = pm25_data_future.result()

            
            # NO2 results
            no2_data, no2_instrument, no2_unit = no2_data_future.result()


            # O3 results
            o3_data, o3_instrument, o3_unit = o3_data_future.result()


            # HCHO results
            hcho_data, hcho_instrument, hcho_unit = hcho_data_future.result()


        # Initialize dictionaries to hold the raw data for each pollutant
        pollutant_data = {
            'no2': {},
            'o3': {},
            'hcho': {},
            'pm25': {}
        }

        try:
            # Open the user's CSV file to read its content
            with open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile) # Use DictReader for easier column access
                for row in reader:
                    date_str = row['date']
                    # Populate the dictionaries, converting pollutant values to float
                    pollutant_data['no2'][date_str] = float(row['no2'])
                    pollutant_data['o3'][date_str] = float(row['o3'])
                    pollutant_data['hcho'][date_str] = float(row['hcho'])
                    pollutant_data['pm25'][date_str] = float(row['pm25'])

        except FileNotFoundError:
            print(f"Error: The file {file_path} was not found.")
            # If the file doesn't exist, create empty lists
            no2_forecast, o3_forecast, hcho_forecast, pm25_forecast = [], [], [], []
        except (ValueError, KeyError) as e:
            print(f"Error processing file {file_path}: Invalid data format or missing column. Details: {e}")
            # Handle cases with bad data or incorrect headers
            no2_forecast, o3_forecast, hcho_forecast, pm25_forecast = [], [], [], []
        else:
            # Transform the pollutant data into the desired list format using your template
            
            # NO2 forecast list
            no2_forecast = [
                {"time": datetime.strptime(date_str, "%Y-%m-%d").strftime("%b %d"), "value": value}
                for date_str, value in pollutant_data['no2'].items()
            ]

            # O3 forecast list
            o3_forecast = [
                {"time": datetime.strptime(date_str, "%Y-%m-%d").strftime("%b %d"), "value": value}
                for date_str, value in pollutant_data['o3'].items()
            ]

            # HCHO forecast list
            hcho_forecast = [
                {"time": datetime.strptime(date_str, "%Y-%m-%d").strftime("%b %d"), "value": value}
                for date_str, value in pollutant_data['hcho'].items()
            ]

            # PM2.5 forecast list
            pm25_forecast = [
                {"time": datetime.strptime(date_str, "%Y-%m-%d").strftime("%b %d"), "value": value}
                for date_str, value in pollutant_data['pm25'].items()
            ]
        pm25_current = str(pm25_data) + pm25_unit
        no2_current = str(no2_data) + no2_unit
        o3_current = str(o3_data) + o3_unit    
        hcho_current = str(hcho_data) + hcho_unit
        aqi_level_text, aqi_category = get_aqi_category(pm25_data)
        
        return jsonify({
            "pollutants": {
                "PM2.5": {"current": pm25_current, "forecast": pm25_forecast, "level": aqi_level_text},
                "NO2": {"current": no2_current, "forecast": no2_forecast, "level": "Good"},
                "O3": {"current": o3_current, "forecast": o3_forecast, "level": "Good"},
                "HCHO": {"current": hcho_current, "forecast": hcho_forecast, "level": "Good"}
            },
            "guidance": guidance_data.get(aqi_category, [])
        })

    # Now you have four variables ready to use:
    # no2_forecast, o3_forecast, hcho_forecast, pm25_forecast
    #
    # print("PM2.5 Forecast:", pm25_forecast)
    # Example output for pm25_forecast:
    # [{'time': 'Oct 04', 'value': 12.44}, {'time': 'Oct 05', 'value': 13.13}, ...]
@app.route('/api/pollutant-info')
def get_pollutant_info():
    """
    Provides detailed information about various air pollutants.
    """
    pollutant_data = {
        "PM2.5": {
            "name": "Fine particulate matter",
            "risks": [
                "Can penetrate deep into lungs and bloodstream",
                "Increases risk of heart disease and stroke",
                "Aggravates asthma and respiratory conditions",
                "Particularly harmful to children and elderly"
            ]
        },
        "NO2": {
            "name": "Nitrogen dioxide",
            "risks": [
                "Irritates airways in lungs",
                "Increases susceptibility to respiratory infections",
                "Can worsen asthma symptoms",
                "Long-term exposure may decrease lung function"
            ]
        },
        "O3": {
            "name": "Ground-level ozone",
            "risks": [
                "Irritates airways causing coughing and throat irritation",
                "Reduces lung function temporarily",
                "Worsens asthma symptoms",
                "Can cause permanent lung damage with long exposure"
            ]
        },
        "HCHO": {
            "name": "Formaldehyde",
            "risks": [
                "Irritates eyes, nose, throat, and skin",
                "Can cause watery eyes and burning sensations",
                "May trigger asthma symptoms",
                "Prolonged exposure linked to cancer risk"
            ]
        }
    }
    return jsonify(pollutant_data)

@app.route('/api/weather-data/<latitude>/<longitude>')
def weather_data(latitude, longitude):
    """Provides current weather data from WeatherAPI.com."""
    latitude = parse_coordinate(latitude)
    longitude = parse_coordinate(longitude)
    # Best practice: Store your API key as an environment variable
    WEATHERTAPI_APIKEY = os.getenv('WEATHERTAPI_APIKEY')

    if not WEATHERTAPI_APIKEY:
        return jsonify({"error": "WeatherAPI key not found. Please set the WEATHERAPI_KEY environment variable."}), 500

    try:
        # Define the API endpoint and parameters for WeatherAPI.com
        api_url = "http://api.weatherapi.com/v1/current.json"
        params = {
            "key": WEATHERTAPI_APIKEY,
            "q": f"{latitude},{longitude}", # Location query as "lat,lon"
            "aqi": "no" # Air Quality Data is not needed
        }

        # Make the API request
        response = requests.get(api_url, params=params)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        
        data = response.json()
        current_weather = data.get("current", {})

        # Prepare the response payload from the data structure of WeatherAPI.com
        return jsonify({
            "temperature": current_weather.get("temp_c"),
            "wind_speed": current_weather.get("wind_kph"),
            "precipitation": current_weather.get("precip_mm") # Precipitation in mm in the last hour
            # "rh": current_weather.get("humidity")
        })


    except requests.exceptions.RequestException as e:
        # Handle network errors or bad responses
        return jsonify({"error": f"Failed to retrieve weather data: {e}"}), 500
    except (KeyError, TypeError) as e:
        # Handle missing keys or unexpected structure in the API response
        return jsonify({"error": f"Error parsing weather data: {e}"}), 500

@app.route('/api/notifications')
def get_notifications():
    """
    Analyzes current conditions and forecasts future changes to return a list
    of alerts aimed at school administrators.
    """
    # --- Mocking session for testing. In your app, this is set on login. ---
    if 'user_id' not in session:
        session['user_id'] = 'test_user' 
        # In a real app, you would handle unauthenticated users properly.

    user_id = session['user_id']
    data_folder = 'user'
    file_path = os.path.join(data_folder, f'{user_id}.csv')
    
    # Load pollutant level definitions from JSON file
    try:
        with open('./config/pollutant_levels.json', 'r', encoding='utf-8') as f:
            pollutants_config = json.load(f)
    except FileNotFoundError:
        return jsonify({"alerts": [{"message": "Error: pollutant_levels.json not found.", "suggestion": ""}], "error": True})

    if not os.path.exists(file_path):
        return jsonify({"alerts": [], "message": "No data file found for user."})

    try:
        df = pd.read_csv(file_path)
        df['date'] = pd.to_datetime(df['date'])
    except Exception as e:
        return jsonify({"alerts": [{"message": f"Error reading or parsing CSV file: {e}", "suggestion": ""}], "error": True})

    # --- CORRECTED Main Analysis Logic ---
    # Get today's date to find the start of the forecast.
    # In a live app, this line would be: today = datetime.now().date()
    # --- CHANGE HERE ---
    # OLD hardcoded way:
    # today = pd.to_datetime('2025-10-05').date()
    # today_dt = pd.to_datetime(today)

    # NEW dynamic way:
    # First, ensure the 'date' column is a proper datetime type
    # --- CHANGE HERE ---
    # OLD hardcoded way:
    # today = pd.to_datetime('2025-10-05').date()
    # today_dt = pd.to_datetime(today)

    # NEW dynamic way using .iloc[0]:
    # First, ensure the 'date' column is a proper datetime type
    alerts = []
    df['date'] = pd.to_datetime(df['date'])
    
    # ⚠️ IMPORTANT: You MUST sort by date for .iloc[0] to be correct.
    df = df.sort_values(by='date').reset_index(drop=True)

    # Check if the DataFrame is empty after potential filtering
    if df.empty:
        return jsonify({"alerts": [], "message": "The forecast data is empty."})
        
    # Get the date from the very first row (index 0)
    start_date_dt = df['date'].iloc[0]
    start_date = start_date_dt.date()

    # Find the forecast data for that first day
    today_s_data = df[df['date'].dt.date == start_date]

    # The message should reflect the actual date being used
    if today_s_data.empty:
        return jsonify({"alerts": [], "message": f"No forecast data available for the starting date ({start_date.strftime('%Y-%m-%d')})."})

    # The entire forecast period starts from the first available date
    forecast_df = df[df['date'] >= start_date_dt].copy()
    current_values = today_s_data.iloc[0] 

    for key, config in pollutants_config.items():
        if key not in forecast_df.columns:
            continue

        # Get today's value for the current alert
        latest_value = current_values[key]
        level_name, level_info = get_pollutant_level(latest_value, config["levels"])

        # --- Forecast Logic (phrasing is now very short) ---
        forecast_phrase = ""
        avg_daily_change = forecast_df[key].diff().mean()

        if avg_daily_change > 0.01:  # Worsening trend
            next_level_threshold = None
            next_level_name = ""
            sorted_levels = sorted(config["levels"].items(), key=lambda item: item[1]['min'])
            
            for name, details in sorted_levels:
                if details['min'] > latest_value:
                    next_level_threshold = details['min']
                    next_level_name = name.replace("_", " ")
                    break
            
            if next_level_threshold:
                value_to_increase = next_level_threshold - latest_value
                if value_to_increase > 0 and avg_daily_change > 0:
                    days_to_next_level = math.ceil(value_to_increase / avg_daily_change)
                    
                    if days_to_next_level == 1:
                        forecast_phrase = f"Worsening to {next_level_name} by tomorrow."
                    else:
                        # This is the "how many days" forecast you wanted
                        forecast_phrase = f"Worsening to {next_level_name} in ~{days_to_next_level} days."

        elif avg_daily_change < -0.01:  # Improving trend
            forecast_phrase = "Conditions expected to improve."

        # If the level is not "Good", create the short, direct alert string
        if level_name and level_name != "Good":
            
            # NOTE: For best results, your suggestion text in the config should be short.
            # This code takes the first part of a suggestion.
            short_suggestion = level_info['suggestion'].split('.')[0]

            # Build the final alert string in the format: "Pollutant: Level - Action."
            alert_string = f"{config['name']}: {level_name} - {short_suggestion}."
            
            # Append the short forecast if it exists
            if forecast_phrase:
                alert_string += f" {forecast_phrase}"

            alerts.append(alert_string)

    # If the list is still empty, add a simple "Good" status message
    if not alerts:
        alerts.append("Air quality is Good. Outdoor activities are encouraged.")

    return jsonify({"alerts": alerts})

if __name__ == '__main__':
    # Start the background tasks in a separate thread
    # The 'daemon=True' ensures the thread will exit when the main app exits.
    task_thread = threading.Thread(target=background_tasks, daemon=True)
    task_thread.start()

    # Now, start your web server. It will run in the main thread.
    print("Starting web server...")
    # Ensure you have a 'templates' folder with index.html in it.
    print("Starting web server with Waitress on http://127.0.0.1:5000")
    serve(app, host='127.0.0.1', port=5000)

