from openaq import OpenAQ
from dotenv import load_dotenv
import os
from datetime import datetime, timezone
import requests

def get_WeatherAPI_data(lat, lon):
    """
    Retrieves latest air quality data from WeatherAPI.com.

    Args:
        api_key (str): Your personal WeatherAPI.com API key.
        lat (float): The latitude for the location.
        lon (float): The longitude for the location.

    Returns:
        None: Prints the air quality data or an error message.
    """
    # Construct the API URL with air quality data included (aqi=yes)
    load_dotenv()
    api_key = os.getenv('WEATHERTAPI_APIKEY')
    base_url = "http://api.weatherapi.com/v1/current.json"
    query_params = {
        'key': api_key,
        'q': f'{lat},{lon}',
        'aqi': 'yes'  # This parameter is crucial to get air quality data
    }

    try:
        # Make the GET request to the API
        response = requests.get(base_url, params=query_params)
        # Raise an exception for bad status codes (4xx or 5xx)
        response.raise_for_status()

        data = response.json()

        # Extract the air quality data from the response
        air_quality = data.get('current', {}).get('air_quality', {})

        if not air_quality:
            print("Could not find air quality data in the API response.")
            return

        # Extract specific pollutant values
        pm2_5 = air_quality.get('pm2_5')
        no2 = air_quality.get('no2')
        o3 = air_quality.get('o3')
        return round(pm2_5, 2), 'µg/m³'


    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response content: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred during the request: {req_err}")
    except KeyError:
        print("Error: Could not parse the expected data from the API response.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def get_latest_sensor_data(sensor_id, lat, lon):
    # --- Setup ---
    # It's good practice to handle cases where environment variables might be missing.
    load_dotenv()
    api_key = os.getenv("OPENAQ_API")
    if not api_key:
        raise ValueError("OPENAQ_API key not found in .env file.")
    client = OpenAQ(api_key=api_key)

    # --- Fetch Data ---
    # Note: The 'data' parameter is not a valid filter for this endpoint. 
    # It's removed to avoid potential errors.
    response = client.measurements.list(
        sensors_id=sensor_id,
        limit=1,
        page=1,
        data="measurements"
    )

    # --- Process Data ---
    if response.results:
        latest = response.results[0]
        
        # 1. Get the measurement time from the 'datetime_to.utc' field
        measurement_time_str = latest.period.datetime_to.utc
        
        # Convert the string to a timezone-aware datetime object
        measurement_time = datetime.fromisoformat(measurement_time_str.replace("Z", "+00:00"))
        
        # 2. Get the current UTC time and calculate the delta
        now = datetime.now(timezone.utc)
        delta = now - measurement_time

        # 3. Format the time difference string, now including days
        days = delta.days
        hours = delta.seconds // 3600
        minutes = (delta.seconds % 3600) // 60

        if days > 0:
            time_str = f"{days} days ago"
        elif hours > 0:
            time_str = f"{hours} hours ago"
        else:
            time_str = f"{minutes} minutes ago"
            
        if days > 3:
            value, unit = get_WeatherAPI_data(lat, lon)
            return value, unit
        return latest.value, latest.parameter.units


    else:
        value, unit = get_WeatherAPI_data(lat, lon)
        return value, unit