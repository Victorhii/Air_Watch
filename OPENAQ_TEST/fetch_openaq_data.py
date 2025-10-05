from openaq import OpenAQ
from dotenv import load_dotenv
import os
from datetime import datetime, timezone

def get_latest_sensor_data(sensor_id):
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
            
        return latest.value, time_str, latest.parameter.units


    else:
        print(f"No measurements found for sensor ID: {sensor_id}")