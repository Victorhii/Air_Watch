import json
from math import radians, sin, cos, sqrt, atan2

def calculate_haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculates the great-circle distance between two points
    on the Earth's surface using the Haversine formula.
    """
    R = 6371  # Radius of Earth in kilometers

    # Convert latitude and longitude from degrees to radians
    lat1_rad = radians(lat1)
    lon1_rad = radians(lon1)
    lat2_rad = radians(lat2)
    lon2_rad = radians(lon2)

    # Difference in coordinates
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad

    # Haversine formula
    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c

    return distance # in kilometers

# --- SCRIPT START ---

# 1. Your data (as if loaded from the JSON file)
# In a real application, you might load this from a file:
def get_nearest_sensor(input_lat, input_lon):
    """
    Finds the nearest sensor ID from 'sensors.json' based on input coordinates.

    Args:
        input_lat (float): The latitude of the input location.
        input_lon (float): The longitude of the input location.

    Returns:
        int: The ID of the nearest sensor.
    """
    # Note: Your original code had two different folder names.
    # Ensure this path is correct for your project structure.
    file_path = './PM25_DATA_PROCESSING/sensors.json'

    with open(file_path, 'r') as f:
        sensors_data = json.load(f)

    # Find the nearest sensor by directly iterating over the dictionary items.
    # The 'key' for the min() function now parses the coordinate string on the fly.
    nearest_sensor_item = min(
        sensors_data.items(),
        key=lambda item: calculate_haversine_distance(
            input_lat,
            input_lon,
            # item[0] is the key string, e.g., "29.814530,-95.387690"
            # We split it by the comma and convert the parts to float.
            float(item[0].split(',')[0]),  # This becomes the latitude
            float(item[0].split(',')[1])   # This becomes the longitude
        )
    )

    # The result 'nearest_sensor_item' is a tuple like ('lat,lon', [id]).
    # We just need the ID from the value part of the tuple.
    found_sensor_id = nearest_sensor_item[1][0]

    return found_sensor_id