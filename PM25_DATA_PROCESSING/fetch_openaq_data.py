from openaq import OpenAQ
from dotenv import load_dotenv
import os

load_dotenv()
api = os.getenv("OPENAQ_API")
client = OpenAQ(api_key=api)

from datetime import datetime, timezone

sensor_id = 13727287  # replace with a real sensor ID

# Fetch the latest measurement for this sensor
response = client.measurements.list(
    sensors_id=sensor_id,
    limit=1,
    page=1,
    data="measurements"
)

if response.results:
    latest = response.results[0]
    print(latest)
    
    # Convert datetime to aware object
    measurement_time = latest.datetime
    if isinstance(measurement_time, str):
        measurement_time = datetime.fromisoformat(measurement_time.replace("Z", "+00:00"))
    
    now = datetime.now(timezone.utc)
    delta = now - measurement_time
    minutes = int(delta.total_seconds() // 60)

    if minutes > 120:
        hours = minutes // 60
        time_str = f"{hours} hours ago"
    else:
        time_str = f"{minutes} minutes ago"

    print(f"{latest.parameter}: {latest.value} {latest.unit}, updated {time_str}")
else:
    print("No measurements found for this sensor.")


