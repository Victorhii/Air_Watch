#!/usr/bin/env python3
import re
import os
import time
import json
from collections import defaultdict
from dotenv import load_dotenv
from openaq import OpenAQ

# config
INPUT_TXT = "filtered_stations_with_pm25.txt"
OUTPUT_JSON = "sensors_by_coord.json"
RATE_LIMIT_SLEEP = 1.93  # seconds between API calls
COORD_DECIMALS = 6       # round coords to this many decimals for canonical keys

load_dotenv()
API_KEY = os.getenv("OPENAQ_API")
if not API_KEY:
    raise RuntimeError("Set OPENAQ_API in environment or .env")

client = OpenAQ(api_key=API_KEY)

def parse_location_ids(path):
    """Return list of integer location IDs parsed from file lines like: '164 | ... | 29.81453,-95.38769'"""
    ids = []
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            # first token before '|' should be the numeric ID
            parts = [p.strip() for p in line.split("|")]
            try:
                loc_id = int(parts[0])
                ids.append(loc_id)
            except Exception:
                print(f"Skipping unparseable line: {line}")
    return ids

def coord_key(lat, lon, decimals=COORD_DECIMALS):
    if lat is None or lon is None:
        return None
    try:
        return f"{round(float(lat), decimals):.{decimals}f},{round(float(lon), decimals):.{decimals}f}"
    except Exception:
        return None

def get_pm25_sensors_for_location(location_id):
    """Return list of tuples (sensor_id, lat, lon) for pm25 sensors for given location."""
    try:
        resp = client.locations.sensors(location_id)
    except Exception as e:
        print(f"Error querying location {location_id}: {e}")
        return []

    out = []
    for sensor in getattr(resp, "results", resp):
        # parameter name detection (safe)
        param_name = None
        if isinstance(sensor, dict):
            param = sensor.get("parameter")
            if isinstance(param, dict):
                param_name = param.get("name")
            else:
                param_name = param
        else:
            # object-like
            try:
                param_name = sensor.parameter.name
            except Exception:
                try:
                    param_name = sensor.parameter
                except Exception:
                    param_name = None

        if not param_name or str(param_name).lower() != "pm25":
            continue

        # coordinates extraction (robust)
        lat = lon = None
        if isinstance(sensor, dict):
            latest = sensor.get("latest") or {}
            coords = latest.get("coordinates") or {}
            lat = coords.get("latitude") or coords.get("lat") or sensor.get("latitude")
            lon = coords.get("longitude") or coords.get("lon") or coords.get("longitude")
            sensor_id = sensor.get("id")
        else:
            latest = getattr(sensor, "latest", None)
            coords = getattr(latest, "coordinates", None) if latest else None
            if coords:
                lat = getattr(coords, "latitude", None)
                lon = getattr(coords, "longitude", None)
            lat = lat or getattr(sensor, "latitude", None) or getattr(sensor, "lat", None)
            lon = lon or getattr(sensor, "longitude", None) or getattr(sensor, "lon", None)
            sensor_id = getattr(sensor, "id", None)

        try:
            if sensor_id is not None:
                sensor_id = int(sensor_id)
        except Exception:
            pass

        out.append((sensor_id, lat, lon))
    return out

def main():
    loc_ids = parse_location_ids(INPUT_TXT)
    print(f"Found {len(loc_ids)} location IDs.")
    mapping = defaultdict(list)

    for idx, loc in enumerate(loc_ids, start=1):
        print(f"[{idx}/{len(loc_ids)}] location {loc} ...")
        sensors = get_pm25_sensors_for_location(loc)
        if not sensors:
            print("  -> no pm25 sensors found")
        for sensor_id, lat, lon in sensors:
            key = coord_key(lat, lon)
            if key is None:
                key = f"location_{loc}_no_coords"
            if sensor_id not in mapping[key]:
                mapping[key].append(sensor_id)
        time.sleep(RATE_LIMIT_SLEEP)

    # export
    with open(OUTPUT_JSON, "w", encoding="utf-8") as fh:
        json.dump(mapping, fh, indent=2, ensure_ascii=False)

    print(f"Wrote {len(mapping)} coordinate keys to {OUTPUT_JSON}")

if __name__ == "__main__":
    main()
