from openaq import OpenAQ
from dotenv import load_dotenv
import os
import json

load_dotenv()
api = os.getenv("OPENAQ_API")
client = OpenAQ(api_key=api)

def get_attr(obj, *keys):
    """Try object.attribute, then obj[key] for each key in order. Return first non-None."""
    for k in keys:
        # object attribute
        val = getattr(obj, k, None)
        if val is not None:
            return val
        # dict-like access
        try:
            val = obj[k]
        except Exception:
            val = None
        if val is not None:
            return val
    return None

def flatten_location(loc):
    """Return a plain dict with only primitive fields for JSON."""
    loc_id = get_attr(loc, "id")
    name = get_attr(loc, "name")
    locality = get_attr(loc, "locality")
    timezone = get_attr(loc, "timezone")

    # coordinates may be object or dict
    coords = get_attr(loc, "coordinates")
    if coords is None:
        lat = lon = None
    else:
        lat = get_attr(coords, "latitude", "lat")
        lon = get_attr(coords, "longitude", "lon")

    # country may be object CountryBase(id=..., code='GH', name='Ghana')
    country = get_attr(loc, "country")
    c_code = c_name = None
    if country is not None:
        c_code = get_attr(country, "code", "country_code")
        c_name = get_attr(country, "name")

    provider = get_attr(loc, "provider")
    provider_name = get_attr(provider, "name") if provider else None

    # sensors: list of SensorBase objects
    sensors = get_attr(loc, "sensors") or []
    sensor_params = []
    for s in sensors:
        param = get_attr(s, "parameter")  # ParameterBase
        if param:
            p_name = get_attr(param, "name") or get_attr(param, "display_name")
            p_units = get_attr(param, "units")
            sensor_params.append({"name": p_name, "units": p_units})
        else:
            # fallback to sensor.name
            sensor_params.append({"name": get_attr(s, "name")})

    return {
        "id": loc_id,
        "name": name,
        "locality": locality,
        "timezone": timezone,
        "latitude": lat,
        "longitude": lon,
        "country_code": c_code,
        "country_name": c_name,
        "provider": provider_name,
        "sensors": sensor_params,
    }

# paginate until no more results
limit = 100  # server limit shown in meta
page = 1
all_locations = []

while True:
    resp = client.locations.list(limit=limit, page=page)
    # get results whether resp is object-like or dict-like
    if hasattr(resp, "results"):
        results = resp.results
    elif isinstance(resp, dict) and "results" in resp:
        results = resp["results"]
    else:
        try:
            results = list(resp)
        except Exception:
            results = []

    if not results:
        break

    for loc in results:
        flat = flatten_location(loc)
        # print something useful instead of None
        print(f"id={flat['id']} name={flat['name']} coords=({flat['latitude']},{flat['longitude']}) country={flat['country_code']}")
        all_locations.append(flat)

    page += 1

# save_human_readable_txt.py
output_path = "./openaq_locations.txt"

with open(output_path, "w", encoding="utf-8") as f:
    for loc in all_locations:
        loc_id = loc.get("id")
        name = loc.get("name") or ""
        country = loc.get("country_code") or ""
        lat = loc.get("latitude")
        lon = loc.get("longitude")
        coords = f"{lat},{lon}" if lat is not None and lon is not None else ""
        # change the separator or fields as you like
        line = f"{loc_id} | {name} | {country} | {coords}\n"
        f.write(line)

print("Saved human-readable text to", output_path)


client.close()




# location = client.locations.get(2178)

# client.close()

# print(location)