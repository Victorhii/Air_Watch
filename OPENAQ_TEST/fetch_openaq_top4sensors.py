# analyze_active_pollutants.py
"""
Reads location IDs from a filtered_openaq_locations.txt file and, referencing the
pattern used in fetch_openaq_data.py, queries OpenAQ for sensors at each location,
determines which pollutants are active (last measurement within `ACTIVE_DAYS`),
and produces a top-4 list of most-active pollutants across all locations.

Outputs:
 - active_pollutants_by_location.json  (mapping location_id -> [pollutant,...])
 - top4_active_pollutants.json        (list of (pollutant, location_count))
"""

import os
import re
import json
import time
from collections import Counter
from datetime import datetime, timezone
from dotenv import load_dotenv
from openaq import OpenAQ
from typing import List, Set, Tuple, Dict, Optional
# exclude ones with this error: [3168/6131] Checking location 219609... HTTP 500 - Internal Server Error
NoneType: None
# ----- CONFIG -----
LOCATIONS_FILE = "./filtered_openaq_locations.txt"  # adjust if needed
OUTPUT_DIR = "./"
ACTIVE_DAYS = 5           # considered "active" if last measurement within this many days
REQUEST_SLEEP = 1.93      # seconds between API calls to be polite with rate limits
# ------------------

load_dotenv()
API_KEY = os.getenv("OPENAQ_API")
if not API_KEY:
    raise RuntimeError("OPENAQ_API not found in environment. Put it into .env (OPENAQ_API=...)")

client = OpenAQ(api_key=API_KEY)

def parse_location_ids(txt_path: str) -> List[int]:
    """
    Extract integer location IDs from each line of the txt file.
    Expected formats seen in your file: "1001650 | NAME | CC | lat,lon" or "1001650,..."
    We'll find the first contiguous digits on each non-empty line.
    """
    ids: List[int] = []
    int_re = re.compile(r'^\s*(\d+)\b')  # prefer leading integer
    any_int_re = re.compile(r'(\d{1,10})')  # fallback: first integer anywhere
    with open(txt_path, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            m = int_re.search(line)
            if not m:
                m = any_int_re.search(line)
            if m:
                try:
                    lid = int(m.group(1))
                    ids.append(lid)
                except ValueError:
                    continue
    # deduplicate while preserving order
    seen = set()
    uniq = []
    for i in ids:
        if i not in seen:
            uniq.append(i)
            seen.add(i)
    return uniq

def _safe_get_datetime_utc(sensor) -> Optional[str]:
    """
    Try multiple approaches to extract the sensor's last measurement UTC string.
    The fetch_openaq_data.py used sensor.datetime_last["utc"] so we try dictionary
    style first then attribute style.
    """
    # sensor may be a dict-like
    try:
        if isinstance(sensor, dict):
            dl = sensor.get("datetime_last")
            if isinstance(dl, dict):
                return dl.get("utc")
    except Exception:
        pass

    # attribute-style
    try:
        dl = getattr(sensor, "datetime_last", None)
        if isinstance(dl, dict):
            return dl.get("utc")
    except Exception:
        pass

    # fallback: different key names
    try:
        if isinstance(sensor, dict):
            return sensor.get("lastUpdated")
    except Exception:
        pass

    return None

def _safe_get_parameter_name(sensor) -> Optional[str]:
    """
    Extract parameter/pollutant name robustly.
    fetch_openaq_data.py used sensor.parameter['name'].
    """
    try:
        if isinstance(sensor, dict):
            param = sensor.get("parameter")
            if isinstance(param, dict):
                return param.get("name")
            if isinstance(param, str):
                return param
            pname = sensor.get("parameter_name") or sensor.get("parameterCode")
            if pname:
                return pname
    except Exception:
        pass

    try:
        param = getattr(sensor, "parameter", None)
        if isinstance(param, dict):
            return param.get("name")
        if isinstance(param, str):
            return param
    except Exception:
        pass

    return None

def is_active_utc(utc_str: Optional[str], days_threshold: int) -> bool:
    if not utc_str:
        return False
    try:
        # normalize trailing Z -> +00:00 for fromisoformat
        iso = utc_str.replace("Z", "+00:00")
        last = datetime.fromisoformat(iso)
        if last.tzinfo is None:
            # assume UTC if naive
            last = last.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - last
        return delta.total_seconds() >= 0 and delta.days <= days_threshold
    except Exception:
        return False

def process_locations(location_ids: List[int]) -> Tuple[Dict[int, List[str]], Counter]:
    """
    For each location ID, query OpenAQ sensors and collect active pollutants.
    Returns:
      - dict: location_id (int) -> sorted list of active pollutant names
      - Counter: pollutant -> number of locations where it's active
    """
    loc_active_map: Dict[int, List[str]] = {}
    pollutant_counts: Counter = Counter()
    total = len(location_ids)
    for idx, loc_id in enumerate(location_ids, start=1):
        print(f"[{idx}/{total}] Checking location {loc_id}...", end=" ", flush=True)
        try:
            resp = client.locations.sensors(loc_id)
        except Exception as e:
            print(f"ERROR (API): {e}")
            # skip and continue
            time.sleep(REQUEST_SLEEP)
            continue

        # resp may be a custom object or dict-like; try to extract results
        sensors = None
        try:
            sensors = getattr(resp, "results", None)
        except Exception:
            sensors = None
        if sensors is None and isinstance(resp, dict):
            sensors = resp.get("results")

        if not sensors:
            print("no sensors")
            time.sleep(REQUEST_SLEEP)
            continue

        active_set: Set[str] = set()
        for s in sensors:
            last_time_str = _safe_get_datetime_utc(s)
            if not last_time_str:
                continue
            if is_active_utc(last_time_str, ACTIVE_DAYS):
                pname = _safe_get_parameter_name(s)
                if pname:
                    active_set.add(pname.strip().lower())

        if active_set:
            loc_active_map[loc_id] = sorted(active_set)
            for p in active_set:
                pollutant_counts[p] += 1
            print(f"found {len(active_set)} active pollutants")
        else:
            loc_active_map[loc_id] = []
            print("none active")

        time.sleep(REQUEST_SLEEP)  # small pause between requests

    return loc_active_map, pollutant_counts

def main():
    print("Parsing location IDs from:", LOCATIONS_FILE)
    location_ids = parse_location_ids(LOCATIONS_FILE)
    print(f"Found {len(location_ids)} unique location IDs (using pattern-based extraction).")
    if not location_ids:
        print("No location IDs found; check the LOCATIONS_FILE path or file content.")
        return

    loc_active_map, pollutant_counts = process_locations(location_ids)

    # Top 4 pollutants by number of locations where they're active
    top4 = pollutant_counts.most_common(4)

    # Save outputs
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_loc_file = os.path.join(OUTPUT_DIR, "active_pollutants_by_location.json")
    out_top_file = os.path.join(OUTPUT_DIR, "top4_active_pollutants.json")

    with open(out_loc_file, "w", encoding="utf-8") as f:
        json.dump(loc_active_map, f, indent=2)

    with open(out_top_file, "w", encoding="utf-8") as f:
        json.dump({"top4": top4}, f, indent=2)

    # Print summary
    print("\n=== SUMMARY ===")
    total_locs_with_active = sum(1 for v in loc_active_map.values() if v)
    print(f"Locations checked: {len(location_ids)}")
    print(f"Locations with ≥1 active pollutant: {total_locs_with_active}")
    print("Top 4 most-active pollutants (pollutant : number_of_locations_active):")
    for p, cnt in top4:
        print(f"  {p} : {cnt}")

    print(f"\nSaved per-location results → {out_loc_file}")
    print(f"Saved top-4 results       → {out_top_file}")

if __name__ == "__main__":
    main()
