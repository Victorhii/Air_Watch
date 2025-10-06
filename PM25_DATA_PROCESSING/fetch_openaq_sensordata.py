from openaq import OpenAQ
from dotenv import load_dotenv
import os
import json
from datetime import datetime, timezone, timedelta

load_dotenv()
API_KEY = os.getenv("OPENAQ_API")
client = OpenAQ(api_key=API_KEY)

def parse_iso_to_utc(dt_str):
    """Parse many ISO-like strings to a timezone-aware UTC datetime."""
    if dt_str is None:
        return None
    if isinstance(dt_str, datetime):
        dt = dt_str
    else:
        s = str(dt_str)
        # replace trailing Z with +00:00 so fromisoformat accepts it
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        # If no timezone info, assume UTC
        if "+" not in s and "-" not in s[-6:]:
            s = s + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except Exception:
            # last resort: try to parse date-only
            try:
                dt = datetime.fromisoformat(s.split("T")[0]).replace(tzinfo=timezone.utc)
            except Exception:
                return None
    # normalize to UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt

def midpoint_iso_z(date_from, date_to):
    """Return midpoint datetime (UTC) between two ISO strings, formatted like 'YYYY-MM-DDTHH:MM:SSZ'."""
    a = parse_iso_to_utc(date_from)
    b = parse_iso_to_utc(date_to)
    if a is None or b is None:
        return None
    mid = a + (b - a) / 2
    # format without offset and with trailing Z
    return mid.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

def _extract_results(resp):
    if resp is None:
        return []
    if isinstance(resp, dict):
        return resp.get("results") or resp.get("data") or []
    return getattr(resp, "results", []) or getattr(resp, "data", []) or []

def _extract_datetime_from_measurement(m):
    """Try to get the measurement timestamp (fall back to many places). Returns ISO string or None."""
    # object-like Measurement with period.datetime_from.utc
    try:
        period = getattr(m, "period", None)
        if period is not None:
            df = getattr(period, "datetime_from", None) or getattr(period, "datetimeFrom", None)
            if df is not None:
                utc = getattr(df, "utc", None)
                if utc:
                    return str(utc)
                if isinstance(df, dict):
                    return df.get("utc") or df.get("utc_datetime") or df.get("local")
                if hasattr(df, "isoformat"):
                    return df.isoformat()
    except Exception:
        pass

    # object-like fallbacks
    for attr in ("datetime", "date", "timestamp", "measured_at"):
        try:
            val = getattr(m, attr, None)
            if val:
                if isinstance(val, dict):
                    return val.get("utc") or val.get("utc_datetime") or val.get("local")
                if isinstance(val, str):
                    return val
                if hasattr(val, "isoformat"):
                    return val.isoformat()
        except Exception:
            pass

    # dict-style measurement
    if isinstance(m, dict):
        d = m.get("date") or m.get("datetime") or m.get("timestamp")
        if isinstance(d, dict):
            return d.get("utc") or d.get("utc_datetime") or d.get("local")
        if isinstance(d, str):
            return d

    return None

def _extract_value_from_measurement(m):
    try:
        if isinstance(m, dict):
            v = m.get("value")
        else:
            v = getattr(m, "value", None)
        if v is None:
            return None
        return float(v)
    except Exception:
        # best-effort parse from string
        try:
            s = str(v).strip()
            return float(s)
        except Exception:
            return None

def fetch_time_value_with_midpoint(sensor_id: int, date_from: str, date_to: str,
                                   limit: int = 100, apply_midpoint_to_measurements: bool = False):
    """
    Fetch measurements (time+value) and include the midpoint between date_from and date_to
    as top-level 'dt'. If apply_midpoint_to_measurements=True then each measurement's
    datetime_utc will be set to that midpoint.
    """
    page = 1
    out = {
        "sensor_id": sensor_id,
        "date_from": date_from,
        "date_to": date_to,
        "dt": midpoint_iso_z(date_from, date_to),
        "measurements": []
    }

    while True:
        resp = client.measurements.list(
            sensors_id=sensor_id,
            datetime_from=date_from,
            datetime_to=date_to,
            limit=limit,
            page=page
        )
        results = _extract_results(resp)
        if not results:
            break

        for m in results:
            raw_dt = _extract_datetime_from_measurement(m)
            val = _extract_value_from_measurement(m)
            if apply_midpoint_to_measurements:
                dt_to_store = out["dt"]
            else:
                # normalize extracted datetime to ISO Z if possible
                parsed = parse_iso_to_utc(raw_dt)
                dt_to_store = parsed.isoformat().replace("+00:00", "Z") if parsed else None

            out["measurements"].append({
                "datetime_utc": dt_to_store,
                "value": val
            })

        if len(results) < limit:
            break
        page += 1

    out["n_measurements"] = len(out["measurements"])
    return out

if __name__ == "__main__":
    sensor_id = 2071327  # replace with real sensor id
    # your requested date range:
    date_from = "2025-09-20T00:00:00Z"
    date_to   = "2025-09-30T23:59:59Z"

    # set to True if you want every measurement's datetime replaced by the midpoint
    apply_midpoint_to_measurements = False

    result = fetch_time_value_with_midpoint(
        sensor_id,
        date_from,
        date_to,
        limit=100,
        apply_midpoint_to_measurements=apply_midpoint_to_measurements
    )

    # pretty print
    print(json.dumps(result, indent=2))

    # save file
    fname = f"sensor_{sensor_id}_{date_from[:10]}_to_{date_to[:10]}_time_value_midpoint.json"
    with open(fname, "w", encoding="utf-8") as fh:
        json.dump(result, fh, indent=2, ensure_ascii=False)
    print(f"Saved {result['n_measurements']} records to {fname}")
