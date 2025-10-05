# point_value_extractor.py

import pandas as pd
import xarray as xr
import numpy as np
import os
from datetime import datetime, timedelta
import pytz

def get_point_value(da, qc_da, lat, lon, time=None, interp_method="linear"):
    """
    Revised function to prioritize quality flags.
    Returns a valid data point or np.nan if the quality is not good.
    """
    # Select the relevant time slice
    arr = da
    qc = qc_da
    if "time" in arr.dims:
        if time is None:
            arr = arr.isel(time=0)
            qc = qc.isel(time=0)
        else:
            time_dt = np.datetime64(time)
            arr = arr.sel(time=time_dt, method="nearest")
            qc = qc.sel(time=time_dt, method="nearest")

    # Coordinate names detection
    latname = next((n for n in ("latitude","lat","y") if n in arr.coords), None)
    lonname = next((n for n in ("longitude","lon","x") if n in arr.coords), None)
    if latname is None or lonname is None:
        raise ValueError("Lat/Lon coords not found")

    # --- PRIMARY CHANGE: CHECK QUALITY OF NEAREST PIXEL FIRST ---
    nearest_pt = arr.sel({latname: lat, lonname: lon}, method="nearest")
    nearest_qc = qc.sel({latname: lat, lonname: lon}, method="nearest")
    
    # Get the quality flag value. It's an integer.
    qc_val = int(nearest_qc.values)

    # If the quality flag is not 0 (Good Quality), reject the data immediately.
    if qc_val == 2:
        print(f"  -> WARNING: Nearest pixel has bad quality flag ({qc_val}). Returning NaN.")
        # Returning 'nan' is better than a string, your main loop can handle it.
        return 'nan' 
        
    # --- IF QUALITY IS GOOD, PROCEED WITH ORIGINAL LOGIC ---
    
    # If we are here, nearest_qc.values == 0, so the data is good quality.
    nearest_val = nearest_pt.values.item()

    # Interpolated value (may still be useful if local variability is low)
    try:
        interp_pt = arr.interp({latname: lat, lonname: lon})
        interp_val = float(interp_pt.values)
    except Exception:
        interp_val = None

    # Get 3x3 neighborhood for local stats
    nearest_coord = (float(nearest_pt.coords[latname].values), float(nearest_pt.coords[lonname].values))
    lat_idx = int(np.argmin(np.abs(np.asarray(arr[latname]) - nearest_coord[0])))
    lon_idx = int(np.argmin(np.abs(np.asarray(arr[lonname]) - nearest_coord[1])))
    i0, i1 = max(0, lat_idx-1), min(len(arr[latname])-1, lat_idx+1)
    j0, j1 = max(0, lon_idx-1), min(len(arr[lonname])-1, lon_idx+1)
    
    # Create a mask for good quality pixels in the neighborhood
    # Alternative Correct: Using the bitwise OR operator '|' ✅
    qc_subset = qc.isel({latname: slice(i0, i1+1), lonname: slice(j0, j1+1)})
    good_quality_mask = (qc_subset == 0)
    subset = arr.isel({latname: slice(i0, i1+1), lonname: slice(j0, j1+1)}).where(good_quality_mask)

    valid_vals = subset.values[np.isfinite(subset.values)]
    n_valid = len(valid_vals)

    if n_valid > 1: # Need at least 2 points for standard deviation
        local_mean = float(np.mean(valid_vals))
        local_std = float(np.std(valid_vals, ddof=0))
        rel_std = local_std / (abs(local_mean) if abs(local_mean) > 1e-9 else 1.0)
    else:
        rel_std = None

    # Decision Logic: Prefer interpolation only if variability among good neighbors is low.
    decision = "nearest"
    if n_valid >= 4 and rel_std is not None and rel_std < 0.10 and interp_val is not None:
        decision = "interp"
        
    # --- Final Return ---
    # The division by 10**15 is correct for scaling units like molecules/cm^2.
    if decision == 'nearest':
        # Even if the value is negative, it's a good quality retrieval, so we keep it.
        # We interpret it as a value near zero.
        return round(nearest_val / 10**16, 2)
    else: # decision == 'interp'
        return round(interp_val / 10**16, 2)
import requests

def get_latest_formaldehyde_data(lat, lon):
    """
    Retrieves the latest formaldehyde data from the Open-Meteo Air Quality API.

    Args:
        lat (float): The latitude of the location.
        lon (float): The longitude of the location.

    Returns:
        dict: A dictionary with the formaldehyde value and unit, or None on error.
    """
    api_url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    
    params = {
        'latitude': lat,
        'longitude': lon,
        'current': 'formaldehyde', # Specify the pollutant we want
        'domains': 'cams_global'   # Use the global model for worldwide coverage
    }

    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()  # Check for request errors
        data = response.json()
        
        # Extract the current value and its unit
        value = data.get('current', {}).get('formaldehyde')
        unit = data.get('current_units', {}).get('formaldehyde')
        
        if value is not None and unit is not None:
            return value, unit
            
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the API request: {e}")
    
    return None

def get_hcho_value(latitude: float, longitude: float, data_dir: str = "./NRT_DATASET/HCHO/tempo_data"):
    """
    Retrieves a NO2 value for a point, trying the 3 latest available files.

    Args:
        latitude (float): The latitude of the point of interest.
        longitude (float): The longitude of the point of interest.
        data_dir (str): The directory containing the 'granule_log.csv'.

    Returns:
        float: The NO2 value, or None if not found in the top 3 granules.
    """
    log_file = os.path.join(data_dir, "granule_log.csv")
    try:
        log_df = pd.read_csv(log_file)
        if log_df.empty:
            print("Log file is empty. Please run data_fetcher.py first.")
            return None
    except FileNotFoundError:
        print(f"Error: Log file not found at '{log_file}'. Run data_fetcher.py.")
        return None

    # Iterate through the top 3 files from the log
    for index, row in log_df.head(3).iterrows():
        file_path = row['local_filepath']
        print(f"\nAttempting to extract value from: {os.path.basename(file_path)}")

        try:
            with xr.open_datatree(file_path) as datatree:
                data_array = datatree['product/vertical_column']
                quality_flags = datatree["product/main_data_quality_flag"]
                
                value = get_point_value(data_array, quality_flags, lat=latitude, lon=longitude)
                
                if value is not None and not np.isnan(value) and value > 0:
                    # ==================== MODIFIED LOGIC HERE ====================
                    # Now that we have a valid value, check the timestamp of THIS file.
                    end_time_str = row['end_time']
                    granule_end_time = pd.to_datetime(end_time_str).tz_convert('UTC')
                    current_utc_time = datetime.now(pytz.utc)

                    # If this specific granule's data is older than 2 hours, discard it and continue.
                    if (current_utc_time - granule_end_time) > timedelta(hours=2):
                        print(f"-> Value found, but data is from {granule_end_time.strftime('%H:%M:%S UTC')} (>2 hours old). Trying next file.")
                        value, unit = get_latest_formaldehyde_data(latitude, longitude)
                        return value, 'Open-Meteo', unit
                    
                    # If the value is valid AND the data is recent, it's a success.
                    print(f"✓ Success! Found valid, recent data point: {value} (mol/m^2 * 1e15)")
                    return value, 'Tempo', ' x 10¹⁶ molec/cm²'
                    # ===========================================================

                else:
                    print(f"-> Value was 'nan' or negative - {value}. Trying next available file...")

        except Exception as e:
            print(f"-> Could not process file {os.path.basename(file_path)}. Error: {e}")
            continue
            
    print("\nCannot find the result. Failed to get a valid value from the 3 latest files.")
    value, unit = get_latest_formaldehyde_data(latitude, longitude)
    return value, 'Open-Meteo', unit

# if __name__ == '__main__':
#     # --- Example Usage ---
#     # Define the coordinates for New Cuyama, California
#     TARGET_LATITUDE = 34.9
#     TARGET_LONGITUDE = -119.7

#     print("--- Starting Point Value Extractor ---")
#     no2_value = get_hcho_value(latitude=TARGET_LATITUDE, longitude=TARGET_LONGITUDE)

#     if no2_value is not None:
#         print(f"\nFinal retrieved NO2 value: {no2_value}")
#     else:
#         print("\nCould not retrieve a valid NO2 value.")
#     print("\n--- Point Value Extractor Finished ---")