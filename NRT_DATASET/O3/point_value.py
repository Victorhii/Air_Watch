# point_value_extractor.py

import pandas as pd
import xarray as xr
import numpy as np
import os

import xarray as xr
import numpy as np
from datetime import datetime, timedelta
import pytz


def get_point_value(da: xr.DataArray, qc_da: xr.DataArray | None, lat: float, lon: float):
    """
    Extracts a single point value from a DataArray using either interpolation
    or the nearest neighbor based on local data quality and variability.

    If qc_da is None, the quality check is skipped, and the decision is based
    only on data variability.

    Args:
        da (xr.DataArray): The data variable array (e.g., NO2 column).
        qc_da (xr.DataArray | None): The quality control flag array. Can be None.
        lat (float): The target latitude.
        lon (float): The target longitude.

    Returns:
        float: The calculated value, or np.nan if unable to produce a valid result.
    """
    # --- 1. SETUP and COORDINATE DETECTION ---
    if "time" in da.dims and da.sizes["time"] > 1:
        arr = da.isel(time=0)
        # Handle qc_da potentially being None
        qc = qc_da.isel(time=0) if qc_da is not None else None
    else:
        arr = da
        qc = qc_da

    latname = next((n for n in ("latitude", "lat", "y") if n in arr.coords), None)
    lonname = next((n for n in ("longitude", "lon", "x") if n in arr.coords), None)
    if latname is None or lonname is None:
        raise ValueError("Latitude/Longitude coordinates not found in DataArray")

    # --- 2. GET NEAREST AND INTERPOLATED VALUES ---
    try:
        nearest_pt = arr.sel({latname: lat, lonname: lon}, method="nearest")
        nearest_val = nearest_pt.values.item()

        interp_pt = arr.interp({latname: lat, lonname: lon})
        interp_val = interp_pt.values.item()
    except (IndexError, KeyError, ValueError) as e:
        print(f"Could not select data for point ({lat}, {lon}). Error: {e}")
        return np.nan

    # --- 3. DECISION LOGIC BASED ON NEIGHBORHOOD ---
    # Find indices of the 3x3 grid around the nearest point
    lat_idx = np.argmin(np.abs(arr[latname].values - lat))
    lon_idx = np.argmin(np.abs(arr[lonname].values - lon))

    i0 = max(0, lat_idx - 1)
    i1 = min(len(arr[latname]) - 1, lat_idx + 1)
    j0 = max(0, lon_idx - 1)
    j1 = min(len(arr[lonname]) - 1, lon_idx + 1)

    subset = arr.isel({latname: slice(i0, i1 + 1), lonname: slice(j0, j1 + 1)})
    
    # Get a mask for valid (non-NaN) data points
    valid_mask = np.isfinite(subset.values)
    valid_vals = subset.values[valid_mask]

    if valid_vals.size == 0:
        decision = "nearest"  # Fallback if no valid neighbors
    else:
        local_mean = np.mean(valid_vals)
        local_std = np.std(valid_vals)
        rel_std = local_std / abs(local_mean) if abs(local_mean) > 1e-9 else 0

        # Start with the variability condition
        should_interp = (rel_std < 0.10)

        # If quality control data is available, apply its condition as well
        if qc is not None:
            subset_qc = qc.isel({latname: slice(i0, i1 + 1), lonname: slice(j0, j1 + 1)})
            # Get QC values corresponding to the valid data points
            valid_qcs = subset_qc.values[valid_mask]

            if valid_qcs.size > 0:
                qc_good_fraction = np.sum(valid_qcs == 0) / valid_qcs.size
                # Both conditions (variability and quality) must be true
                should_interp = should_interp and (qc_good_fraction >= 0.66)
            else:
                # If no valid QC flags exist for the valid data, be conservative
                should_interp = False

        decision = "interp" if should_interp else "nearest"

    # --- 4. RETURN FINAL VALUE ---
    final_value = interp_val if decision == 'interp' else nearest_val

    if np.isnan(final_value):
        return np.nan

    # Scale the value as in the original script and round
    return round(final_value, 2)

import requests
from dotenv import load_dotenv

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
        return round(o3, 2), 'µg/m³'


    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response content: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred during the request: {req_err}")
    except KeyError:
        print("Error: Could not parse the expected data from the API response.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def get_o3_value(latitude: float, longitude: float, data_dir: str = "./NRT_DATASET/O3/tempo_data"):
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
                data_array = datatree['product/troposphere_ozone_column']
                quality_flags = None
                
                value = get_point_value(data_array, quality_flags, lat=latitude, lon=longitude)
                
                if value is not None and not np.isnan(value) and value > 0:
                    end_time_str = row['end_time']
                    granule_end_time = pd.to_datetime(end_time_str).tz_convert('UTC')
                    current_utc_time = datetime.now(pytz.utc)

                    # If this specific granule's data is older than 2 hours, discard it and continue.
                    if (current_utc_time - granule_end_time) > timedelta(hours=2):
                        print(f"-> Value found, but data is from {granule_end_time.strftime('%H:%M:%S UTC')} (>2 hours old). Trying next file.")
                        value, unit = get_WeatherAPI_data(latitude, longitude)
                        return value, 'WeatherAPI', unit
                    
                    # If the value is valid AND the data is recent, it's a success.
                    print(f"✓ Success! Found valid, recent data point: {value} (mol/m^2 * 1e15)")
                    return value, 'Tempo', 'DU'
                    # ===========================================================

                else:
                    print(f"-> Value was 'nan' or negative - {value}. Trying next available file...")

        except Exception as e:
            print(f"-> Could not process file {os.path.basename(file_path)}. Error: {e}")
            continue
            
    print("\nCannot find the result. Failed to get a valid value from the 3 latest files.")
    value, unit = get_WeatherAPI_data(latitude, longitude)
    return value, 'WeatherAPI', unit

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