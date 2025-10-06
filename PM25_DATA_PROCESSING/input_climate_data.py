import xarray as xr
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
import json
import time
import sys
import math
# To this (just the class name):
import concurrent
from concurrent.futures import ProcessPoolExecutor
# Add this import at the top of the main block
from multiprocessing import Manager


# =========================================================================
# --- Configuration Constants for NASA GEOS-5 ---
# =========================================================================

# URL for GEOS-5 Assimilation 2D Single Level Variables (tavg1_2d_slv_Nx)
GEOS_SLV_URL = "https://opendap.nccs.nasa.gov/dods/GEOS-5/fp/0.25_deg/assim/tavg1_2d_slv_Nx"
# URL for GEOS-5 Assimilation 2D Flux Variables (tavg1_2d_flx_Nx) for precipitation
GEOS_FLX_URL = "https://opendap.nccs.nasa.gov/dods/GEOS-5/fp/0.25_deg/assim/tavg1_2d_flx_Nx"

# Dictionary for variables from the SLV dataset
GEOS_SLV_VARIABLES = {
    'qv2m': 'Specific_Humidity_2m_kg/kg',
    'v2m': 'V2M_North_m/s',
    'u2m': 'U2M_East_m/s',
    't2m': 'Air_Temperature_2m_K',
    'ps': 'Surface_Pressure_Pa'
}

# The GEOS-5 flux files contain a 1-hour average *rate* (kg/m^2/s).
SECONDS_PER_HOUR = 3600

# =========================================================================
# --- Helper Functions ---
# =========================================================================

def specific_humidity_to_rh(q, T_celsius, p_hpa):
    """
    Convert specific humidity to relative humidity.
    """
    if q < 0: q = 0
    if q >= 1: return 100.0
    w = q / (1 - q)
    e = (w * p_hpa) / (0.622 + w)
    es = 6.112 * math.exp((17.67 * T_celsius) / (T_celsius + 243.5))
    RH = (e / es) * 100
    return max(0.0, min(RH, 100.0))

# =========================================================================
# --- Core Data Fetching Functions (Unchanged) ---
# =========================================================================

def fetch_slv_data(lat, lon, target_datetime_utc):
    """
    Fetches single-level variables, calculates mean values,
    and converts to final units (Celsius, Relative Humidity).
    """
    print(f"[INFO] Targeting GEOS-5 SLV data from: {GEOS_SLV_URL.split('/')[-1]}")
    
    start_time_utc = target_datetime_utc
    end_time_utc = start_time_utc + timedelta(hours=1)

    print(f"[INFO] Requesting SLV data slice from {start_time_utc.strftime('%Y-%m-%d %H:%M UTC')} to {end_time_utc.strftime('%Y-%m-%d %H:%M UTC')}")

    try:
        with xr.open_dataset(GEOS_SLV_URL, engine='netcdf4') as ds:
            corrected_time_index = pd.to_datetime(ds['time'].values).tz_localize(None).tz_localize(timezone.utc)
            ds = ds.assign_coords(time=corrected_time_index)
            
            target_lon = lon % 360
            if target_lon > 180:
                target_lon -= 360
                
            ds_time_slice = ds.sel(time=slice(start_time_utc, end_time_utc - timedelta(seconds=1)))
            ds_point = ds_time_slice.sel(lat=lat, lon=target_lon, method='nearest', tolerance=0.5)

            data_vars = list(GEOS_SLV_VARIABLES.keys())
            ds_vars = ds_point[data_vars]
            
            combined_df = ds_vars.to_dataframe().reset_index()
            
            combined_df['Wind_Speed_2m_m/s'] = np.sqrt(combined_df['u2m']**2 + combined_df['v2m']**2)
            
            final_name_map = {geos_name: user_name for geos_name, user_name in GEOS_SLV_VARIABLES.items()}
            renamed_df = combined_df.rename(columns=final_name_map)
            
            mean_raw_values = renamed_df.drop(columns=['time', 'lat', 'lon']).mean()
            
            T_kelvin = mean_raw_values['Air_Temperature_2m_K']
            q_specific = mean_raw_values['Specific_Humidity_2m_kg/kg']
            p_pascal = mean_raw_values['Surface_Pressure_Pa']

            T_celsius = T_kelvin - 273.15
            p_hpa = p_pascal / 100.0
            
            relative_humidity = specific_humidity_to_rh(q_specific, T_celsius, p_hpa)

            final_mean_values = {
                'Air_Temperature_2m_C': round(float(T_celsius), 2),
                'Relative_Humidity_%': round(float(relative_humidity), 2),
                'Wind_Speed_2m_m/s': round(float(mean_raw_values['Wind_Speed_2m_m/s']), 2)
            }
            
            return final_mean_values
    
    except Exception as e:
        error_message = str(e).strip().replace('\n', ' ')
        print(f"[ERROR] Failed to process GEOS-5 SLV data: {type(e).__name__}: {error_message[:100]}...")
        return None

def fetch_precipitation_data(lat, lon, target_datetime_utc):
    """
    Fetches and calculates the AVERAGE PRECIPITATION RATE in mm/hr.
    """
    print(f"[INFO] Targeting GEOS-5 FLX data from: {GEOS_FLX_URL.split('/')[-1]}")

    start_time_utc = target_datetime_utc
    end_time_utc = start_time_utc + timedelta(hours=1)
    
    print(f"[INFO] Requesting FLX data slice from {start_time_utc.strftime('%Y-%m-%d %H:%M UTC')} to {end_time_utc.strftime('%Y-%m-%d %H:%M UTC')}")

    try:
        with xr.open_dataset(GEOS_FLX_URL, engine='netcdf4') as ds:
            corrected_time_index = pd.to_datetime(ds['time'].values).tz_localize(None).tz_localize(timezone.utc)
            ds = ds.assign_coords(time=corrected_time_index)

            target_lon = lon % 360
            if target_lon > 180:
                target_lon -= 360

            ds_time_slice = ds.sel(time=slice(start_time_utc, end_time_utc - timedelta(seconds=1)))
            ds_point = ds_time_slice.sel(lat=lat, lon=target_lon, method='nearest', tolerance=0.5)
            
            prectot_series = ds_point['prectot'].to_series().dropna()
            
            if prectot_series.empty:
                print("[WARNING] No precipitation data found for the specified period. Returning 0.")
                return 0.0

            mean_rate_kg_m2_s = prectot_series.mean()
            mean_rate_mm_hr = mean_rate_kg_m2_s * SECONDS_PER_HOUR
            
            return round(float(mean_rate_mm_hr), 2)
            
    except Exception as e:
        error_message = str(e).strip().replace('\n', ' ')
        print(f"[ERROR] Failed to process GEOS-5 precipitation data: {type(e).__name__}: {error_message[:100]}...")
        return None


# =========================================================================
# --- NEW: Worker Function for Parallel Processing ---
# =========================================================================

# In your main script, before you start the parallel processing, define the correct column order
# This must match your output CSV file's header exactly.
CORRECT_COLUMN_ORDER = [
    'year', 'date', 'time', 'lat', 'lon',
    'prectot', 'rh', 'temp', 'wind_speed', 'data'
]

def process_row(index, row, output_filename, lock, column_order): # Pass the order in
    """
    Worker function that processes a single row and appends the result
    directly to the output CSV file in a process-safe way.
    """
    output_data = row.to_dict()
    
    try:
        # ... (your data fetching logic remains the same) ...
        target_lat = float(row['lat'])
        target_lon = float(row['lon'])
        date_str = str(row['date'])
        time_str = str(row['time'])
        target_hour, _ = map(int, time_str.split(':'))
        dt_naive = datetime.strptime(
            f"{row['year']}-{date_str} {target_hour:02d}:00",
            '%Y-%m-%d %H:%M'
        )
        target_datetime_utc = dt_naive.replace(tzinfo=timezone.utc)
        
        print(f"[Worker] Starting Row {index+1} for {target_datetime_utc.strftime('%Y-%m-%d %H:%M UTC')}")

        slv_data = fetch_slv_data(target_lat, target_lon, target_datetime_utc)
        if slv_data is None:
            raise Exception("Failed to retrieve SLV weather data.")
        
        avg_precipitation_rate = fetch_precipitation_data(target_lat, target_lon, target_datetime_utc)
        if avg_precipitation_rate is None:
            raise Exception("Failed to retrieve precipitation data.")

        output_data['temp'] = slv_data['Air_Temperature_2m_C']
        output_data['rh'] = slv_data['Relative_Humidity_%']
        output_data['wind_speed'] = slv_data['Wind_Speed_2m_m/s']
        output_data['prectot'] = avg_precipitation_rate

    except Exception as e:
        print(f"[Worker ERROR] Failed on row {index+1}: {e}")
        output_data['temp'] = -9999.0
        output_data['rh'] = -9999.0
        output_data['wind_speed'] = -9999.0
        output_data['prectot'] = -9999.0
    
    lock.acquire()
    try:
        df_to_append = pd.DataFrame([output_data])
        
        # --- FIX: Enforce the correct column order before writing ---
        df_to_append_ordered = df_to_append[column_order]
        
        df_to_append_ordered.to_csv(output_filename, mode='a', header=False, index=False)
        
    finally:
        lock.release()

    return f"Row {index+1} completed."

# When you call this function in your multiprocessing pool, make sure to pass the list:
# e.g., pool.starmap(process_row, [(i, row, 'out.csv', lock, CORRECT_COLUMN_ORDER) for i, row in df.iterrows()])
# =========================================================================
# --- MODIFIED: Main Execution to update CSV row-by-row ---
# =========================================================================


# =========================================================================
# --- REVISED: Main Execution with Parallel Processing ---
# =========================================================================

if __name__ == '__main__':
    
    from multiprocessing import Manager
    from concurrent.futures import ProcessPoolExecutor, as_completed

    INPUT_CSV_PATH = 'corrected_data.csv'
    OUTPUT_CSV_PATH = 'locations_with_weather_live.csv' # New output file
    MAX_WORKERS = 5

    try:
        input_df = pd.read_csv(INPUT_CSV_PATH)
        print(f"[INFO] Successfully loaded {len(input_df)} rows from {INPUT_CSV_PATH}")
    except FileNotFoundError:
        print(f"[CRITICAL ERROR] The file '{INPUT_CSV_PATH}' was not found.")
        sys.exit(1)

    # --- 1. Prepare the output file with a header ONCE ---
    # Create a copy to define the structure and headers
    output_df_structure = input_df.copy()
    new_columns = ['prectot', 'rh', 'temp', 'wind_speed']
    insertion_point = output_df_structure.columns.get_loc('lon') + 1
    
    for i, col_name in enumerate(new_columns):
        if col_name not in output_df_structure.columns:
            output_df_structure.insert(insertion_point + i, col_name, np.nan)
    
    # Write the header to the new file. The file is now ready for appending.
    output_df_structure.head(0).to_csv(OUTPUT_CSV_PATH, index=False)
    print(f"[INFO] Output file created with headers: {OUTPUT_CSV_PATH}")

    # --- 2. Create a Manager and a Lock to share between processes ---
    manager = Manager()
    lock = manager.Lock()

    print(f"\n[INFO] Starting parallel processing with {MAX_WORKERS} workers...")
    
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        
        # Submit jobs, passing the filename and the lock to each worker
        futures = [executor.submit(process_row, index, row, OUTPUT_CSV_PATH, lock, CORRECT_COLUMN_ORDER
                                   ) 
                   for index, row in input_df.iterrows()]

        # --- 3. Track progress as jobs complete ---
        total_rows = len(input_df)
    
        for i, future in enumerate(as_completed(futures)):
            result_message = future.result() # Catches any errors from the worker
            print(f"--- PROGRESS: {i + 1} / {total_rows} rows saved. Last message: '{result_message}' ---")

    print(f"\n🎉 All rows processed and saved instantly. Final output is in: {OUTPUT_CSV_PATH} 🎉")