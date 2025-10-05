import requests
import json
import time
def get_weather_forecast(latitude, longitude):
    """
    Retrieves a 7-day weather forecast from the Open-Meteo API
    and formats it into a JSON object.
    """
    
    url = "https://api.open-meteo.com/v1/forecast"
    
    # --- The line below has been corrected ---
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": "temperature_2m_mean,relative_humidity_2m_mean,precipitation_sum", # Corrected variable name
        "timezone": "auto",
        "forecast_days": 7
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        forecast_data = {}
        daily_data = data.get("daily", {})
        
        dates = daily_data.get("time", [])
        temps = daily_data.get("temperature_2m_mean", [])
        humidity = daily_data.get("relative_humidity_2m_mean", [])
        precip_sum = daily_data.get("precipitation_sum", [])

        for i, date_str in enumerate(dates):
            try:
                # Calculate average precipitation in mm/hr
                avg_precip_mm_hr = precip_sum[i] / 24.0
                
                forecast_data[date_str] = {
                    "temp": round(temps[i], 2),
                    "rh": round(humidity[i], 2),
                    "prectot": round(avg_precip_mm_hr, 4)
                }
            except IndexError:
                # If any list is missing data for this day, skip it.
                print(f"Warning: Missing data for date {date_str}. Skipping this day.")
            
        return json.dumps(forecast_data, indent=4)

    except requests.exceptions.RequestException as e:
        return f"Error fetching data from API: {e}"
    except KeyError:
        return "Error: Unexpected data format received from the API."

# --- Run the function and print the result ---
if __name__ == "__main__":
    
    weather_json = get_weather_forecast(40, -90)
    print(weather_json)
    