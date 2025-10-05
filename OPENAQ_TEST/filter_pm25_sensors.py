import json

# Load JSON file
with open("active_pollutants_by_location.json", "r") as f:
    data = json.load(f)

# Collect IDs that contain "pm25"
pm25_ids = [key for key, values in data.items() if "pm25" in values]
input_file = "filtered_openaq_locations.txt"     # your txt file
output_file = "filtered_stations_with_pm25.txt"

with open(input_file, "r", encoding="utf-8", errors="ignore") as infile, \
     open(output_file, "w", encoding="utf-8") as outfile:
    
    for line in infile:
        # Get the first part before " | " (station ID)
        station_id = line.split("|")[0].strip()
        
        # Keep the row if station_id matches pm25_ids
        if station_id in pm25_ids:
            outfile.write(line)

print(f"Filtered file saved to {output_file}")
