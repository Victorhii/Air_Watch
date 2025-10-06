import json

# Define the input and output filenames
input_filename = 'sensors.json'
output_filename = 'sensors_formatted.json'

try:
    # --- 1. Load the original JSON file ---
    with open(input_filename, 'r') as f:
        original_data = json.load(f)

    # --- 2. Create the new data structure ---
    
    # This list will hold the new coordinate objects
    formatted_list = []

    # Loop through each "lat,lon" string in the original data
    for lat_lon_string in original_data.keys():
        
        # Split the string into two parts at the comma
        parts = lat_lon_string.split(',')
        
        # Convert the string parts to floating-point numbers
        latitude = float(parts[0])
        longitude = float(parts[1])
        
        # Create the new dictionary { "lon": ..., "lat": ... }
        new_coord_object = {
            "lon": longitude,
            "lat": latitude
        }
        
        # Add this new object to our list
        formatted_list.append(new_coord_object)

    # Create the final JSON structure with a top-level key
    final_json_structure = {
        "coordinates": formatted_list
    }

    # --- 3. Save the newly formatted data ---
    with open(output_filename, 'w') as f:
        # Use indent=2 for nice, readable formatting
        json.dump(final_json_structure, f, indent=2)

    print(f"✅ Success! Your data has been reformatted and saved to '{output_filename}'.")


except FileNotFoundError:
    print(f"❌ Error: '{input_filename}' not found. Make sure it's in the same folder as the script.")
except Exception as e:
    print(f"❌ An error occurred: {e}")