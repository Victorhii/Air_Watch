import pandas as pd
import json

# Read the CSV and JSON files
df = pd.read_csv('output_with_data.csv') # Make sure to replace 'your_data.csv' with the actual name of your csv file
with open('sensors_formatted.json', 'r') as f: # Make sure to replace 'your_points.json' with the actual name of your json file
    json_data = json.load(f)

# Extract lat/lon pairs from the JSON file into a set for efficient lookups
json_points = set()
for point in json_data['points']:
    json_points.add((point['lat'], point['lon']))

# Filter the DataFrame
# The script will keep rows where the tuple of (lat, lon) is present in our set of json_points
filtered_df = df[df.apply(lambda row: (row['lat'], row['lon']) in json_points, axis=1)]

# Display the filtered DataFrame
print("Filtered CSV data:")
print(filtered_df)

# Save the filtered data to a new CSV file
filtered_df.to_csv('filtered_dataset_latlon.csv', index=False)