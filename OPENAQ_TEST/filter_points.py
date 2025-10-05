import json

def reduce_json_points(input_file, output_file, desired_points):
    """
    Reads a JSON file with a list of points, reduces the number of points
    uniformly, and saves the result to a new file.
    """
    try:
        # 1. Read the original JSON file
        with open(input_file, 'r') as f:
            data = json.load(f)

        original_points = data['points']
        original_count = len(original_points)
        print(f"Found {original_count} points in '{input_file}'.")

        if original_count < desired_points:
            print("Error: The file already has fewer points than desired.")
            return

        # 2. Calculate the interval to select points uniformly
        # To get 250 points from 1000, we need to pick 1 out of every 4.
        step = original_count // desired_points
        
        # 3. Select every 'step'-th point from the list
        # This is the key step for uniform reduction.
        reduced_points = original_points[::step]
        new_count = len(reduced_points)
        
        # 4. Update the data structure with the new list of points
        data['points'] = reduced_points
        
        # 5. Update the 'n_points' in the metadata
        if 'metadata' in data and 'n_points' in data['metadata']:
            data['metadata']['n_points'] = new_count
        
        # 6. Save the new data to the output file
        # Using indent=2 makes the new JSON file readable.
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
            
        print(f"Successfully reduced points to {new_count} and saved to '{output_file}'.")

    except FileNotFoundError:
        print(f"Error: The file '{input_file}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# --- Main execution ---
if __name__ == "__main__":
    INPUT_FILENAME = 'reduced_points.json'
    OUTPUT_FILENAME = 'reduced_points.json'
    TARGET_POINTS = 100
    
    reduce_json_points(INPUT_FILENAME, OUTPUT_FILENAME, TARGET_POINTS)