import pandas as pd

def process_csv(input_file, output_file):
    """
    Reads a CSV file, removes specified columns, and saves the result to a new file.

    Args:
        input_file (str): The path to the input CSV file.
        output_file (str): The path to save the modified CSV file.
    """
    try:
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(input_file)
        print(f"Successfully loaded '{input_file}'.")

        # Define the list of columns to remove
        columns_to_drop = ['wind_speed']
        
        # Check which of the columns to drop actually exist in the DataFrame
        existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]

        if not existing_columns_to_drop:
            print("The specified columns to remove were not found in the file.")
            return

        # Drop the existing columns
        df.drop(columns=existing_columns_to_drop, inplace=True)
        print(f"Removed columns: {', '.join(existing_columns_to_drop)}")

        # Save the modified DataFrame to a new CSV file without the index
        df.to_csv(output_file, index=False)
        print(f"Successfully saved the modified data to '{output_file}'.")

    except FileNotFoundError:
        print(f"Error: The file '{input_file}' was not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Define the names of your input and output files
    input_filename = 'train.csv'
    output_filename = 'train.csv'
    
    # Run the processing function
    process_csv(input_filename, output_filename)
