import pandas as pd

# --- Step 1: Read the CSV file ---
# Replace 'your_file.csv' with the actual name of your CSV file.
try:
    df = pd.read_csv('cleaned_data.csv')
except FileNotFoundError:
    print("Error: The file 'your_file.csv' was not found.")
    print("Please make sure the file is in the same directory as the script,")
    print("or provide the full path to the file.")
    exit()

# --- Step 2: Correct the 'data' column ---
# This line rounds the 'data' column to 2 decimal places.
df['data'] = df['data'].round(2)

# # --- Step 3: Display the corrected data ---
# print("Corrected Data:")
# print(df)

# --- (Optional) Step 4: Save the corrected data to a new CSV file ---
# If you want to save the corrected data to a new file, uncomment the following line.
df.to_csv('corrected_data.csv', index=False)
print("\nCorrected data saved to 'corrected_data.csv'")