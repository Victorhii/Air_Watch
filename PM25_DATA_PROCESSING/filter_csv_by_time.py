import pandas as pd

# Load your CSV file into a pandas DataFrame
# Make sure to replace 'your_file.csv' with the actual name of your file
df = pd.read_csv('filtered_dataset_date.csv')



# --- Option 2: Filter by a List of Month-Day values ---

# Create a list of the specific 'mm-dd' dates you want to filter for
time_to_filter = ['00:00', '02:00', '04:00', '06:00', '08:00', '10:00', '12:00', '14:00', '16:00', '18:00', '20:00', '22:00']

# Use the .isin() method to find rows with dates in your list
filtered_df_list = df[df['time'].isin(time_to_filter)]

# Save the result to another new CSV file
filtered_df_list.to_csv('final_filter.csv', index=False)

print("\nFiltered by a list of dates and saved to 'output_list.csv'")
print(filtered_df_list)