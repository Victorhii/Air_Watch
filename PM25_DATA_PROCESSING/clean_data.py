import pandas as pd
import io

# --- Step 1: Load the data ---

# In a real scenario, you would load your data from a file like this:
try:
    df = pd.read_csv('final_filter.csv')
except FileNotFoundError:
    print("Error: Make sure 'your_data.csv' is in the same directory as the script.")
    exit()

# # For this example, we'll use the data you provided.
# # I've added a row with an empty 'data' field to demonstrate that case.
# csv_data = """year,date,time,lat,lon,granuleid,data
# 2025,09-28,19:58,35.903383,-60.97679,G3778052022-LARC_CLOUD,0.42
# 2025,09-28,17:58,54.611341,-81.093475,G3777989161-LARC_CLOUD,1.04
# 2025,09-28,16:58,60.453573,-37.078656,G3777968683-LARC_CLOUD,0.27
# 2025,09-28,21:58,24.928051,-101.791607,G3778104574-LARC_CLOUD,0.49
# 2025,09-28,21:58,28.676541,-97.812181,G3778104574-LARC_CLOUD,1.02
# 2025,09-28,16:58,43.561979,-105.125882,G3777968683-LARC_CLOUD,0.14
# 2025,09-28,16:58,51.227498,-125.35484,G3777968683-LARC_CLOUD,0.33
# 2025,09-28,13:08,42.716425,-91.475778,G3777807417-LARC_CLOUD,0.47
# 2025,09-28,12:28,44.373521,-82.769429,G3777753260-LARC_CLOUD,1.3
# 2025,09-28,19:58,55.44144,-75.280553,G3778052022-LARC_CLOUD,0.35
# 2025,09-28,18:58,33.664117,-77.091558,G3778025937-LARC_CLOUD,2.14
# 2025,09-28,12:28,51.811109,-62.074904,G3777753260-LARC_CLOUD,-0.46
# 2025,09-28,15:10,52.111111,-63.111111,G3777753261-LARC_CLOUD,
# """

# # Use io.StringIO to read the string data as if it were a file
# df = pd.read_csv(io.StringIO(csv_data))

print("--- Original Data ---")
print(df)
print(f"\nOriginal number of rows: {len(df)}")


# --- Step 2: Clean the data ---

# When pandas reads a CSV, it usually interprets empty values in a numeric
# column as 'NaN' (Not a Number). We can drop these rows first.
cleaned_df = df.dropna(subset=['data'])

# Next, we filter the DataFrame to keep only the rows where
# the 'data' column is greater than or equal to 0.
cleaned_df = cleaned_df[cleaned_df['data'] > 0]


# # --- Step 3: Show the result ---

# print("\n--- Cleaned Data ---")
# print(cleaned_df)
# print(f"\nNumber of rows after cleaning: {len(cleaned_df)}")


# --- Step 4 (Optional): Save the cleaned data to a new file ---

# You can save the cleaned DataFrame to a new CSV file by uncommenting the next line.
# The 'index=False' argument prevents pandas from writing a new index column.
cleaned_df.to_csv('cleaned_data.csv', index=False)