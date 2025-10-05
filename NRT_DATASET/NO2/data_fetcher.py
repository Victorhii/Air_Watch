import os
import datetime as dt
from dateutil import parser
import pandas as pd
from harmony import Client, Collection, Request
import earthaccess
from dotenv import load_dotenv


def fetch_and_manage_tempo_no2_granules(output_dir: str = "./NRT_DATASET/NO2/tempo_data"):

        """
        Fetches the 3 latest TEMPO NO2 granules, ensuring new data is downloaded
        before old data is removed. It maintains a CSV log of the current 3 granules.

        This function performs a "safe swap":
        1. Identifies the 3 latest granules available online.
        2. Compares with the local log to determine what's new and what's outdated.
        3. Downloads all new granules.
        4. Only after successful downloads, it deletes the outdated local files.
        5. Updates the CSV log to reflect the new set of 3 latest granules.

        Args:
            output_dir (str): The directory to save data files and the log.
        """
        # --- 1. SETUP ---
        load_dotenv()
        os.makedirs(output_dir, exist_ok=True)
        log_file = os.path.join(output_dir, "granule_log.csv")

        # TEMPO Level-3 NO2 Collection ID and variables of interest
        C_ID = "C3685668637-LARC_CLOUD"
        VARIABLES = [
            'product/vertical_column_troposphere',
            'product/main_data_quality_flag'
        ]

        # --- 2. LOAD LOCAL STATE ---
        # Define the columns the log file should have.
        log_columns = ['granule_id', 'start_time', 'end_time', 'local_filepath']

        # Initialize an empty DataFrame with the correct columns. This prevents the KeyError.
        current_log_df = pd.DataFrame(columns=log_columns)
        current_local_ids = set()

        if os.path.exists(log_file):
            try:
                # Read the existing log file
                temp_df = pd.read_csv(log_file)
                # Only proceed if the file is not empty
                if not temp_df.empty:
                    current_log_df = temp_df
                    current_local_ids = set(current_log_df['granule_id'])
                    print(f"Found {len(current_local_ids)} granules in the existing log file.")
                else:
                    print("Log file is empty. Will treat as a first-time run.")
            except Exception as e:
                print(f"Error reading log file: {e}. Starting fresh.")
        else:
            print("Log file not found. Will create a new one.")

        # --- 3. SEARCH FOR LATEST REMOTE GRANULES ---
        print("Authenticating with NASA Earthdata...")
        auth = earthaccess.login(strategy="environment")
        if not auth:
            print("Earthdata login failed. Please check your .env file.")
            return

        collections = earthaccess.search_datasets(
            concept_id=C_ID 
        )


        for c in collections:
            shortname = c['umm']['ShortName']
            print(c['umm']['CollectionCitations'][0])

            # Safely extract version if it exists
            versionid = c['umm']['CollectionCitations'][0].get('Version', None)

            # Build search arguments
            search_args = {
                "short_name": shortname,
                "cloud_hosted": True,
                "temporal": (dt.datetime.now() - dt.timedelta(days=3), dt.datetime.now())
            }

            if versionid:  # Only include if not None/empty
                search_args["version"] = versionid

            # Search the collection
            search_results = earthaccess.search_data(**search_args)


        # Sort results to find the absolute most recent ones
        latest_granules = sorted(
            search_results,
            key=lambda g: parser.isoparse(g['umm']['TemporalExtent']['RangeDateTime']['BeginningDateTime']),
            reverse=True
        )
        
        # Define the desired state: the top 3 latest granules from the server
        latest_3_remote_meta = latest_granules[:3]
        latest_remote_ids = {g['meta']['concept-id'] for g in latest_3_remote_meta}
        
        # --- 4. DETERMINE ACTIONS (DOWNLOAD/DELETE) ---
        granules_to_download_meta = [g for g in latest_3_remote_meta if g['meta']['concept-id'] not in current_local_ids]

        ids_to_delete = current_local_ids - latest_remote_ids
        # <<< FIX: Populate this DataFrame with the rows from the log that need deleting.
        granules_to_delete_df = current_log_df[current_log_df['granule_id'].isin(ids_to_delete)]

        # Only filter the log if it actually has data in it
        if not granules_to_download_meta and not granules_to_delete_df.empty:
            print("\nLocal data is already up-to-date. No new granules to download.")
            return
        elif not granules_to_download_meta:
            print("\nNo new granules to download. Local data is already up-to-date.")
            return

        print(f"\nFound {len(granules_to_download_meta)} new granules to download.")
        print(f"Identified {len(granules_to_delete_df)} old granules to be replaced.")

        # --- 5. DOWNLOAD NEW GRANULES FIRST ---
        username = os.getenv("EARTHDATA_USERNAME") # <<< ADD THIS
        password = os.getenv("EARTHDATA_PASSWORD") # <<< ADD THIS

        # Pass credentials to the client
        harmony_client = Client(auth=(username, password)) # <<< EDIT THIS LINE
        successfully_downloaded = []
        for granule in granules_to_download_meta:
            granule_id = granule['meta']['concept-id']
            start_time = granule['umm']['TemporalExtent']['RangeDateTime']['BeginningDateTime']
            end_time = granule['umm']['TemporalExtent']['RangeDateTime']['EndingDateTime']
            
            print(f"\nSubmitting Harmony request for Granule ID: {granule_id}")
            request = Request(
                collection=Collection(id=C_ID),
                granule_id=granule_id,
                variables=VARIABLES
            )

            if not request.is_valid():
                print(f"Request for {granule_id} is invalid. Skipping.")
                continue

            try:
                job_id = harmony_client.submit(request)
                print(f"Job ID: {job_id}. Waiting for processing...")
                harmony_client.wait_for_processing(job_id, show_progress=True)
                
                print(f"Downloading data for Job ID: {job_id}")
                results = harmony_client.download_all(job_id, directory=output_dir, overwrite=True)
                
                filepath = [f.result() for f in results][0]
                print(f"Successfully downloaded to: {filepath}")

                successfully_downloaded.append({
                    'granule_id': granule_id,
                    'start_time': start_time,
                    'end_time': end_time,
                    'local_filepath': filepath
                })
            except Exception as e:
                print(f"An error occurred while processing granule {granule_id}: {e}")
                print("Aborting operation to prevent data inconsistency.")
                return # Stop everything if a download fails

        # --- 6. ATOMIC SWAP: UPDATE LOG AND DELETE OLD FILES ---
        print("\nAll new granules downloaded. Proceeding with swap.")

        # a) Identify granules from the old log that we are keeping
        granules_to_keep_df = pd.DataFrame(columns=log_columns)
        if not current_log_df.empty:
            granules_to_keep_df = current_log_df[current_log_df['granule_id'].isin(latest_remote_ids)]

        # b) Create the new log dataframe from kept and newly downloaded granules
        newly_downloaded_df = pd.DataFrame(successfully_downloaded)
        updated_log_df = pd.concat([granules_to_keep_df, newly_downloaded_df], ignore_index=True)

        # c) **Now, delete the old physical files**
        if not granules_to_delete_df.empty:
            print(f"Cleaning up {len(granules_to_delete_df)} old granule file(s)...")
            
            for index, row in granules_to_delete_df.iterrows():
                # 1. It gets the full file path from the 'local_filepath' column
                old_filepath = row['local_filepath'] 
                
                try:
                    # 2. It checks if the file actually exists
                    if os.path.exists(old_filepath):
                        # 3. It DELETES the file from your disk
                        os.remove(old_filepath) 
                        print(f"   - Deleted: {os.path.basename(old_filepath)}")
                    else:
                        print(f"   - Warning: File not found, cannot delete: {old_filepath}")
                except Exception as e:
                    print(f"   - Error deleting file {old_filepath}: {e}")

        # d) **Finally, write the new log file**
        updated_log_df['start_time'] = pd.to_datetime(updated_log_df['start_time'], format='ISO8601')
        updated_log_df = updated_log_df.sort_values(by='start_time', ascending=False).reset_index(drop=True)
        
        updated_log_df.to_csv(log_file, index=False)
        print(f"\nLog file '{log_file}' has been successfully updated.")



    # if __name__ == '__main__':
    #     print("--- Starting Data Fetcher & Manager ---")
    #     fetch_and_manage_tempo_no2_granules()
    #     print("\n--- Data Fetcher & Manager Finished ---")