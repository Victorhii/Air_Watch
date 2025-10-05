import pandas as pd
import numpy as np
import joblib
import json

# --- HELPER FUNCTION (Must be identical to the one used in training) ---
def lat_lon_to_cartesian(lat, lon):
    """Converts latitude and longitude to 3D Cartesian coordinates."""
    lat_rad = np.deg2rad(lat)
    lon_rad = np.deg2rad(lon)
    x = np.cos(lat_rad) * np.cos(lon_rad)
    y = np.cos(lat_rad) * np.sin(lon_rad)
    z = np.sin(lat_rad)
    return x, y, z

# --- CORE PREDICTION FUNCTION ---
def predict_single_instance(input_data, model, scaler):
    """
    Loads the trained model and scaler to make a prediction on a single
    data instance provided as a dictionary (from JSON).

    Args:
        input_data (dict): A dictionary containing all required input features.
        model_path (str): The file path to the saved XGBoost model (.joblib).
        scaler_path (str): The file path to the saved StandardScaler (.joblib).

    Returns:
        float: The predicted single value.
    """
    try:

        print("✅ Model and scaler loaded successfully.")

    except FileNotFoundError as e:
        print(f"❌ Error: Could not find model/scaler file. Make sure these paths are correct:\n- {model}\n- {scaler}")
        raise e

    # 2. Convert the input dictionary to a pandas DataFrame
    # The input_data dict is wrapped in a list to create a single-row DataFrame
    input_df = pd.DataFrame([input_data])
    print("\nOriginal Input Data:")
    print(input_df[['lat', 'lon', 'industrial', 'population']].to_string(index=False))

    # 3. Apply the *exact same* feature engineering as in training
    input_df['x'], input_df['y'], input_df['z'] = lat_lon_to_cartesian(input_df['lat'], input_df['lon'])
    
    # 4. Ensure the feature order is identical to the one used for training
    # This is a critical step!
    features_in_order = [
        'industrial', 'road', 'population', 'elev', 'prectot', 'rh', 
        'temp', 'x', 'y', 'z'
    ]
    
    # Reorder DataFrame columns to match the model's expectation
    X = input_df[features_in_order]
    print("\nData after Feature Engineering (Ready for Scaling):")
    print(X.to_string(index=False))

    # 5. Scale the features using the loaded scaler
    # IMPORTANT: Use .transform() only, NOT .fit_transform()
    X_scaled = scaler.transform(X)

    # 6. Make the prediction
    prediction = model.predict(X_scaled)
    
    # The model outputs a numpy array, so we extract the single value
    return round(float(prediction[0]), 2)

# # --- MAIN EXECUTION BLOCK ---
# if __name__ == '__main__':
#     # Define the paths to your saved model and scaler
#     MODEL_FILE_PATH = './model/xgboost_model_v2.joblib'
#     SCALER_FILE_PATH = './model/data_scaler_v2.joblib'

#     # --- Example Usage ---
#     # This simulates a JSON input. In a real application (like a web API),
#     # you would get this from a request body (e.g., using Flask's request.json).
#     sample_input_json = {
#         "lat": 3.1390,
#         "lon": 101.6869,
#         "industrial": 0.65,
#         "road": 150.5,
#         "population": 30000,
#         "elev": 50.0,
#         "prectot": 2.5,
#         "rh": 85.2,
#         "temp": 27.5,
#         "wind_speed": 5.1
#     }

#     print("--- Starting Prediction Process ---")
    
#     # Make sure the model and scaler files exist before proceeding
#     import os
#     if not os.path.exists(MODEL_FILE_PATH) or not os.path.exists(SCALER_FILE_PATH):
#         print("\n⚠️  Error: Model or scaler file not found.")
#         print("Please run the training script first to generate 'xgboost_model_v2.joblib' and 'data_scaler_v2.joblib'.")
#     else:
#         # Call the prediction function with the sample data
#         predicted_value = predict_single_instance(sample_input_json, MODEL_FILE_PATH, SCALER_FILE_PATH)
    
#         # Print the final result
#         print("\n------------------------------------")
#         print(f"📈 Predicted Value: {predicted_value:.4f}")
#         print("------------------------------------")