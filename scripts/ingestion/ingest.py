import os

# Set your Kaggle datasets
flight_dataset = "patrickzel/flight-delay-and-cancellation-dataset-2019-2023"
weather_dataset = "marslandis/largest-50-us-cities-weather-data-2020-to-2023"

# Define paths
flight_data_path = "../data/raw/flight_data"
weather_data_path = "../data/raw/weather_data"

# Download datasets only if the directories don't exist
if not os.path.exists(flight_data_path):
    os.makedirs(flight_data_path, exist_ok=True)
    os.system(f"kaggle datasets download -d {flight_dataset} --unzip -p {flight_data_path}")
else:
    print(f"Flight data already exists at {flight_data_path}. Skipping download.")

if not os.path.exists(weather_data_path):
    os.makedirs(weather_data_path, exist_ok=True)
    os.system(f"kaggle datasets download -d {weather_dataset} --unzip -p {weather_data_path}")
else:
    print(f"Weather data already exists at {weather_data_path}. Skipping download.")
