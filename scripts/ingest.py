import os

# Set your Kaggle datasets
flight_dataset = "patrickzel/flight-delay-and-cancellation-dataset-2019-2023"
weather_dataset = "marslandis/largest-50-us-cities-weather-data-2020-to-2023"

# Download datasets using Kaggle API
os.system(f"kaggle datasets download -d {flight_dataset} --unzip -p ./data/raw/flight_data")
os.system(f"kaggle datasets download -d {weather_dataset} --unzip -p ./data/raw/weather_data")
