import pandas as pd

# Load the weather data
weather_file_path = '../data/raw/weather_data/largest_us_cities_weatherdata_2020_2023.csv'
weather = pd.read_csv(weather_file_path)

# Preview the data
print(weather.head())
print(weather.info())
