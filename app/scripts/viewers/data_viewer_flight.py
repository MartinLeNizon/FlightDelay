import pandas as pd

# Load the flight data
flight_file_path = '../data/raw/flight_data/flights_sample_3m.csv'
flights = pd.read_csv(flight_file_path)

# Preview the data
print(flights.head())
print(flights.info())
