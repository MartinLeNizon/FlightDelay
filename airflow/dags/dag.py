from datetime import datetime, timedelta
import airflow
import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.utils.task_group import TaskGroup

# =================== Environment ===================

PRODUCTION_MODE = False     # PLEASE CHANGE WITH CAUTION, CONSUMES API KEY TOKENS


# =================== Credentials ===================
POSTGRES_CONNECTION_ID = 'postgres_default'

# =================== Path ===================

INGESTION_DATA_PATH = 'data/ingestion/'
STAGING_DATA_PATH = 'data/staging/'


AVIATION_EDGE_API_KEY_PATH = '.aviationedge/api.txt'

INGESTED_FLIGHTS_PREFIX = 'flight_data_'

WEATHER_DATA_JSON = 'weather_data.json'


# =========================================================

# =================== Global DAG definition ===================

# Default arguments for the DAG
default_args = {
    'concurrency': 1,
    'schedule_interval': None,
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

global_dag = DAG(
    dag_id='daily_flight_weather_ingestion',
    start_date=airflow.utils.dates.days_ago(0),
    default_args=default_args,
    catchup=False,
)

# =========================================================


# =================== Python Functions ===================

from datetime import datetime

def convert_timestamp_to_iso(timestamp):
    """
    Convert timestamp in 'DDHHMMZ' format to ISO 8601 format 'YYYY-MM-DDTHH:MM:SS.mmm'.
    Assumes the timestamp refers to the current year and month.
    """
    current_year_month = datetime.now().strftime("%Y-%m-")  # Get current year and month (e.g., 2024-12-)
    
    # Extract day, hour, and minute from the timestamp
    day = timestamp[:2]
    hour = timestamp[2:4]
    minute = timestamp[4:6]
    
    # Create the full datetime string and convert it to the desired format
    formatted_time = current_year_month + day + f" {hour}:{minute}:00"  # 'YYYY-MM-DD HH:MM:00'
    
    # Convert to ISO 8601 format (with milliseconds)
    formatted_time = datetime.strptime(formatted_time, "%Y-%m-%d %H:%M:%S")
    return formatted_time.isoformat()  # Returns in the format 'YYYY-MM-DDTHH:MM:SS.mmm'



# ======== Ingestion ========

def ingest_flight_data(airport_code):
    """
    Fetch flight data for a specified airport code from an API in production mode or from a local file in development mode.
    :param airport_code: The IATA code of the airport (e.g., 'MCO' for Orlando).
    """

    try: 
        import json

        if PRODUCTION_MODE:
            import requests
            from datetime import datetime, timedelta

            api_key_path = AVIATION_EDGE_API_KEY_PATH
            output_file_path = f'{INGESTION_DATA_PATH}{INGESTED_FLIGHTS_PREFIX}{airport_code}.json'  # Temporary location to store fetched data
            # Read the API key
            with open(api_key_path, 'r') as f:
                api_key = f.read().strip()

            # Calculate the date range: 15 days ago to 5 days ago
            today = datetime.utcnow()
            date_from = (today - timedelta(days=15)).strftime('%Y-%m-%d')
            date_to = (today - timedelta(days=5)).strftime('%Y-%m-%d')

            api_url = f'https://aviation-edge.com/v2/public/flightsHistory?key={api_key}&code={airport_code}&type=departure&date_from={date_from}&date_to={date_to}'

            # Fetch data from the API
            response = requests.get(api_url)
            response.raise_for_status()  # Raise an error for bad status codes

            # Parse the JSON response
            flight_data = response.json()
            print(f"Fetched {len(flight_data)} records from the API.")

        else: # Developer mode
            local_file_path = f'.aviationedge/response_departure_{airport_code}.json'
            output_file_path = f'{INGESTION_DATA_PATH}{INGESTED_FLIGHTS_PREFIX}{airport_code}.json'  # Temporary location to store fetched data

            with open(local_file_path, 'r') as f:
                flight_data = json.load(f)
            print(f"Loaded {len(flight_data)} records from the local JSON file.")
            # -------

            # Write the fetched data to a temporary output file
            with open(output_file_path, 'w') as f:
                json.dump(flight_data, f, indent=4)

            print(f"Flight data of {airport_code} saved to {output_file_path}")
        

    except Exception as e:
        print(f"Error while ingesting flight data: {e}")
        raise


def ingest_weather_data(**kwargs):
    # Placeholder function for weather data ingestion
    # TODO: Implement REST API call to fetch weather data

    try:
        import json
        import requests

        output_file_path = f'{INGESTION_DATA_PATH}{WEATHER_DATA_JSON}'

        if PRODUCTION_MODE:
            from datetime import datetime, timedelta

            # Calculate the date range: 15 days ago to 5 days ago
            today = datetime.utcnow()
            date = (today - timedelta(days=5)).strftime('%Y%m%d')
            api_url = f'https://aviationweather.gov/api/data/metar?ids=KMCO%2CKFLL&format=json&hours=216&date={date}_235959Z'
            
            # Fetch data from the API
            response = requests.get(api_url)
            response.raise_for_status()  # Raise an error for bad status codes

            # Parse the JSON response
            weather_data = response.json()
            print(f"Loaded {len(weather_data)} records from the API.")

            # Write the fetched data to a temporary output file
            with open(output_file_path, 'w') as f:
                json.dump(weather_data, f, indent=4)
        else:
            input_file_path = '.aviationweather/response.json'
            with open(input_file_path, 'r') as f:
                weather_data = json.load(f)
            print(f"Loaded {len(weather_data)} records from local file.")
            
            # Write the data to the output file (you can skip or adjust this step as needed)
            with open(output_file_path, 'w') as f:
                json.dump(weather_data, f, indent=4)

    except Exception as e:
        print(f"Error while ingesting flight data: {e}")
        raise

# ======== Staging ========
# === Clean ====
def clean_flight_data(departure_code, filter_arrival_code):
    """
    Cleans the flight data by filtering flights where the arrival airport matches the given code.
    :param input_file_path: Path to the input JSON file with raw flight data.
    :param output_file_path: Path to save the cleaned flight data.
    :param filter_arrival_code: IATA code of the arrival airport to filter by.
    """
    try:
        import json

        input_file_path = f'{INGESTION_DATA_PATH}{INGESTED_FLIGHTS_PREFIX}{departure_code}.json'
        output_file_path = f'{STAGING_DATA_PATH}{INGESTED_FLIGHTS_PREFIX}{filter_arrival_code}.json'
        
        with open(input_file_path, 'r') as input_file:
            flight_data = json.load(input_file)
        
        # Filter flights where the arrival airport matches the specified code
        cleaned_data = [
            flight for flight in flight_data
            if flight.get('arrival', {}).get('iataCode', '').upper() == filter_arrival_code.upper()
        ]
        
        # Save the cleaned flight data
        with open(output_file_path, 'w') as output_file:
            json.dump(cleaned_data, output_file, indent=4)
        
        print(f"Cleaned data saved to {output_file_path}. Total records: {len(cleaned_data)}")
    except Exception as e:
        print(f"Error while cleaning flight data: {e}")
        raise

# === Parse & Load ===

def load_weather_data():
    """
    Parse weather data from JSON and insert it into the PostgreSQL 'weather' table.
    """
    try:
        import json
        input_file_path = f'{INGESTION_DATA_PATH}{WEATHER_DATA_JSON}'

        # Load JSON data
        with open(input_file_path, 'r') as f:
            weather_data = json.load(f)

        # Establish connection to PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Iterate through the JSON data
        for record in weather_data:
            from datetime import datetime
            raw_ob = record.get("rawOb", "")
            
            # Parse rawOb
            parts = raw_ob.split()
            if len(parts) < 6:
                print(f"Skipping invalid record: {raw_ob}")
                continue

            airport_icao = parts[0]
            timestamp = record.get("receiptTime","")
            iso_timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").isoformat(timespec='milliseconds')
            wind = parts[2]
            visibility = parts[3]
            sky_condition = parts[4]
            temperature = parts[5]
            altimeter = float(parts[6][1:]) if len(parts) > 6 and parts[6].startswith('A') else None

            
            insert_query = """
                INSERT INTO weather (
                    airport_icao, timestamp, wind, visibility, sky_condition, temperature, altimeter
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (airport_icao, iso_timestamp, wind, visibility, sky_condition, temperature, altimeter))



        # Commit changes
        conn.commit()
        cursor.close()
        print("Weather data loaded successfully.")

    except Exception as e:
        print(f"Error loading weather data: {e}")
        raise

def load_flights_from_file(file_path: str) -> None:
    """
    Parse flight data from a specified JSON file and insert it into the PostgreSQL 'flights' table.
    :param file_path: Path to the JSON file containing flight data.
    """
    try:
        import json

        # Load the JSON file
        with open(file_path, 'r') as f:
            flights_data = json.load(f)
        print(f"Loaded {len(flights_data)} flight records from {file_path}.")

        # Connect to PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Prepare the SQL insert query
        insert_query = """
            INSERT INTO flights (
                scheduled_time, departure_icao, arrival_icao, delay_in_minutes, flight_status, airline, flight_number
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        # Iterate through the flight records and insert them
        for record in flights_data:
            departure = record.get('departure', {})
            arrival = record.get('arrival', {})
            airline = record.get('airline', {})
            flight = record.get('flight', {})

            cursor.execute(insert_query, (
                departure.get('scheduledTime'),
                departure.get('icaoCode'),
                arrival.get('icaoCode'),
                departure.get('delay', 0),
                record.get('status', 'unknown'),
                airline.get('name', 'unknown'),
                flight.get('number', 'unknown')
            ))

        # Commit the changes and close the connection
        conn.commit()
        cursor.close()
        print(f"Flight data from {file_path} loaded successfully into PostgreSQL.")

    except Exception as e:
        print(f"Error loading flight data from {file_path}: {e}")
        raise

def clean_weather_data():
    """
    Remove rows where:
    - Wind doesn't end with "KT"
    - Visibility doesn't end with "SM"
    - Sky condition starts with a number
    - Temperature doesn't start with a number
    """
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        # Query to select rows where we will apply the checks
        cursor.execute("SELECT id, wind, visibility, sky_condition, temperature FROM weather")
        rows = cursor.fetchall()

        # List to collect ids of rows that need to be deleted
        rows_to_delete = []

        for row in rows:
            weather_id, wind, visibility, sky_condition, temperature = row

            # Check each condition and if any are not met, mark for deletion
            if not wind.endswith("KT"):
                rows_to_delete.append(weather_id)
            elif not visibility.endswith("SM"):
                rows_to_delete.append(weather_id)
            elif sky_condition[0].isdigit():  # Sky condition starts with a number
                rows_to_delete.append(weather_id)
            elif not temperature[0].isdigit():  # Temperature does not start with a number
                rows_to_delete.append(weather_id)

        # Delete the rows that do not meet the conditions
        if rows_to_delete:
            cursor.execute(
                """
                DELETE FROM weather
                WHERE id IN %s
                """, 
                (tuple(rows_to_delete),)
            )
            conn.commit()

        print(f"Cleaned up {len(rows_to_delete)} rows from the weather data.")

    except Exception as e:
        print(f"Error cleaning weather data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def update_formatted_timestamps():
    """
    Update the `scheduled_time` column in the `flights` table by replacing
    lowercase 't' with uppercase 'T' in the timestamp format.
    """
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        # Update the scheduled_time column
        cursor.execute(
            """
            UPDATE flights
            SET scheduled_time = REPLACE(scheduled_time, 't', 'T')
            WHERE scheduled_time LIKE '%t%';
            """
        )
        
        # Commit the changes
        conn.commit()
        print(f"Updated timestamps in the `flights` table.")

    except Exception as e:
        print(f"Error updating timestamps: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def associate_weather_with_flights():
    """
    Associate flights with the most recent weather data at the departure and arrival airports.
    """
    try:

        # Establish connection to PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Query to fetch all flights
        cursor.execute("SELECT id, departure_icao, arrival_icao, scheduled_time FROM flights")
        flights = cursor.fetchall()

        for flight in flights:
            flight_id, departure_icao, arrival_icao, scheduled_time = flight

            # Find the most recent weather data for the departure airport
            departure_weather_query = """
                SELECT id FROM weather
                WHERE airport_icao = UPPER(%s) AND timestamp <= %s
                ORDER BY timestamp DESC
                LIMIT 1
            """
            cursor.execute(departure_weather_query, (departure_icao, scheduled_time))
            departure_weather = cursor.fetchone()
            departure_weather_id = departure_weather[0] if departure_weather else None

            # Find the most recent weather data for the arrival airport
            arrival_weather_query = """
                SELECT id FROM weather
                WHERE airport_icao = UPPER(%s) AND timestamp <= %s
                ORDER BY timestamp DESC
                LIMIT 1
            """
            cursor.execute(arrival_weather_query, (arrival_icao, scheduled_time))
            arrival_weather = cursor.fetchone()
            arrival_weather_id = arrival_weather[0] if arrival_weather else None

            # Update the flight record with weather data IDs
            update_query = """
                UPDATE flights
                SET departure_weather_id = %s, destination_weather_id = %s
                WHERE id = %s
            """
            cursor.execute(update_query, (departure_weather_id, arrival_weather_id, flight_id))

        # Commit changes
        conn.commit()
        cursor.close()
        print("Flights successfully associated with weather data.")

    except Exception as e:
        print(f"Error associating weather with flights: {e}")
        raise

def clean_flights_table():
    """
    Remove rows from the `flights` table where either `departure_weather_id`
    or `destination_weather_id` is NULL.
    """
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        # Execute the delete statement
        cursor.execute(
            """
            DELETE FROM flights
            WHERE departure_weather_id IS NULL
               OR destination_weather_id IS NULL;
            """
        )
        
        # Commit the changes
        conn.commit()
        print("Rows with missing weather IDs removed successfully.")

    except Exception as e:
        print(f"Error removing rows: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def export_flights_with_weather(csv_file_path):
    """
    Export flight data with complete weather details for both departure and destination airports.
    
    Args:
        csv_file_path (str): Path to save the generated CSV file.
    """
    import csv

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        # SQL query to join flights and weather data for departure and destination
        query = """
        SELECT 
            f.id, 
            f.scheduled_time, 
            f.departure_icao, 
            f.arrival_icao, 
            f.delay_in_minutes, 
            f.flight_status, 
            f.airline, 
            f.flight_number,
            w1.wind AS departure_wind,
            w1.visibility AS departure_visibility,
            w1.sky_condition AS departure_sky_condition,
            w1.temperature AS departure_temperature,
            w1.altimeter AS departure_altimeter,
            w2.wind AS destination_wind,
            w2.visibility AS destination_visibility,
            w2.sky_condition AS destination_sky_condition,
            w2.temperature AS destination_temperature,
            w2.altimeter AS destination_altimeter
        FROM flights f
        LEFT JOIN weather w1 ON f.departure_weather_id = w1.id
        LEFT JOIN weather w2 ON f.destination_weather_id = w2.id
        """

        # Execute the query
        cursor.execute(query)
        rows = cursor.fetchall()

        # Define the column headers
        headers = [
            "id", "scheduled_time", "departure_icao", "arrival_icao", "delay_in_minutes",
            "flight_status", "airline", "flight_number", 
            "departure_wind", "departure_visibility", "departure_sky_condition",
            "departure_temperature", "departure_altimeter",
            "destination_wind", "destination_visibility", "destination_sky_condition",
            "destination_temperature", "destination_altimeter"
        ]

        # Write to a CSV file
        with open(csv_file_path, mode="w", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(headers)  # Write headers
            writer.writerows(rows)  # Write data rows

        print(f"CSV file exported successfully to {csv_file_path}")

    except Exception as e:
        print(f"Error exporting data: {e}")
    finally:
        cursor.close()
        conn.close()

# =================== Operators Definition ===================
with TaskGroup("ingestion_pipeline",dag=global_dag) as ingestion_pipeline:
    start = DummyOperator(
        task_id='start',
        dag=global_dag,
    )

    ingest_flights_mco = PythonOperator(
        task_id='ingest_flights_mco_data',
        dag=global_dag,
        python_callable=lambda: ingest_flight_data('MCO'),
    )

    ingest_flights_fll = PythonOperator(
        task_id='ingest_flights_fll_data',
        python_callable=lambda: ingest_flight_data('FLL'),
    )

    ingest_weather_task = PythonOperator(
        task_id='ingest_weather_data',
        python_callable=ingest_weather_data,
    )

    end = DummyOperator(
        task_id='end',
        dag=global_dag,
        trigger_rule='all_success'
    )

    start >> [ingest_flights_mco, ingest_flights_fll, ingest_weather_task] >> end


with TaskGroup("staging_pipeline",dag=global_dag) as staging_pipeline:
    start = DummyOperator(
        task_id='start',
        dag=global_dag,
    )

    # === CLEAN ===
    clean_flights_mco = PythonOperator(
        task_id='clean_flights_mco_data',
        dag=global_dag,
        python_callable=lambda: clean_flight_data('MCO', 'FLL'),
    )

    clean_flights_fll = PythonOperator(
        task_id='clean_flights_fll_data',
        python_callable=lambda: clean_flight_data('FLL', 'MCO'),
    )

    # === Postgres ===
    create_flights_table = PostgresOperator(
        task_id='create_flights_table',
        dag=global_dag,
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        sql='sql/create_flights_table.sql',
        trigger_rule='none_failed',
        autocommit=True,
    )

    create_weather_table = PostgresOperator(
        task_id='create_weather_table',
        dag=global_dag,
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        sql='sql/create_weather_table.sql',
        trigger_rule='none_failed',
        autocommit=True,
    )

    load_weather_data_postgres = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_weather_data,
    )
    
    load_flights_fll = PythonOperator(
        task_id='load_flights_fll',
        python_callable=lambda: load_flights_from_file('data/staging/flight_data_FLL.json'),
        dag=global_dag,
    )

    load_flights_mco = PythonOperator(
        task_id='load_flights_mco',
        python_callable=lambda: load_flights_from_file('data/staging/flight_data_MCO.json'),
        dag=global_dag,
    )

    clean_weather_data_postgres = PythonOperator(
        task_id='clean_weather_data',
        python_callable=clean_weather_data,
        dag=global_dag,
    )

    format_weather_timestamp = PythonOperator(
        task_id='format_weather_timestamp',
        python_callable=update_formatted_timestamps,
        dag=global_dag,
    )

    join_weather_data = PythonOperator(
        task_id='join_weather_data',
        python_callable=associate_weather_with_flights,
        dag=global_dag,
    )

    clean_flights = PythonOperator(
        task_id='clean_flights',
        python_callable=clean_flights_table,
        dag=global_dag,
    )
    

    end = DummyOperator(
        task_id='end',
        dag=global_dag,
        trigger_rule='all_success'
    )

    start >> [clean_flights_mco, clean_flights_fll, create_weather_table]
    [clean_flights_mco, clean_flights_fll] >> create_flights_table
    create_flights_table >> [load_flights_fll, load_flights_mco]
    create_weather_table >> load_weather_data_postgres
    [load_flights_fll, load_flights_mco, load_weather_data_postgres] 
    load_weather_data_postgres >> clean_weather_data_postgres >> format_weather_timestamp >> join_weather_data
    [load_flights_fll, load_flights_mco] >> join_weather_data
    join_weather_data >> clean_flights >> end

with TaskGroup("production_pipeline",dag=global_dag) as production_pipeline:
    start = DummyOperator(
        task_id='start',
        dag=global_dag,
    )

    generate_csv = PythonOperator(
        task_id='generate_csv',
        python_callable= lambda: export_flights_with_weather("data/production/flights.csv"),
        dag=global_dag,
    )

    end = DummyOperator(
        task_id='end',
        dag=global_dag,
        trigger_rule='all_success'
    )

    start >> generate_csv >> end



start = DummyOperator(
    task_id='start',
    dag=global_dag,
    trigger_rule='all_success'
)

end = DummyOperator(
    task_id='end',
    dag=global_dag,
    trigger_rule='all_success'
)

start >> ingestion_pipeline >> staging_pipeline >> production_pipeline >> end


# =========================================================
