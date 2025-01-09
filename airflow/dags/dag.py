from datetime import datetime, timedelta
import airflow
import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.utils.task_group import TaskGroup

# =================== Environment ===================

PRODUCTION_MODE = False     # PLEASE CHANGE WITH CAUTION, CONSUMES API KEY TOKENS

AVIATION_EDGE_API_KEY_PATH = '../.aviationedge/api.txt'

INGESTED_FLIGHTS_PREFIX = 'flight_data_'


# =========================================================


# =================== Global Definitions ===================

INGESTION_DATA_PATH = 'data/ingestion/'
STAGING_DATA_PATH = 'data/staging'


AVIATION_EDGE_API_KEY_PATH = '../.aviationedge/api.txt'

INGESTED_FLIGHTS_PREFIX = 'flight_data_'


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
    catchup=False,  # Prevent backfilling of DAG runs
)

# =========================================================


# =================== Python Functions ===================

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

            api_key_path = AVIATION_EDGE_API_KEY_PATH
            output_file_path = f'{INGESTION_DATA_PATH}{INGESTED_FLIGHTS_PREFIX}{airport_code}.json'  # Temporary location to store fetched data
            # Read the API key
            with open(api_key_path, 'r') as f:
                api_key = f.read().strip()

            # TO CHANGE!!!
            api_url = f'https://aviation-edge.com/v2/public/flightsHistory?key={api_key}&code={airport_code}&type=departure&date_from=2024-12-29&date_to=2025-01-04'

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

        output_file_path = f'{INGESTION_DATA_PATH}weather_data.json'  # Temporary location to store fetched data

        if PRODUCTION_MODE:
            # TO CHANGE!!!
            api_url = 'https://aviationweather.gov/api/data/metar?ids=KMCO%2CKFLL&format=json&hours=168&date=20250104_235959Z'
        else:
            api_url = 'https://aviationweather.gov/api/data/metar?ids=KMCO%2CKFLL&format=json&hours=168&date=20250104_235959Z'

        # Fetch data from the API
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an error for bad status codes

        # Parse the JSON response
        weather_data = response.json()
        print(f"Loaded {len(weather_data)} records from the API.")

        # Write the fetched data to a temporary output file
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


# =========================================================


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

    clean_flights_mco = PythonOperator(
        task_id='clean_flights_mco_data',
        dag=global_dag,
        python_callable=lambda: clean_flight_data('MCO', 'FLL'),
    )

    clean_flights_fll = PythonOperator(
        task_id='clean_flights_fll_data',
        python_callable=lambda: clean_flight_data('FLL', 'MCO'),
    )

    create_flights_table = PostgresOperator(
        task_id='create_flights_table',
        dag=global_dag,
        postgres_conn_id='postgres_default',
        sql='sql/create_flights_table.sql',
        trigger_rule='none_failed',
        autocommit=True,
    )

    end = DummyOperator(
        task_id='end',
        dag=global_dag,
        trigger_rule='all_success'
    )

    start >> [clean_flights_mco, clean_flights_fll]
    [clean_flights_mco, clean_flights_fll] >> create_flights_table  
    create_flights_table >> end

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

start >> ingestion_pipeline >> staging_pipeline >> end


# =========================================================
