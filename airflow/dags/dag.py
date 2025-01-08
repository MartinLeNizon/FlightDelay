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


# =========================================================


# =================== Global Definitions ===================

INGESTION_DATA_PATH = 'data/ingestion/'


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

def ingest_flight_data(airport_code):
    """
    Fetch flight data for a specified airport code from an API in production mode or from a local file in development mode.
    :param airport_code: The IATA code of the airport (e.g., 'MCO' for Orlando).
    """

    try: 
        import json

        if PRODUCTION_MODE:
            import requests

            api_key_path = '../.aviationedge/api.txt'
            output_file_path = f'{INGESTION_DATA_PATH}flight_data_{airport_code}.json'  # Temporary location to store fetched data
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
            output_file_path = f'{INGESTION_DATA_PATH}flight_data_{airport_code}.json'  # Temporary location to store fetched data

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

# =========================================================


# =================== Operators Definition ===================
with TaskGroup("ingestion_pipeline",dag=global_dag) as ingestion_pipeline:
    start = DummyOperator(
        task_id='start',
        dag=global_dag,
    )

    with TaskGroup("ingestion_flights_data",dag=global_dag) as ingest_flight_task:

        ingest_flights_mco = PythonOperator(
            task_id='ingest_flights_mco_data',
            dag=global_dag,
            python_callable=lambda: ingest_flight_data('MCO'),
        )

        ingest_flights_fll = PythonOperator(
            task_id='ingest_flights_fll_data',
            python_callable=lambda: ingest_flight_data('FLL'),
        )

        ingest_flights_mco >> ingest_flights_fll

    ingest_weather_task = PythonOperator(
        task_id='ingest_weather_data',
        python_callable=ingest_weather_data,
    )

    end = DummyOperator(
        task_id='end',
        dag=global_dag,
        trigger_rule='all_success'
    )

    start >> [ingest_flight_task, ingest_weather_task] >> end


with TaskGroup("staging_pipeline",dag=global_dag) as staging_pipeline:
    start = DummyOperator(
        task_id='start',
        dag=global_dag,
    )

    end = DummyOperator(
        task_id='staging_end',
        dag=global_dag,
        trigger_rule='all_success'
    )

    start >> end

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
