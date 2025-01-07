from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

import os

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
    start_date=datetime(2025, 1, 1),
    default_args=default_args,
    catchup=False,  # Prevent backfilling of DAG runs
)

# =========================================================


# =================== Python Functions ===================

# Define Python functions for data ingestion (placeholders)
def ingest_flight_data(**kwargs):
    # Placeholder function for flight data ingestion

    try: 
        import json
        ''' Production mode
        import requests

        api_key_path = '../.aviationedge/api.txt'
        output_file_path = '/tmp/flight_data.json'  # Temporary location to store fetched data
        # Read the API key
        with open(api_key_path, 'r') as f:
            api_key = f.read().strip()

        # Define the API URL
        api_url = f'https://aviation-edge.com/v2/public/flightsHistory?key={api_key}&code=LYS&type=departure&date_from=2025-01-01&date_to=2025-01-02'

        # Fetch data from the API
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an error for bad status codes

        # Parse the JSON response
        flight_data = response.json()
        print(f"Fetched {len(flight_data)} records from the API.")

        # -------- '''

        # Developer mode
        local_file_path = '.aviationedge/example_output.json'
        output_file_path = f'{INGESTION_DATA_PATH}flight_data.json'  # Temporary location to store fetched data

        with open(local_file_path, 'r') as f:
            flight_data = json.load(f)
        print(f"Loaded {len(flight_data)} records from the local JSON file.")
        # -------

        # Write the fetched data to a temporary output file
        with open(output_file_path, 'w') as f:
            json.dump(flight_data, f, indent=4)

        print(f"Flight data saved to {output_file_path}")

    except Exception as e:
        print(f"Error while ingesting flight data: {e}")
        raise

    pass

def ingest_weather_data(**kwargs):
        # Placeholder function for weather data ingestion
        # TODO: Implement REST API call to fetch weather data
        pass

# =========================================================


# =================== Operators Definition ===================
with TaskGroup("ingestion_pipeline","data ingestion step",dag=global_dag) as ingestion_pipeline:
    start = DummyOperator(
        task_id='start',
        dag=global_dag,
    )

    ingest_flight_task = PythonOperator(
        task_id='ingest_flight_data',
        python_callable=ingest_flight_data,
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

    start >> [ingest_flight_task, ingest_weather_task] >> end


with TaskGroup("staging_pipeline","data staging step",dag=global_dag) as staging_pipeline:
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
