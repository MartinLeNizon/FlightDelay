from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

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
    # TODO: Implement REST API call to fetch flight data

    

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
