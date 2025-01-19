from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from scripts.collecthist import collect
from scripts.clean import clean_data
from scripts.db import save_data_to_db
from scripts.transform import transform_and_save_to_db
from datetime import datetime, timedelta, timezone

def is_not_current_month(execution_date, **kwargs):

    execution_date = datetime.fromisoformat(execution_date)
    today = datetime.now()
    # Check if execution_date is in the current year and month
    return not (execution_date.year == today.year and execution_date.month == today.month)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Pipeline to fetch, process, and save weather data',
    schedule='@monthly',
    start_date=datetime(2024, 10, 1),
    catchup=True,
) as dag:

    check_month = ShortCircuitOperator(
        task_id='check_month',
        python_callable=is_not_current_month,
        op_kwargs={'execution_date': '{{ execution_date }}'},
        provide_context=True,
    )

    collect_ = PythonOperator(
        task_id='collect_',
        python_callable=collect,
        op_kwargs={
            'start_date': '{{ ds }}',
            'file_path': 'data/raw/weather_{{ ds }}.csv'
        },
    )

    #process and save only last month data
    clean_ = PythonOperator(
        task_id='clean_',
        python_callable=clean_data,
        op_kwargs={
            'input_file': 'data/raw/weather_{{ ds }}.csv',
            'output_file': 'data/processed/clean_weather_data.csv'
        }
    )

    save_to_db = PythonOperator(
        task_id='save_to_db',
        python_callable=save_data_to_db,
        op_kwargs={
            'csv_file': 'data/processed/clean_weather_data.csv',  # Path to cleaned data
        }
    )

    transform_ = PythonOperator(
        task_id='transform_',
        python_callable=transform_and_save_to_db,
    )

    check_month >> collect_ >> clean_ >> save_to_db >> transform_





