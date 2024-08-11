from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from  extract import extract 
from transform import transform
from load import load
from analyse import analyse

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'air_pollution_etl',
    default_args=default_args,
    description='ETL pipeline for air pollution data',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)


extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform,
    dag=dag,
)

analyse = PythonOperator(
    task_id='analyse_data',
    python_callable=analyse,
    dag=dag,
)

load = PythonOperator(
    task_id='load_data',
    python_callable=load,
    dag=dag,
)

extract >> transform >> analyse >> load 
