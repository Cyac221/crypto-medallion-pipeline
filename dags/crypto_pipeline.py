#Orchestator
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta

default_args = {
    'owner':'Carlos_yepes',
    'retries':2,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    dag_id = 'crypto_medallion_pipeline',
    default_args = default_args,
    start_date = datetime(2024,1,1),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    def task_extract(**context):
        from ingestion.coingecko_extractor import extract_and_save
        output_path = extract_and_save('/opt/airflow/data/bronze')
        context['ti'].xcom_push(key='bronze_path',value = output_path)
        return output_path
    
    extract_python = PythonOperator(
        task_id = 'extract_from_api',
        python_callable = task_extract,
    )

    def task_validate_bronze(**context):
        import json
        from pathlib import Path

        bronze_path = context['ti'].xcom_pull(
            key = 'bronze_path',
            task_ids = 'extract_from_api'
        )

        if not Path(bronze_path).exists():
            raise FileNotFoundError(f'Bronze file not found {bronze_path}')
        
        with open(bronze_path,'r') as f:
            data = json.load(f)

        if len(data) == 0:
            raise ValueError('Bronze file is empty')
        
        print(f'Valitadios passeed - {len(data)} records found')
    
    validate_bronze = PythonOperator(
        task_id = 'validate_bronze',
        python_callable = task_validate_bronze
    )

    extract_python >> validate_bronze
