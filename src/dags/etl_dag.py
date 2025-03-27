'''
This file contains the DAG definition for the ETL process.
'''

import sys
from pathlib import Path
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# Adjust Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# Correct import from sibling module etl.py
from etl import extract, transform, load


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email': ['frank.daza2@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    'knarf_etl_dag',
    default_args=default_args,
    description='ETL final project - UAO',
    schedule_interval='@daily',
) as dag:
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
        provide_context=True,
        )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load,
        provide_context=True,
        )

    extract_task >> transform_task >> load_task
