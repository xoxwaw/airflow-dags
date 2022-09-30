import airflow
from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from datetime import datetime, timedelta

import os


DBT_ROOT_PATH = os.getenv('DBT_ROOT_PATH')  # Set from ArgoCD

default_args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    'email': ['phi@honestbank.com'],
}

# The DAG definition
dag = DAG(
    dag_id='dbt_aggregate',
    schedule="0 5 * * *",
    default_args=default_args,
)

run_aggregate_job = BashOperator(
    task_id='aggregate',
    bash_command='dbt run --project-dir {} --profiles-dir {}'.format(os.path.join(DBT_ROOT_PATH, 'dbt/aggregate'),
                                                                     os.path.join(DBT_ROOT_PATH, 'dbt/aggregate')),
    dag=dag)

run_aggregate_job
