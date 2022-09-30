import airflow
from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from datetime import datetime, timedelta

import os


# Global variables that are set using environment varaiables
DBT_ROOT_PATH = os.getenv('DBT_ROOT_PATH')


default_args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    'email': ['phi@honestbank.com'],
}

# The DAG definition
dag = DAG(
    dag_id='dbt_refine',
    default_args=default_args,
    schedule="0 1 * * *",
)

run_refine_job = BashOperator(
    task_id='refine',
    bash_command='dbt run --project-dir {} --profiles-dir {}'.format(os.path.join(DBT_ROOT_PATH, 'dbt/refine'),
                                                                     os.path.join(DBT_ROOT_PATH, 'dbt/refine')),
    dag=dag)

run_refine_job

