import airflow
from airflow.operators.bash_operator import BashOperator
from airflow import DAG
import os


# Global variables that are set using environment varaiables
DBT_ROOT_PATH = os.getenv('DBT_ROOT_PATH')


default_args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(1)
}

# The DAG definition
dag = DAG(
    dag_id='ge_tutorials_dag_no_ge',
    default_args=default_args,
    schedule_interval=None,
)

task_transform_data_in_db = BashOperator(
    task_id='task_transform_data_in_db',
    bash_command='dbt run --project-dir {} ---profiles-dir {}'.format(os.path.join(DBT_ROOT_PATH, 'dbt'), os.path.join(DBT_ROOT_PATH, 'dbt')),
    dag=dag)


# DAG dependencies
task_transform_data_in_db
