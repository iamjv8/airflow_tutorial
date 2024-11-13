from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"owner": "Jayesh", "retries": 3, "retry-delay": timedelta(minutes=5)}

with DAG(
    default_args=default_args,
    dag_id="dag_with_cron_expression_v2",
    description="This is CRON DAG",
    start_date=datetime(2024, 11, 1),
    schedule_interval="0 0 * * *",
) as dag:
    task1 = BashOperator(
        task_id="Task1", bash_command="echo Print data from crone expression DAG"
    )
    task1
