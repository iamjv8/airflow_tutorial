from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {"owner": "Jayesh", "retries": 5, "retry_delay": timedelta(minutes=2)}
with DAG(
    dag_id="first_dag",
    default_args=default_args,
    description="This is first DAG",
    start_date=datetime(2024, 11, 10, 2),
    schedule_interval="@daily",
) as dag:
    task1 = BashOperator(
        task_id="first_task", bash_command="echo Hello world in Airflow by Jayesh "
    )

    task2 = BashOperator(
        task_id="second_task", bash_command="echo This is a second task"
    )

    task3 = BashOperator(
        task_id="Third_task", bash_command="echo Third Task is executed"
    )

    # task1 >> task2 >> task3
    task1 >> [task2, task3]
