from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


def greet(ti):
    first_name = ti.xcom_pull(task_ids="get_name", key="first_name")
    last_name = ti.xcom_pull(task_ids="get_name", key="last_name")
    print(f"Hello {first_name} {last_name} from Python operator DAG...!")


def get_name(ti):
    ti.xcom_push(key="first_name", value="Jayesh")
    ti.xcom_push(key="last_name", value="Vekariya")


default_args = {"owner": "Jayesh", "retries": 5, "retry_delay": timedelta(minutes=2)}
with DAG(
    dag_id="python_operator_dag",
    default_args=default_args,
    description="This is first DAG",
    start_date=datetime(2024, 11, 10, 2),
    schedule_interval="@daily",
) as dag:
    task1 = PythonOperator(python_callable=greet, task_id="greet")

    task2 = PythonOperator(python_callable=get_name, task_id="get_name")

    task2 >> task1
