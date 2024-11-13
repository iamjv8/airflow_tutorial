from airflow.decorators import dag, task

from datetime import timedelta, datetime

default_args = {"owner": "Jayesh", "retries": 5, "retry_delay": timedelta(minutes=2)}


@dag(
    dag_id="dag_with_airflow_task_api_v2",
    default_args=default_args,
    description="This is first DAG",
    start_date=datetime(2024, 11, 10, 2),
    schedule_interval="@daily",
)
def hello_world_etl():

    @task()
    def get_name():
        return {"first_name": "Jayesh", "last_name": "Vekariya"}

    @task()
    def get_age():
        return 30

    @task()
    def greet(first_name, last_name, age):
        print(f"My name is {first_name} {last_name}, and I am {age} years old.")

    name_dict = get_name()
    print(name_dict)
    age = get_age()
    greet(first_name=name_dict["first_name"], last_name=name_dict["last_name"], age=age)


greet_dag = hello_world_etl()
