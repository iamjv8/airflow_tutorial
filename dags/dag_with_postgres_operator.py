from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {"owner": "Jayesh", "retries": 5, "retry_delay": timedelta(minutes=3)}

with DAG(
    dag_id="postgres_opearator_v2",
    default_args=default_args,
    start_date=datetime(2024, 11, 13),
    schedule_interval="0 0 * * *",
) as dag:
    task1 = PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id="postgres_localhost",
        sql="""
            create table if not exists dag_runs(
                dt date,
                dag_id varchar,
                primary key(dt, dag_id)
            )
        """,
    )

    task2 = PostgresOperator(
        task_id="insert_data_in_table",
        postgres_conn_id="postgres_localhost",
        sql="""
            INSERT INTO dag_runs(dt,dag_id) VALUES ('{{ds}}', '{{ dag.dag_id}}')  
        """,
    )
    task1 >> task2
