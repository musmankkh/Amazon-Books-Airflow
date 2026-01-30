from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import timedelta
import pendulum

from plugins.books_etl import (
    read_csv_files,
    transform_data,
    insert_data,
    POSTGRES_CONN_ID,
)

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "retries": 0,  # 🔥 retries now work correctly
}

with DAG(
    dag_id="load_books_from_csv",
    start_date=pendulum.datetime(2025, 1, 29, tz="UTC"),
    schedule="0 9 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["etl", "books"],
    template_searchpath=[
        "/opt/airflow/sql", 
    ],
) as dag:

    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id=POSTGRES_CONN_ID,
        sql="create_tables.sql",
    )

    extract = PythonOperator(
        task_id="read_csv",
        python_callable=read_csv_files,
        retries=0,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        retries=0,
    )

    load = PythonOperator(
        task_id="insert_data",
        python_callable=insert_data,
        retries=1,
    )

    create_tables >> extract >> transform >> load
