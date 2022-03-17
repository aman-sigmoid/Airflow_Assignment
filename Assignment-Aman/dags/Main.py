from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from create_csv import csv_creation
from create_table import table_creation
from load_table import populate_table

default_args = {
    "owner": "Aman",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 15),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(dag = DAG("weather", default_args=default_args, schedule_interval="0 6 * * *"))as dag:
    t1 = PythonOperator(task_id='csv_creation_t', python_callable=csv_creation)

    t2 = PythonOperator(task_id="table_creation_t", python_callable=table_creation)

    t3 = PythonOperator(task_id="populate_table_t", python_callable=populate_table)

    t1 >> t2 >> t3
