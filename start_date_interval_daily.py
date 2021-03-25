import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace(".py", "")
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

DAYS = 14

with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        description='test hourly dags',
        schedule_interval="@daily",
        start_date=days_ago(DAYS),
        tags=['test'],
) as dag:
    start = DummyOperator(task_id="start")

    hourly = BashOperator(
        task_id='print_hourly_date',
        bash_command='echo prev_ds :{{ prev_ds }}; echo ds :{{ ds }}; echo next_ds :{{ next_ds }};',
    )
    end = DummyOperator(task_id="end")

    start >> hourly >> end
