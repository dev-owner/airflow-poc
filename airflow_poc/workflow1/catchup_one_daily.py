from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

from airflow_poc.module.util import get_dag_and_tag_id

DAG_ID, WORKFLOW_ID = get_dag_and_tag_id(__file__)

DAYS = 1

default_args = {
    'owner': 'airflow',
    'depends_on_past': False
}

with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        schedule_interval="@daily",
        start_date=datetime(2021, 3, 29, 0, 0, 0),
        tags=[WORKFLOW_ID],
) as dag:
    start = DummyOperator(task_id="start")

    daily = BashOperator(
        task_id='print_daily_date',
        bash_command=(
            "echo prev_ds :{{ prev_ds }}, "
            "ds :{{ ds }}, "
            "next_ds :{{ next_ds }}, "
            "next_execution_date : {{ next_execution_date }}"
        )
    )
    end = DummyOperator(task_id="end")

    start >> daily >> end
