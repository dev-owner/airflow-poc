from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

from airflow_poc.module.util import get_dag_and_tag_id

DAG_ID, WORKFLOW_ID = get_dag_and_tag_id(__file__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        description='test start_time=now(), interval=@daily dags',
        schedule_interval="@daily",
        start_date=datetime.now(),
        tags=[WORKFLOW_ID],
) as dag:
    start = DummyOperator(task_id="start")

    hourly = BashOperator(
        task_id='print_hourly_date',
        bash_command='echo prev_ds :{{ prev_ds }}; echo ds :{{ ds }}; echo next_ds :{{ next_ds }};',
    )
    end = DummyOperator(task_id="end")

    start >> hourly >> end
