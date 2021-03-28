from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

from airflow_poc.module.util import get_dag_and_tag_id

DAG_ID, WORKFLOW_ID = get_dag_and_tag_id(__file__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "retries": 3,
    "retry_delay": timedelta(seconds=5)
}

DAYS = 1
START_DATE = datetime(2021, 3, 28, 0, 0, 0)

with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        description="test hourly dags",
        schedule_interval="@hourly",
        start_date=START_DATE,
        tags=[WORKFLOW_ID],
        user_defined_macros=default_args
) as dag:
    start = DummyOperator(task_id="start")
    command = (
        "echo prev_ds :{{ prev_ds }}, "
        "ds :{{ ds }}, "
        "next_ds :{{ next_ds }}, "
        "next_execution_date : {{ next_execution_date }}"
    )

    t1 = BashOperator(
        task_id='t1',
        bash_command=command
    )

    t2 = BashOperator(
        task_id='t2',
        bash_command=command
    )

    t3 = BashOperator(
        task_id='t3',
        bash_command=command
    )

    end = DummyOperator(task_id="end")

    start >> t1 >> t2 >> t3 >> end
