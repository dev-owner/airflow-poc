from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor

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
        schedule_interval="25 * * * *",
        start_date=START_DATE,
        tags=[WORKFLOW_ID],
        user_defined_macros=default_args
) as dag:
    sensor = ExternalTaskSensor(
        task_id="wait_for_upstream_dag1_t2",
        external_dag_id="triggering_between_dags_hourly1",
        external_task_id="t2",
        start_date=START_DATE,
        # execution_delta=timedelta(minutes=25)
        execution_date_fn=lambda dt: dt + timedelta(minutes=-25),  # #1은 정각마다 수행, #2는 25분마다 수행
        mode="reschedule",
        timeout=10
    )

    start = DummyOperator(task_id="start")

    t1 = BashOperator(
        task_id='t1',
        bash_command=(
            "echo prev_ds :{{ prev_ds }}, "
            "ds :{{ ds }}, "
            "next_ds :{{ next_ds }}, "
            "next_execution_date : {{ next_execution_date }}"
        )
    )

    end = DummyOperator(task_id="end")

    start >> sensor >> t1 >> end
