from datetime import timedelta
from pprint import pprint

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from airflow_poc.module.util import get_dag_and_tag_id

DAG_ID, WORKFLOW_ID = get_dag_and_tag_id(__file__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "retries": 3,
    "retry_delay": timedelta(seconds=5),
    "offset": timedelta(hours=2)  # -2h
}

DAYS = 1

with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        schedule_interval="@hourly",
        start_date=days_ago(DAYS),
        tags=[WORKFLOW_ID],
        user_defined_macros=default_args
) as dag:

    start = DummyOperator(task_id="start")

    def print_ds(**context):
        pprint(context)
        execution_date = context['execution_date']
        offset = context['templates_dict']['offset']
        year, month, day, hour, *_ = (execution_date - offset).timetuple()
        print(f"day: {day:0>2}, hh: {hour:0>2}")


    hourly = PythonOperator(
        task_id="print_ds",
        python_callable=print_ds,
        templates_dict=default_args
    )
    end = DummyOperator(task_id="end")

    start >> hourly >> end
