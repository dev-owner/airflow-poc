from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

from airflow_poc.module.util import get_dag_and_tag_id

DAG_ID, WORKFLOW_ID = get_dag_and_tag_id(__file__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

DAYS = 14

with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        description='test 14 days catchup daily dags',
        schedule_interval="@daily",
        start_date=days_ago(DAYS),
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
