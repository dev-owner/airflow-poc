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

DAYS = 1

with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        description='test 1days catchup hourly dags',
        schedule_interval="@hourly",
        start_date=days_ago(DAYS),
        tags=[WORKFLOW_ID],
) as dag:
    start = DummyOperator(task_id="start")

    hourly = BashOperator(
        task_id='print_hourly_date',
        bash_command=(
            "echo ts :{{ ts_nodash }}, "
            "next_execution_date : {{ next_execution_date }}"
        )
    )
    end = DummyOperator(task_id="end")

    start >> hourly >> end
