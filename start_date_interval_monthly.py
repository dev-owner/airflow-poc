import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from sqlalchemy_utils.types.enriched_datetime.pendulum_datetime import pendulum

DAG_ID = os.path.basename(__file__).replace(".py", "")
KST = pendulum.timezone("Asia/Seoul")
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

DAYS = 35

with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        description='test hourly dags',
        schedule_interval="@monthly",
        start_date=days_ago(DAYS),
        tags=['test'],
) as dag:
    start = DummyOperator(task_id="start")

    hourly = BashOperator(
        task_id='print_hourly_date',
        bash_command='date',
    )
    end = DummyOperator(task_id="end")

    start >> hourly >> end
