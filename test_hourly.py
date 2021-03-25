"""
Description : 14 days ago 부터 Trigger한 시점까지 interval 단위로 실행
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from sqlalchemy_utils.types.enriched_datetime.pendulum_datetime import pendulum

DAG_ID = os.path.basename(__file__).replace(".py", "")
KST = pendulum.timezone("Asia/Seoul")
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}
with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        description='test hourly dags',
        schedule_interval="@hourly",
        start_date=datetime(2021, 3, 17, tzinfo=KST),
        tags=['test'],
) as dag:
    start = DummyOperator(task_id="start")

    hourly = BashOperator(
        task_id='print_hourly_date',
        bash_command='date',
    )
    end = DummyOperator(task_id="end")

    start >> hourly >> end
