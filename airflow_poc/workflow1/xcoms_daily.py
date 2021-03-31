# Author : 1111587
# Date : 2021.03.27
# Purpose : XComs 기능 테스트를 위해 daily DAG 생성
# Wiki : ...

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
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
        schedule_interval="@daily",
        start_date=days_ago(DAYS),
        tags=[WORKFLOW_ID],
) as dag:
    lst = [1, 2, 3]

    start = DummyOperator(task_id="start")


    def push(**context):
        context["task_instance"].xcom_push(key="skt", value="de")


    push = PythonOperator(
        task_id="push_task",
        python_callable=push
    )

    pull = BashOperator(
        task_id='pull_task',
        bash_command="echo {{ task_instance.xcom_pull(task_ids='push_task', key='skt') }}"
    )

    end = DummyOperator(task_id="end")

    start >> push >> pull >> end
