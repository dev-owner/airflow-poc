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
    "retry_delay": timedelta(seconds=5)
}

DAYS = 1

with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        description="test hourly dags",
        schedule_interval="25 * * * *",  # 매시 25분마다 수행
        start_date=days_ago(DAYS),
        tags=[WORKFLOW_ID],
) as dag:
    dt = "{{ ts }}"
    dd = "{{ ts.split('T')[0] }}"
    hh = "{{ ts.split('T')[1].split(':')[0] }}"

    start = DummyOperator(task_id="start")


    def test_retry(ds, **kwargs):
        pprint(kwargs)
        print(ds)
        raise ValueError("fail test")


    hourly = PythonOperator(
        task_id="print_ts",
        python_callable=test_retry,
    )
    end = DummyOperator(task_id="end")

    start >> hourly >> end
