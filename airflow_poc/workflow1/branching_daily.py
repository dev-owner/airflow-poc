from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum

from airflow_poc.module.util import get_dag_and_tag_id

DAG_ID, WORKFLOW_ID = get_dag_and_tag_id(__file__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

DAYS = 14
MIGRATE_DAY = datetime(2021, 3, 26, 0, 0, 0, tzinfo=pendulum.timezone("UTC"))

with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        description='test 14 days catchup daily dags',
        schedule_interval="@daily",
        start_date=days_ago(DAYS),
        tags=[WORKFLOW_ID],
) as dag:

    options = ['load_old', 'load_new']
    start = DummyOperator(
        task_id="start"
    )
    end = DummyOperator(
        task_id="end"
    )

    def choose_source(**context):
        if context["execution_date"] < MIGRATE_DAY:
            return "load_old"
        return "load_new"


    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=choose_source,
        dag=dag,
    )

    start >> branching

    join = DummyOperator(
        task_id='join',
        trigger_rule='none_failed_or_skipped',
    )

    for option in options:
        t = DummyOperator(
            task_id=option
        )

        dummy_follow = DummyOperator(
            task_id='clear_' + option
        )

        branching >> t >> dummy_follow >> join >> end
