from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy_utils.types.enriched_datetime.pendulum_datetime import pendulum

from airflow_poc.module.util import get_dag_and_tag_id
from airflow_poc.module.util import utility

DAG_ID, WORKFLOW_ID = get_dag_and_tag_id(__file__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False
}
with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        description='test sub package import',
        schedule_interval="@once",
        start_date=datetime(2021, 3, 17, tzinfo=pendulum.timezone("Asia/Seoul")),
        tags=[WORKFLOW_ID],
) as dag:
    t1 = PythonOperator(
        task_id="sub_package_import_test",
        python_callable=utility
    )
