from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
import json
from airflow.operators.python_operator import PythonOperator
from restapicall_operator import RestAPICallOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2017, 3, 20),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
} 

dag = DAG('Temptest_pipeline',schedule_interval="00 9 * * *", catchup=False, description='Data Pipeline for testing', default_args = default_args)

ingestion = RestAPICallOperator(
         task_id='SS_to_S3',
         dag=dag)

ingestion

