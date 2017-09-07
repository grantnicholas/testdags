from __future__ import print_function
from builtins import range
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG

import time
from pprint import pprint

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='dag_that_fails', default_args=args,
    schedule_interval=None
)

def good():
    print("I'm good")
    return None

def bad():
    print("I'm bad")
    raise Exception("I'm bad")

good = PythonOperator(
    task_id='good',
    python_callable=good,
    dag=dag
)

bad = PythonOperator(
    task_id='bad',
    python_callable=bad,
    dag=dag
)

good >> bad


