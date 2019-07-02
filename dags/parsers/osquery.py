from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('parser-osquery', default_args=default_args, schedule_interval=timedelta(hours=1))

trigger_log_command = """
cd $AIRFLOW_HOME
./scripts/parsers/osquery/trigger-log.sh {{ ts }} {{ var.value.osquery_hours_diff }}
"""

t1 = BashOperator(
    task_id='trigger-log',
    bash_command=trigger_log_command,
    dag=dag)
