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

dag = DAG('parser-jumpcloud', default_args=default_args, schedule_interval=timedelta(hours=1))

trigger_log_command = """
cd $AIRFLOW_HOME
./dags/scripts/parsers/jumpcloud/trigger-log.sh {{ ts }} {{ var.value.jumpcloud_hours_diff }} {{ var.value.k8s_host}} {{ var.value.spark_image }} {{ var.value.executor_ram }} {{ var.value.executor_core }} {{ var.value.executor_num }} {{ var.value.jumpcloud_source }} {{ var.value.jumpcloud_target }}
"""

t1 = BashOperator(
    task_id='trigger-log',
    bash_command=trigger_log_command,
    dag=dag)
