#
# Packages
#

import datetime

import dateutil
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from plugins.ut_kube_plugin import SparkK8sSubmitOperator

#
# Variables and functions
#

default_args = {
    'owner': 'airflow',
    'slack_dag_author': '@ivan.ding @anurag.singh',
    'start_date': datetime.datetime(
        year=2018,
        month=5,
        day=24,
        hour=4
    ),
    # 'end_date': datetime.datetime(
    #     year=2018,
    #     month=6,
    #     day=13,
    #     hour=0,
    #     minute=5
    # ),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5),
    # 'on_failure_callback': notify_failure
}


#
# Functions
#

def get_file_path_suffix(ds, **kwargs):
    ts = dateutil.parser.parse(kwargs['ts']) - datetime.timedelta(hours=1)
    input_parsed_suffix = "{}-{:02d}-{:02d}/{}-{:02d}-{:02d}T{:02d}-00-00Z.gz".format(ts.year, ts.month, ts.day,
                                                                                      ts.year,
                                                                                      ts.month, ts.day,
                                                                                      ts.hour)
    output_parsed_suffix = "{}-{:02d}-{:02d}T{:02d}-00-00Z".format(ts.year, ts.month, ts.day, ts.hour)
    print("suffix is :" + input_parsed_suffix)
    Variable.set('jumpcloud_input_parsed_suffix', input_parsed_suffix)
    Variable.set('jumpcloud_output_parsed_suffix', output_parsed_suffix)
    return


#
# Flow definition
#

dag = DAG(
    "malacca-jumpcloud-v2",
    default_args=default_args,
    catchup=True,
    schedule_interval='5 * * * *',
    concurrency=2
)

# Tasks

t_get_ts = PythonOperator(
    task_id='t_get_ts',
    provide_context=True,
    python_callable=get_file_path_suffix,
    dag=dag)

t_log_parser = SparkK8sSubmitOperator(
    task_id='t_log_parser',
    class_path='com.grab.spark.handlers.LogHandler',
    app_path_s3="s3://malacca-dev/spark-jobs/log-parser/log-parser-1.0-SNAPSHOT-uber.jar",
    config={
        'spark.executor.instances': 3
    },
    extra_params="{{ var.value.malacca_log_parser_config_path }} jumpcloud {{ var.value.malacca_log_parser_jumpcloud_input_prefix }}{{ var.value.jumpcloud_input_parsed_suffix }} {{ var.value.malacca_log_parser_jumpcloud_output_prefix }}{{ var.value.jumpcloud_output_parsed_suffix }}",
    dag=dag
)

t_log_parser.set_upstream(t_get_ts)
