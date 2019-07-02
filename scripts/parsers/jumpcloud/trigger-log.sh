#!/usr/bin/env bash

export from_path=$(date -d "$1 $2 hours ago" "+/%Y-%m-%d/%Y-%m-%dT%H*.gz")
echo from_path=$from_path

export to_path=$(date -d "$1 $2 hours ago" "+/%Y-%m-%d/%H/")
echo to_path=$to_path

kubectl cp $AIRFLOW_HOME/dags/scripts/common/run_spark.sh spark-on-k8s-launcher:/opt/spark/work-dir/malacca/run_spark.sh
kubectl exec spark-on-k8s-launcher bash /opt/spark/work-dir/malacca/run_spark.sh $3 $4 $5 $6 $7 jumpcloud $8$from_path $9$to_path