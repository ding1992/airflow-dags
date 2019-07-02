#!/usr/bin/env bash

export parsed_date=$(date -d "$1 $2 hours ago" "+/%Y/%m/%d/%H/")
echo parsed_date=$parsed_date

kubectl exec spark-on-k8s-launcher bash /opt/spark/work-dir/malacca/run_spark.sh $parsed_date