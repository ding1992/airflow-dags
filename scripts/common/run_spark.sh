#!/usr/bin/env bash

/opt/spark/bin/spark-submit --class com.grab.spark.handlers.LogHandler \
--master $1 \
--deploy-mode client \
--conf spark.kubernetes.container.image=$2 \
--conf spark.kubernetes.driver.pod.name=spark-on-k8s-launcher \
--conf spark.driver.host=$(hostname -i) \
--conf spark.executor.memory=$3 --conf spark.executor.cores=$4 \
--conf spark.executor.instances=$5 /opt/spark/applications/log-parser-1.0-SNAPSHOT-uber.jar /opt/spark/applications/config.yml $6 $7 $8