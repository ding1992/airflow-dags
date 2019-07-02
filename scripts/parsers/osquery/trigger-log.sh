#!/usr/bin/env bash

export parsed_date=$(date -d "$1 $2 hours ago" "+/%Y/%m/%d/%H/")
echo parsed_date=$parsed_date
#/usr/local/spark/bin/spark-submit --class com.grab.spark.handlers.LogHandler \
#--master k8s://https://C89EA0DD3CF22970BF9CAA08E34EA23D.yl4.ap-southeast-1.eks.amazonaws.com \
#--deploy-mode cluster \
#--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
#--conf spark.kubernetes.container.image=551371041312.dkr.ecr.ap-southeast-1.amazonaws.com/spark:2.4.0v3 \
#--conf spark.kubernetes.driver.pod.name=driver4 \
#--executor-memory 3g \
#s3a://ivan-test-data/log-parser-1.0-SNAPSHOT-uber.jar https://ivan-test-data.s3-ap-southeast-1.amazonaws.com/config.yml s3accesslog s3a://grab-551371041312-s3-access-logs/logs/ivan-test-data/ s3a://ivan-test-data/s3access-output-2/

#kubectl exec spark-on-k8s-launcher bash /opt/spark/work-dir/malacca/run_spark.sh $parsed_date
kubectl exec spark-on-k8s-launcher bash /opt/spark/work-dir/malacca/run_spark.sh $1 $2