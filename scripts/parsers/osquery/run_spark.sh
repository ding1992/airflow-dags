#!/usr/bin/env bash

export parsed_date=$(date -d "$1 $2 hours ago" "+/%Y/%m/%d/%H/")
echo parsed_date=$parsed_date

/opt/spark/bin/spark-submit --class com.grab.spark.handlers.LogHandler \
--master k8s://BC6C584679C15A68EA9B8215D5FDDAD4.yl4.ap-southeast-1.eks.amazonaws.com:443 \
--deploy-mode client \
--conf spark.kubernetes.namespace=malacca \
--conf spark.kubernetes.container.image=635353606081.dkr.ecr.ap-southeast-1.amazonaws.com/malacca:spark-2.4.3 \
--conf spark.kubernetes.executor.annotation.iam.amazonaws.com/role=arn:aws:iam::635353606081:role/service_sample_role \
--conf spark.kubernetes.driver.pod.name=spark-on-k8s-launcher \
--conf spark.driver.host=$(hostname -i) \
--conf spark.executor.memory=3g --conf spark.executor.cores=1 \
--conf spark.executor.instances=1 /opt/spark/work-dir/malacca/log-parser-1.0-SNAPSHOT-uber.jar /opt/spark/work-dir/malacca/config.yml osquery s3a://g-eks-prod-cluster/osquery${1} s3a://g-eks-stg-cluster/malacca/osquery${1}