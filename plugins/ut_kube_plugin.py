import re

from airflow.contrib.kubernetes.pod import Resources
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from dags.common.kube_utils import _get_valid_kubernetes_resource_name, _get_this_kubernetes_namespace


class SparkK8sSubmitOperator(KubernetesPodOperator):
    """
    Submit spark job operator.

    This operator help you submit spark job on the Spark cluster running on Kubernetes.
    """
    _supported_version = {
        "2.4.0": "/opt/spark"
    }

    ui_color = '#F36311'
    _s3_regex = "s3[a,n]?://(?P<dir>.+)/(?P<jar_name>.*?\.(?:jar|py))"
    _spark_submit_template = "{env} {spark_home}/bin/spark-submit {config} {app_path_s3} {param}"

    def __init__(self,
                 class_path=None,
                 app_path_s3=None,
                 jars=None,
                 packages=None,
                 py_files=None,
                 extra_params="",
                 config=None,
                 spark_version="2.4.0",
                 env="",
                 *args, **kwargs):
        """
        :param class_path: Fully qualified class name
        :type str
        :param app_path_s3: S3 path to the app (Jar or python file)
        :type str
        :param jars: Comma-separated list of jars to include on the driver and executor classpaths
        :type str
        :param packages: Comma-separated list of maven coordinates of jars to include on the driver and executor classpaths
        :type str
        :param py_files: Comma-separated list of .zip, .egg, or .py files to place on the PYTHONPATH for Python apps
        :type str
        :param extra_params: App params
        :type str
        :param config: Overriding spark config
        :type dict
        :param spark_version:
        :type str
        """
        self.extra_params = extra_params
        self.class_path = class_path
        self.jars = jars
        self.packages = packages
        self.py_files = py_files
        self.app_path_s3 = app_path_s3
        self.config = config

        if spark_version not in self._supported_version:
            raise Exception("Spark version {} not supported".format(spark_version))
        if not re.match(self._s3_regex, self.app_path_s3):
            raise Exception("Jar path is not a s3 jar file path. Path: {}".format(self.app_path_s3))

        name = _get_valid_kubernetes_resource_name(kwargs["task_id"])

        # Get extra configs
        all_config = [
            "--name {}".format(name),
            "--deploy-mode client"
        ]

        if self.class_path is not None:
            all_config.append("--class {}".format(self.class_path))

        if self.jars is not None:
            all_config.append("--jars {}".format(self.jars))

        if self.packages is not None:
            all_config.append("--packages {}".format(self.packages))

        if self.py_files is not None:
            all_config.append("--py-files {}".format(self.py_files))

        if self.config is not None:
            for key, value in self.config.items():
                if "spark.kubernetes.driver.annotation" in key or "spark.kubernetes.executor.annotation" in key:
                    continue
                all_config.append("--conf {}={}".format(key, value))

        _spark_submit_template = self._spark_submit_template.format(
            spark_home=self._supported_version[spark_version],
            config=" ".join(all_config),
            app_path_s3=app_path_s3,
            param=self.extra_params,
            env=env,
        )

        env_vars = {
            'SPARK_KUBERNETES_NAMESPACE': 'malacca'
        }
        volume_mount = VolumeMount('spark-logs',
                                   mount_path='/logs/spark-events',
                                   sub_path=None,
                                   read_only=False)
        volume_config = {
            'persistentVolumeClaim': {
                'claimName': 'airflow-malacca-sparkworker-spark-logs-pvc'
            }
        }
        volume = Volume(name='spark-logs', configs=volume_config)
        node_selectors = {
            "cluster-autoscaler.kubernetes.io/scaling-group": "compute"
        }
        pod_annotation = {
            # "iam.amazonaws.com/role": "arn:aws:iam::406280264215:role/EMR_EC2_DefaultRole",
            # "cluster-autoscaler.kubernetes.io/safe-to-evict": "false"
        }

        super(SparkK8sSubmitOperator, self).__init__(
            namespace='malacca',
            name=name,
            cmds=["/bin/bash", "-ec"],
            arguments=["/run_base.sh true && {}".format(_spark_submit_template)],
            node_selectors=node_selectors,
            resources=Resources(request_cpu="3300m"),
            is_delete_operator_pod=True,
            image='551371041312.dkr.ecr.ap-southeast-1.amazonaws.com/spark:2.4.0v3',
            image_pull_policy='Always',
            in_cluster=True,
            service_account_name='spark',
            env_vars=env_vars,
            volumes=[volume],
            volume_mounts=[volume_mount],
            startup_timeout_seconds=1200,
            queue='kube',
            annotations=pod_annotation,
            *args,
            **kwargs)
