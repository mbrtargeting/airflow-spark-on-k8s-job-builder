"""
    Utilities related running Spark on k8s
    More info:
    https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html#sparkkubernetesoperator
"""

import copy
import logging
from datetime import timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Union

from airflow import DAG
from airflow.decorators import task_group
from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.utils.context import Context
from jinja2 import Template

SPARK_AIRFLOW_TASK_GROUP = "spark_task_group"


class CustomizableSparkKubernetesOperator(SparkKubernetesOperator):
    """
    A decorator that allows using airflow macros inside spark k8s template
    It does so by intercepting execute method with a sole purpose of rendering
    a second time the jinja values of the SparkApplication yaml manifest

    In case you need to add another macros to be jinja rendered in the
    SparkApplication manifest - for example ts_nodash_with_tz - just follow the next steps:
        a) add another constructor variable

            ```python
                def __init__(
                        self,
                        *,
                        application_file: str,
                        template_field_ds: str,
                        template_field_ts: str,
                        template_field_ts_nodash_with_tz: str
                        **kwargs,
                ):
                self.template_field_YOUR_NEW_VAR = template_field_YOUR_NEW_VAR
            ```

        b) add that variable to the template fields (before this class constructor):

            ```python
                template_fields: Sequence[str] = ("application_file",
                                                  "template_field_ds",
                                                  "template_field_ts",
                                                  "template_field_ts_nodash_with_tz")
            ```


        c) inject those macros when the builder class creates


            ```python
                task = CustomizableSparkKubernetesOperator(
                    task_id=self._task_id,
                    params=self._job_spec['params'],
                    dag=self._dag,
                    namespace=self._namespace,
                    application_file=self._application_file,
                    retries=self._retries,
                    do_xcom_push=True,
                    execution_timeout=self._task_timeout,
                    template_field_ds='{{ ds }}',
                    template_field_ts='{{ ts }}',
                    template_field_ts_nodash_with_tz='{{ ts_nodash_with_tz }}'
                )
            ```

        d) in `execute` method, add that variable to template_context:

            ```python

                def execute(self, context: Context):
                    template = Template(self.application_file)
                    template_context = {
                        "ds": self.template_field_ds,
                        "ts": self.template_field_ts,
                        "ts_nodash_with_tz": self.template_field_ts_nodash_with_tz,
                    }
            ```


    Ref docs:
        - Airflow macros: https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html

    """

    template_fields: Sequence[str] = (
        "application_file",
        "template_field_ds",
        "template_field_ts",
        "data_interval_end",
    )

    def __init__(
            self,
            *,
            application_file: str,
            template_field_ds: str,
            template_field_ts: str,
            **kwargs,
    ):
        self.template_field_ds = template_field_ds
        self.template_field_ts = template_field_ts
        self.data_interval_end = kwargs.pop("data_interval_end", "{{ data_interval_end }}")
        super().__init__(application_file=application_file, **kwargs)

    def execute(self, context: Context):
        template = Template(self.application_file)
        template_context = {
            "ds": self.template_field_ds,
            "ts": self.template_field_ts,
            "data_interval_end": self.data_interval_end,
        }
        rendered_template = template.render(template_context)
        self.application_file = rendered_template
        logging.info(f"application file rendered is: \n{self.application_file}")
        return super().execute(context)


class Arch(Enum):
    amd64 = {"key": "kubernetes.io/arch", "operator": "In", "values": ["amd64"]}
    arm64 = {"key": "kubernetes.io/arch", "operator": "In", "values": ["arm64"]}


class K8sZone(Enum):
    eu_central_1a = {
        "key": "topology.kubernetes.io/zone",
        "operator": "In",
        "values": ["eu-central-1a"],
    }
    eu_central_1b = {
        "key": "topology.kubernetes.io/zone",
        "operator": "In",
        "values": ["eu-central-1b"],
    }
    eu_central_1c = {
        "key": "topology.kubernetes.io/zone",
        "operator": "In",
        "values": ["eu-central-1c"],
    }


class CapacityType(Enum):
    on_demand = {
        "key": "karpenter.sh/capacity-type",
        "operator": "In",
        "values": ["on-demand"],
    }
    spot = {
        "key": "karpenter.sh/capacity-type",
        "operator": "In",
        "values": ["spot"],
    }


OVERRIDE_ME = "TODO_OVERRIDE_ME"
SPARK_S3A = "spark.hadoop.fs.s3a"
SPARK_K8S_AUTH = "spark.kubernetes.authenticate.submission"
WEB_IDENT_PROVIDER = "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
DEFAULT_SPARK_CONF = {
    f"{SPARK_S3A}.aws.credentials.provider": WEB_IDENT_PROVIDER,
    f"{SPARK_S3A}.path.style.access": "true",
    f"{SPARK_K8S_AUTH}.caCertFile": "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
    f"{SPARK_K8S_AUTH}.oauthTokenFile": "/var/run/secrets/kubernetes.io/serviceaccount/token",
    "spark.kubernetes.driver.service.deleteOnTermination": "true",
}
DEFAULT_SPARK_VERSION = "3.4.2"

SPARK_JOB_SPEC_TEMPLATE = {
    "params": {
        "on_finish_action": "delete_pod",
        "jobName": OVERRIDE_ME,
        "namespace": "default",
        "language": "Scala",
        "dockerImage": "gcr.io/spark/spark",
        "dockerImageTag": "v3.4.2",
        # example: "mainClass": "com.example.dataplatform.MyApp
        "mainClass": OVERRIDE_ME,
        # example: "mainApplicationFile": "local:///app/my-app.jar"
        "mainApplicationFile": OVERRIDE_ME,
        "sparkVersion": DEFAULT_SPARK_VERSION,
        "sparkConf": DEFAULT_SPARK_CONF,
        "jobArguments": [],
        "imagePullSecrets": {},
        "driver": {
            "serviceAccount": OVERRIDE_ME,
            "cores": 1,
            "coreRequest": "1",
            "coreLimit": "1",
            "memory": "2g",
            "affinity": {
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {
                        "nodeSelectorTerms": [
                            {
                                "matchExpressions": [
                                    CapacityType.on_demand.value,
                                ]
                            }
                        ],
                    },
                    "preferredDuringSchedulingIgnoredDuringExecution": [
                        {
                            "weight": 100,
                            "preference": {
                                "matchExpressions": [
                                    Arch.arm64.value,
                                ]
                            },
                        }
                    ],
                },
            },
            "annotations": {
                "karpenter.sh/do-not-evict": "true",
                "karpenter.sh/do-not-consolidate": "true",
            },
            "labels": {"version": "3.4.2"},
            "secrets": {},
            "env": {},
        },
        "executor": {
            "instances": 2,
            "serviceAccount": OVERRIDE_ME,
            "cores": 2,
            "coreRequest": "2",
            "coreLimit": "2",
            "memory": "4g",
            "affinity": {
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {
                        "nodeSelectorTerms": [{"matchExpressions": [CapacityType.spot.value]}],
                    },
                    "preferredDuringSchedulingIgnoredDuringExecution": [
                        {
                            "weight": 100,
                            "preference": {
                                "matchExpressions": [
                                    Arch.arm64.value,
                                ]
                            },
                        }
                    ],
                },
            },
            "annotations": {},
            "labels": {},
            "secrets": {},
        },
    },
}


class SparkK8sJobBuilder(object):
    def __init__(
            self,
            task_id: Optional[str] = None,
            dag: Optional[DAG] = None,
            job_name: Optional[str] = None,
            docker_img: Optional[str] = None,
            docker_img_tag: Optional[str] = None,
            main_class: Optional[str] = None,
            main_application_file: Optional[str] = None,
            job_arguments: Optional[List[str]] = None,
            spark_version: str = DEFAULT_SPARK_VERSION,
            namespace: Optional[str] = None,
            service_account: Optional[str] = None,
            application_file: Optional[str] = None,
            task_timeout: Optional[timedelta] = timedelta(minutes=120),
            sensor_timeout_in_seconds: float = 4 * 60.0,
            sensor_retry_delay_in_seconds: int = 60,
            retries: int = 0,
            use_sensor: bool = False,
    ):
        self._job_spec = copy.deepcopy(SPARK_JOB_SPEC_TEMPLATE)
        self._retries = retries
        self._task_id = task_id
        self._use_sensor = use_sensor
        self._dag = dag
        self._sensor_timeout: float = sensor_timeout_in_seconds
        self._sensor_retry_delay_seconds: int = sensor_retry_delay_in_seconds
        self._application_file = application_file or "spark_k8s_template.yaml"
        self._task_timeout = task_timeout
        self._job_arguments = job_arguments or []
        self._spark_version = spark_version
        self.set_spark_version(spark_version)
        if job_arguments:
            self.set_job_arguments(job_arguments)
        if namespace:
            self._namespace = namespace
            self.set_namespace(namespace)
        if job_name:
            self.set_job_name(job_name)
        if service_account:
            self.set_service_account(service_account)
        if docker_img:
            self.set_docker_img(docker_img)
        if docker_img_tag:
            self.set_docker_img_tag(docker_img_tag)
        if main_class:
            self.set_main_class(main_class)
        if main_application_file:
            self.set_main_application_file(main_application_file)

    def set_dag(self, dag: DAG):
        self._dag = dag

    def set_job_name(self, name: str) -> "SparkK8sJobBuilder":
        """Sets custom job name for the Spark job."""
        if not name or len(name) == 0:
            raise ValueError("Need to provide a non-empty string for changing the job name")
        self._job_spec["params"]["jobName"] = name
        return self

    def set_namespace(self, name: str) -> "SparkK8sJobBuilder":
        """Sets namespace for the Spark job."""
        if not name or len(name) == 0:
            raise ValueError("Need to provide a non-empty string for changing the namespace")
        self._namespace = name
        self._job_spec["params"]["namespace"] = name
        return self

    def set_service_account(self, name: str) -> "SparkK8sJobBuilder":
        """Sets service account for the Spark job."""
        if not name or len(name) == 0:
            raise ValueError("Need to provide a non-empty string for changing the job name")
        self._job_spec["params"]["driver"]["serviceAccount"] = name
        self._job_spec["params"]["executor"]["serviceAccount"] = name
        return self

    def set_main_class(self, name: str) -> "SparkK8sJobBuilder":
        """Sets custom main class for the Spark job."""
        if not name or len(name) == 0:
            raise ValueError("Need to provide a non-empty string for changing the job main class")
        self._job_spec["params"]["mainClass"] = name
        return self

    def set_main_application_file(self, name: str) -> "SparkK8sJobBuilder":
        """Sets custom main class for the Spark job."""
        if not name or len(name) == 0:
            raise ValueError(
                "Need to provide a non-empty string for changing the main application file"
            )
        self._job_spec["params"]["mainApplicationFile"] = name
        return self

    def set_job_arguments(self, arguments: List[str]) -> "SparkK8sJobBuilder":
        """Sets custom main class for the Spark job."""
        if not arguments or len(arguments) == 0:
            raise ValueError(
                "Need to provide a non-empty List[String] for changing the job arguments"
            )
        self._job_spec["params"]["jobArguments"] = arguments
        return self

    def set_spark_version(self, version: str) -> "SparkK8sJobBuilder":
        """Sets custom job name for the Spark job."""
        if not version or len(version) == 0:
            raise ValueError(
                "Need to provide a non-empty string for changing spark version; for example: 3.4.2"
            )
        self._spark_version = version
        self._job_spec["params"]["sparkVersion"] = version
        self._job_spec["params"]["driver"]["labels"]["version"] = version
        return self

    def set_docker_img(self, name: str) -> "SparkK8sJobBuilder":
        """Sets docker image to be used."""
        if not name or len(name) == 0:
            raise ValueError("Need to provide a non-empty string for docker image")
        self._job_spec["params"]["dockerImage"] = name
        return self

    def set_docker_img_tag(self, name: str) -> "SparkK8sJobBuilder":
        """Sets docker image tag to be used."""
        if not name or len(name) == 0:
            raise ValueError("Need to provide a non-empty string for docker image")
        self._job_spec["params"]["dockerImageTag"] = name
        return self

    def set_driver_cores(self, cores: int) -> "SparkK8sJobBuilder":
        """Sets the number of driver cores."""
        if not cores:
            raise ValueError("Need to provide a non-empty value for the number of driver cores")
        self._job_spec["params"]["driver"]["cores"] = cores
        self._job_spec["params"]["driver"]["coreRequest"] = cores
        self._job_spec["params"]["driver"]["coreLimit"] = cores
        return self

    def set_driver_memory(self, memory: str) -> "SparkK8sJobBuilder":
        """Sets the driver memory."""
        if not memory or len(memory) == 0:
            raise ValueError(
                "Need to provide a non-empty string for changing the driver memory value;"
                " for example: 8g"
            )
        self._job_spec["params"]["driver"]["memory"] = memory
        return self

    def set_executor_cores(self, cores: int) -> "SparkK8sJobBuilder":
        """Sets the number of executor cores."""
        if not cores:
            raise ValueError("Need to provide a non-empty value for the number of executor cores")
        self._job_spec["params"]["executor"]["cores"] = cores
        self._job_spec["params"]["executor"]["coreRequest"] = cores
        self._job_spec["params"]["executor"]["coreLimit"] = cores
        return self

    def set_executor_memory(self, memory: str) -> "SparkK8sJobBuilder":
        """Sets the executor memory."""
        if not memory or len(memory) == 0:
            raise ValueError(
                "Need to provide a non-empty string for changing the executor memory value;"
                " for example: 8g"
            )
        self._job_spec["params"]["executor"]["memory"] = memory
        return self

    def set_executor_instances(self, instances: int) -> "SparkK8sJobBuilder":
        """Sets the number of executor instances."""
        if not instances:
            raise ValueError(
                "Need to provide a non-empty value for the number of executor instances"
            )
        self._job_spec["params"]["executor"]["instances"] = instances
        return self

    def set_driver_labels(self, labels: Dict[str, str]) -> "SparkK8sJobBuilder":
        """Sets custom labels for the Spark job."""
        self._job_spec["params"]["driver"]["labels"] = labels
        return self

    def update_driver_labels(self, labels: Dict[str, str]) -> "SparkK8sJobBuilder":
        """Updates specific keys with custom labels for the Spark job."""
        if not labels or len(labels.keys()) == 0:
            raise ValueError("Need to provide a non-empty map of job labels")
        if not self._job_spec["params"]["driver"].get("labels"):
            self._job_spec["params"]["driver"]["labels"] = {}
        self._job_spec["params"]["driver"]["labels"].update(labels)
        return self

    def set_executor_labels(self, labels: Dict[str, str]) -> "SparkK8sJobBuilder":
        """Sets custom labels for the Spark job."""
        self._job_spec["params"]["executor"]["labels"] = labels
        return self

    def update_executor_labels(self, labels: Dict[str, str]) -> "SparkK8sJobBuilder":
        """Updates specific keys with custom labels for the Spark job."""
        if not labels or len(labels.keys()) == 0:
            raise ValueError("Need to provide a non-empty map of job labels")
        if not self._job_spec["params"]["executor"].get("labels"):
            self._job_spec["params"]["executor"]["labels"] = {}
        self._job_spec["params"]["executor"]["labels"].update(labels)
        return self

    def set_spark_conf(self, conf: Dict[str, Union[str, int, float]]) -> "SparkK8sJobBuilder":
        """Sets custom Spark configuration."""
        if not conf or len(conf.keys()) == 0:
            raise ValueError('Need to provide a non-empty map with spark conf')
        if not self._job_spec["params"].get("sparkConf"):
            self._job_spec["params"]["sparkConf"] = {}
        self._job_spec["params"]["sparkConf"] = conf
        return self

    def update_spark_conf(self, conf: Dict[str, Union[str, int, float]]) -> "SparkK8sJobBuilder":
        """Updates specific keys with custom Spark configuration."""
        if not self._job_spec["params"].get("sparkConf"):
            self._job_spec["params"]["sparkConf"] = {}
        self._job_spec["params"]["sparkConf"].update(conf)
        return self

    def set_image_pull_secrets(
            self, conf: Dict[str, Union[str, int, float]]
    ) -> "SparkK8sJobBuilder":
        """Sets custom docker image pull secrets."""
        self._job_spec["params"]["imagePullSecrets"].update(conf)
        return self

    def update_image_pull_secrets(
            self, conf: Dict[str, Union[str, int, float]]
    ) -> "SparkK8sJobBuilder":
        """Sets custom docker image pull secrets."""
        if not conf or len(conf.keys()) == 0:
            raise ValueError("Need to provide a non-empty map with image pull secrets")
        if not self._job_spec["params"].get("imagePullSecrets"):
            self._job_spec["params"]["imagePullSecrets"] = {}
        self._job_spec["params"]["imagePullSecrets"].update(conf)
        return self

    def set_secrets(self, conf: Dict[str, Union[str, int, float]]) -> "SparkK8sJobBuilder":
        """Sets custom secrets to be injected in the driver + executor nodes."""
        if not conf or len(conf.keys()) == 0:
            raise ValueError("Need to provide a non-empty map with secrets")
        if not self._job_spec["params"]["driver"].get("secrets"):
            self._job_spec["params"]["driver"]["secrets"] = {}
        self._job_spec["params"]["driver"]["secrets"].update(conf)
        if not self._job_spec["params"]["executor"].get("secrets"):
            self._job_spec["params"]["executor"]["secrets"] = {}
        self._job_spec["params"]["executor"]["secrets"].update(conf)
        return self

    def set_sensor_timeout(self, value: float) -> "SparkK8sJobBuilder":
        """Sets sensor timeout"""
        if not value:
            raise ValueError("Need to provide a non-empty value for sensor retry delay")
        self._sensor_timeout = value
        return self

    def set_sensor_retry_delay_seconds(self, value: int) -> "SparkK8sJobBuilder":
        """Sets sensor retry delay"""
        if not value:
            raise ValueError("Need to provide a non-empty value for sensor retry delay")
        self._sensor_retry_delay_seconds = value
        return self

    def set_env_vars(self, value: List[Dict[str, str]]):
        """Sets environmental variables"""
        if not value or len(value) == 0:
            raise ValueError("Need to provide a non-empty map with environmental variables")
        self._job_spec["params"]["driver"]["env"] = value
        return self

    def _validate_task_id(self):
        if not self._task_id:
            raise ValueError("Need to provide a task id")

    def _validate_job_name(self):
        if (
                not self._job_spec["params"]["jobName"]
                or self._job_spec["params"]["jobName"] == OVERRIDE_ME
        ):
            raise ValueError("Need to provide a job name")

    def _validate_dag(self):
        if not self._dag:
            raise ValueError("Need to provide a DAG")

    def _validate_job_spec(self):
        if (
                not self._job_spec["params"]["dockerImage"]
                or self._job_spec["params"]["dockerImage"] == OVERRIDE_ME
        ):
            raise ValueError("Need to provide a docker image")
        if (
                not self._job_spec["params"]["dockerImageTag"]
                or self._job_spec["params"]["dockerImageTag"] == OVERRIDE_ME
        ):
            raise ValueError("Need to provide a docker image tag")
        if (
                not self._job_spec["params"]["namespace"]
                or self._job_spec["params"]["namespace"] == OVERRIDE_ME
        ):
            raise ValueError("Need to provide a namespace")
        if (
                not self._job_spec["params"]["mainClass"]
                or self._job_spec["params"]["mainClass"] == OVERRIDE_ME
        ):
            raise ValueError("Need to provide a docker image")
        if (
                not self._job_spec["params"]["mainApplicationFile"]
                or self._job_spec["params"]["mainApplicationFile"] == OVERRIDE_ME
        ):
            raise ValueError("Need to provide a docker image")
        if (
                not self._job_spec["params"]["driver"]["serviceAccount"]
                or self._job_spec["params"]["driver"]["serviceAccount"] == OVERRIDE_ME
        ):
            raise ValueError("Need to provide a service account")
        if (
                not self._job_spec["params"]["executor"]["serviceAccount"]
                or self._job_spec["params"]["driver"]["serviceAccount"] == OVERRIDE_ME
        ):
            raise ValueError("Need to provide a service account")

    def _validate_build(self):
        self._validate_task_id()
        self._validate_job_name()
        self._validate_dag()
        self._validate_job_spec()

    def build(self, use_task_group: bool = False, **kwargs) -> List[BaseOperator]:
        """Constructs and returns the SparkKubernetesOperator instance."""
        self._validate_build()

        task = CustomizableSparkKubernetesOperator(
            task_id=self._task_id,
            params=self._job_spec["params"],
            dag=self._dag,
            namespace=self._namespace,
            application_file=self._application_file,
            retries=self._retries,
            do_xcom_push=True,
            execution_timeout=self._task_timeout,
            template_field_ds="{{ ds }}",
            template_field_ts="{{ ts }}",
            **kwargs,
        )

        if self._use_sensor:
            clear_task_id = (
                f"{SPARK_AIRFLOW_TASK_GROUP}.{self._task_id}" if use_task_group else self._task_id
            )
            sensor = SparkKubernetesSensor(
                task_id="{}_monitor".format(self._task_id),
                namespace=self._namespace,
                application_name="{{ task_instance.xcom_pull(task_ids='"
                                 + clear_task_id
                                 + "')['metadata']['name'] }}",
                dag=self._dag,
                attach_log=True,
                timeout=self._sensor_timeout,
                retries=self._retries,
                retry_delay=timedelta(minutes=0),  # set to 0 since it clears the task immediately
            )
            return [task, sensor]
        return [task]

    def get_job_params(self):
        return self._job_spec["params"]


def build_task_group_partial_dag_graph(group_id: str):
    @task_group(group_id=group_id)
    def _build_graph(*, builder: SparkK8sJobBuilder) -> BaseOperator:
        tasks = builder.build(use_task_group=True)
        nr_tasks = len(tasks)
        if nr_tasks == 1:
            return tasks[0]
        tasks[0] >> tasks[1]
    return _build_graph


def build_dag_params(
        *, spark_jobs_builder: SparkK8sJobBuilder, extra_params: Dict[str, Any]
) -> Dict[str, Any]:
    return spark_jobs_builder.get_job_params() | extra_params
