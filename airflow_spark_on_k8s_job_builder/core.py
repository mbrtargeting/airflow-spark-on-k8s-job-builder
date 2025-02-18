"""
    Utilities related running Spark on k8s
    More info:
    https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html#sparkkubernetesoperator
"""

import copy
import logging
from datetime import timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from airflow import DAG
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
DEFAULT_NAMESPACE = "default"

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
        # https://kubeflow.github.io/spark-operator/docs/user-guide.html#specifying-application-dependencies
        "deps": {},
        "driver": {
            "serviceAccount": OVERRIDE_ME,
            "cores": 1,
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
            namespace: str = DEFAULT_NAMESPACE,
            service_account: Optional[str] = None,
            application_file: Optional[str] = None,
            task_timeout: Optional[timedelta] = timedelta(minutes=120),
            sensor_timeout_in_seconds: float = 4 * 60.0,
            sensor_retry_delay_in_seconds: int = 60,
            retries: int = 0,
            use_sensor: bool = False,
            update_xcom_sidecar_container: bool = False,
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
        self._xcom_sidecar_container_updated = False
        self._namespace = namespace
        self.set_namespace(namespace)
        if job_arguments:
            self.set_job_arguments(job_arguments)
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
        if update_xcom_sidecar_container:
            self.setup_xcom_sidecar_container()

    def set_dag(self, dag: DAG):
        self._dag = dag
        return self

    def set_job_name(self, name: str) -> "SparkK8sJobBuilder":
        """Sets custom job name for the Spark job."""
        if not name or len(name) == 0:
            raise ValueError("Need to provide a non-empty string for changing the job name")
        self.get_job_params()["jobName"] = name
        return self

    def set_namespace(self, name: str) -> "SparkK8sJobBuilder":
        """Sets namespace for the Spark job."""
        if not name or len(name) == 0:
            raise ValueError("Need to provide a non-empty string for changing the namespace")
        self._namespace = name
        self.get_job_params()["namespace"] = name
        return self

    def set_service_account(self, name: str) -> "SparkK8sJobBuilder":
        """Sets service account for the Spark job."""
        if not name or len(name) == 0:
            raise ValueError("Need to provide a non-empty string for changing the job name")
        self.get_job_params()["driver"]["serviceAccount"] = name
        self.get_job_params()["executor"]["serviceAccount"] = name
        return self

    def set_main_class(self, name: str) -> "SparkK8sJobBuilder":
        """Sets custom main class for the Spark job."""
        if not name or len(name) == 0:
            raise ValueError("Need to provide a non-empty string for changing the job main class")
        self.get_job_params()["mainClass"] = name
        return self

    def set_main_application_file(self, name: str) -> "SparkK8sJobBuilder":
        """Sets custom main class for the Spark job."""
        if not name or len(name) == 0:
            raise ValueError(
                "Need to provide a non-empty string for changing the main application file"
            )
        self.get_job_params()["mainApplicationFile"] = name
        return self

    def set_job_arguments(self, arguments: List[str]) -> "SparkK8sJobBuilder":
        """Sets custom main class for the Spark job."""
        if not arguments or len(arguments) == 0:
            raise ValueError(
                "Need to provide a non-empty List[String] for changing the job arguments"
            )
        self.get_job_params()["jobArguments"] = arguments
        return self

    def set_spark_version(self, version: str) -> "SparkK8sJobBuilder":
        """Sets custom job name for the Spark job."""
        if not version or len(version) == 0:
            raise ValueError(
                "Need to provide a non-empty string for changing spark version; for example: 3.4.2"
            )
        self._spark_version = version
        self.get_job_params()["sparkVersion"] = version
        self.get_job_params()["driver"]["labels"]["version"] = version
        return self

    def set_docker_img(self, name: str) -> "SparkK8sJobBuilder":
        """Sets docker image to be used."""
        if not name or len(name) == 0:
            raise ValueError("Need to provide a non-empty string for docker image")
        self.get_job_params()["dockerImage"] = name
        return self

    def set_docker_img_tag(self, name: str) -> "SparkK8sJobBuilder":
        """Sets docker image tag to be used."""
        if not name or len(name) == 0:
            raise ValueError("Need to provide a non-empty string for docker image")
        self.get_job_params()["dockerImageTag"] = name
        return self

    def get_driver_tolerations(self):
        self.get_job_params()["driver"].get("tolerations")

    def set_driver_tolerations(self, tolerations: List[Dict[str, str]]) -> "SparkK8sJobBuilder":
        """Sets tolerations for the driver."""
        if not tolerations or len(tolerations) == 0:
            raise ValueError("Need to provide a non-empty list of tolerations")
        self.get_job_params()["driver"]["tolerations"] = tolerations
        return self

    def get_executor_tolerations(self):
        self.get_job_params()["driver"].get("tolerations")

    def set_executor_tolerations(self, tolerations: List[Dict[str, str]]) -> "SparkK8sJobBuilder":
        """Sets tolerations for the executor."""
        if not tolerations or len(tolerations) == 0:
            raise ValueError("Need to provide a non-empty list of tolerations")
        self.get_job_params()["executor"]["tolerations"] = tolerations
        return self

    def set_tolerations(self, tolerations: List[Dict[str, str]]) -> "SparkK8sJobBuilder":
        self.set_driver_tolerations(tolerations)
        self.set_executor_tolerations(tolerations)
        return self

    def get_driver_affinity(self):
        return self.get_job_params()["driver"]["affinity"]

    def set_driver_affinity(self, affinity: Dict[str, Any]) -> "SparkK8sJobBuilder":
        """Sets affinity for the driver."""
        if not affinity or len(affinity) == 0:
            raise ValueError("Need to provide a non-empty map of affinity")
        self.get_job_params()["driver"]["affinity"] = affinity
        return self

    def update_driver_affinity(self, affinity: Dict[str, Any]) -> "SparkK8sJobBuilder":
        """Updates specific affinity for the driver."""
        if not affinity or len(affinity.keys()) == 0:
            raise ValueError("Need to provide a non-empty map of affinity")
        if not self.get_job_params()["driver"].get("affinity"):
            self.get_job_params()["driver"]["affinity"] = {}
        self.get_job_params()["driver"]["affinity"].update(affinity)
        return self

    def get_executor_affinity(self):
        return self.get_job_params()["executor"]["affinity"]

    def set_executor_affinity(self, affinity: Dict[str, Any]) -> "SparkK8sJobBuilder":
        """Sets affinity for the executor."""
        if not affinity or len(affinity) == 0:
            raise ValueError("Need to provide a non-empty map of affinity")
        self.get_job_params()["executor"]["affinity"] = affinity
        return self

    def update_executor_affinity(self, affinity: Dict[str, Any]) -> "SparkK8sJobBuilder":
        """Updates specific affinity for the executor."""
        if not affinity or len(affinity.keys()) == 0:
            raise ValueError("Need to provide a non-empty map of affinity")
        if not self.get_job_params()["executor"].get("affinity"):
            self.get_job_params()["executor"]["affinity"] = {}
        self.get_job_params()["executor"]["affinity"].update(affinity)
        return self

    def get_driver_annotations(self):
        return self.get_job_params()["driver"]["annotations"]

    def set_driver_annotations(self, annotations: Dict[str, str]) -> "SparkK8sJobBuilder":
        """Sets annotations for the driver."""
        if not annotations or len(annotations) == 0:
            raise ValueError("Need to provide a non-empty map of annotations")
        self.get_job_params()["driver"]["annotations"] = annotations
        return self

    def update_driver_annotations(self, annotations: Dict[str, str]) -> "SparkK8sJobBuilder":
        """Updates specific annotations for the driver."""
        if not annotations or len(annotations.keys()) == 0:
            raise ValueError("Need to provide a non-empty map of annotations")
        if not self.get_job_params()["driver"].get("annotations"):
            self.get_job_params()["driver"]["annotations"] = {}
        self.get_job_params()["driver"]["annotations"].update(annotations)
        return self

    def get_executor_annotations(self):
        return self.get_job_params()["executor"]["annotations"]

    def set_executor_annotations(self, annotations: Dict[str, str]) -> "SparkK8sJobBuilder":
        """Sets annotations for the executor."""
        if not annotations or len(annotations) == 0:
            raise ValueError("Need to provide a non-empty map of annotations")
        self.get_job_params()["executor"]["annotations"] = annotations
        return self

    def update_executor_annotations(self, annotations: Dict[str, str]) -> "SparkK8sJobBuilder":
        """Updates specific annotations for the executor."""
        if not annotations or len(annotations.keys()) == 0:
            raise ValueError("Need to provide a non-empty map of annotations")
        if not self.get_job_params()["executor"].get("annotations"):
            self.get_job_params()["executor"]["annotations"] = {}
        self.get_job_params()["executor"]["annotations"].update(annotations)
        return self

    def set_annotations(self, annotations: Dict[str, str]) -> "SparkK8sJobBuilder":
        self.set_driver_annotations(annotations)
        self.set_executor_annotations(annotations)
        return self

    def update_annotations(self, annotations: Dict[str, str]) -> "SparkK8sJobBuilder":
        self.update_driver_annotations(annotations)
        self.update_executor_annotations(annotations)
        return self

    def get_driver_cores(self):
        return self.get_job_params()["driver"]["cores"]

    def set_driver_cores(self, cores: int) -> "SparkK8sJobBuilder":
        """Sets the number of driver cores."""
        if not cores:
            raise ValueError("Need to provide a non-empty value for the number of driver cores")
        self.get_job_params()["driver"]["cores"] = cores
        min_max_cores = self._cast_cores_to_int(self.get_driver_cores(), self.get_driver_cores_limit(), "driver")
        if min_max_cores[0] is not None and min_max_cores[1] is not None and min_max_cores[0] > min_max_cores[1]:
            self.set_driver_cores_limit(cores)
        return self

    def get_driver_cores_limit(self):
        return self.get_job_params()["driver"]["coreLimit"]

    def set_driver_cores_limit(self, cores: int) -> "SparkK8sJobBuilder":
        """Sets the number of driver cores."""
        if not cores:
            raise ValueError("Need to provide a non-empty value for the number of driver cores")
        self.get_job_params()["driver"]["coreLimit"] = cores
        return self

    def set_driver_memory(self, memory: str) -> "SparkK8sJobBuilder":
        """Sets the driver memory."""
        if not memory or len(memory) == 0:
            raise ValueError(
                "Need to provide a non-empty string for changing the driver memory value;"
                " for example: 8g"
            )
        self.get_job_params()["driver"]["memory"] = memory
        return self

    def get_executor_cores(self):
        return self.get_job_params()["executor"]["cores"]

    def set_executor_cores(self, cores: int) -> "SparkK8sJobBuilder":
        """Sets the number of executor cores."""
        if not cores:
            raise ValueError("Need to provide a non-empty value for the number of executor cores")
        self.get_job_params()["executor"]["cores"] = cores
        min_max_cores = self._cast_cores_to_int(self.get_executor_cores(), self.get_executor_cores_limit(), "executor")
        if min_max_cores[0] is not None and min_max_cores[1] is not None and min_max_cores[0] > min_max_cores[1]:
            self.set_executor_cores_limit(cores)
        return self

    def get_executor_cores_limit(self):
        return self.get_job_params()["executor"]["coreLimit"]

    def set_executor_cores_limit(self, cores: int) -> "SparkK8sJobBuilder":
        """Sets the number of executor cores."""
        if not cores:
            raise ValueError("Need to provide a non-empty (max) value for the number of executor cores")
        self.get_job_params()["executor"]["coreLimit"] = cores
        return self

    def set_executor_memory(self, memory: str) -> "SparkK8sJobBuilder":
        """Sets the executor memory."""
        if not memory or len(memory) == 0:
            raise ValueError(
                "Need to provide a non-empty string for changing the executor memory value;"
                " for example: 8g"
            )
        self.get_job_params()["executor"]["memory"] = memory
        return self

    def set_executor_instances(self, instances: int) -> "SparkK8sJobBuilder":
        """Sets the number of executor instances."""
        if not instances:
            raise ValueError(
                "Need to provide a non-empty value for the number of executor instances"
            )
        self.get_job_params()["executor"]["instances"] = instances
        return self

    def get_deps(self) -> Dict[str, str]:
        return self.get_job_params().get("deps", {})

    def set_deps(self, deps: Dict[str, List[str]]) -> "SparkK8sJobBuilder":
        """Sets dependencies for the Spark job."""
        self.get_job_params()["deps"] = deps
        return self

    def update_deps(self, deps: Dict[str, List[str]]) -> "SparkK8sJobBuilder":
        """Updates specific dependencies for the Spark job."""
        if not deps or len(deps.keys()) == 0:
            raise ValueError("Need to provide a non-empty map of dependencies")
        accepted_values = {"jars", "files", "repositories", "packages", "excludePackages"}
        if len(set(deps.keys()).difference(accepted_values)) > 0:
            raise ValueError("Need to provide a map with keys one of the following values: 'jars', 'files', "
                             "'packages', 'repositories', or 'excludePackages'")
        if not self.get_deps():
            self.get_job_params()["deps"] = {}
        self.get_job_params()["deps"].update(deps)
        return self

    def set_driver_labels(self, labels: Dict[str, str]) -> "SparkK8sJobBuilder":
        """Sets custom labels for the Spark job."""
        self.get_job_params()["driver"]["labels"] = labels
        return self

    def update_driver_labels(self, labels: Dict[str, str]) -> "SparkK8sJobBuilder":
        """Updates specific keys with custom labels for the Spark job."""
        if not labels or len(labels.keys()) == 0:
            raise ValueError("Need to provide a non-empty map of job labels")
        if not self.get_job_params()["driver"].get("labels"):
            self.get_job_params()["driver"]["labels"] = {}
        self.get_job_params()["driver"]["labels"].update(labels)
        return self

    def set_executor_labels(self, labels: Dict[str, str]) -> "SparkK8sJobBuilder":
        """Sets custom labels for the Spark job."""
        self.get_job_params()["executor"]["labels"] = labels
        return self

    def update_executor_labels(self, labels: Dict[str, str]) -> "SparkK8sJobBuilder":
        """Updates specific keys with custom labels for the Spark job."""
        if not labels or len(labels.keys()) == 0:
            raise ValueError("Need to provide a non-empty map of job labels")
        if not self.get_job_params()["executor"].get("labels"):
            self.get_job_params()["executor"]["labels"] = {}
        self.get_job_params()["executor"]["labels"].update(labels)
        return self

    def set_spark_conf(self, conf: Dict[str, Union[str, int, float]]) -> "SparkK8sJobBuilder":
        """Sets custom Spark configuration."""
        if not conf or len(conf.keys()) == 0:
            raise ValueError('Need to provide a non-empty map with spark conf')
        if not self.get_job_params().get("sparkConf"):
            self.get_job_params()["sparkConf"] = {}
        self.get_job_params()["sparkConf"] = conf
        return self

    def update_spark_conf(self, conf: Dict[str, Union[str, int, float]]) -> "SparkK8sJobBuilder":
        """Updates specific keys with custom Spark configuration."""
        if not self.get_job_params().get("sparkConf"):
            self.get_job_params()["sparkConf"] = {}
        self.get_job_params()["sparkConf"].update(conf)
        return self

    def set_image_pull_secrets(
            self, conf: Dict[str, Union[str, int, float]]
    ) -> "SparkK8sJobBuilder":
        """Sets custom docker image pull secrets."""
        self.get_job_params()["imagePullSecrets"].update(conf)
        return self

    def update_image_pull_secrets(
            self, conf: Dict[str, Union[str, int, float]]
    ) -> "SparkK8sJobBuilder":
        """Sets custom docker image pull secrets."""
        if not conf or len(conf.keys()) == 0:
            raise ValueError("Need to provide a non-empty map with image pull secrets")
        if not self.get_job_params().get("imagePullSecrets"):
            self.get_job_params()["imagePullSecrets"] = {}
        self.get_job_params()["imagePullSecrets"].update(conf)
        return self

    def set_secrets(self, conf: Dict[str, Union[str, int, float]]) -> "SparkK8sJobBuilder":
        """Sets custom secrets to be injected in the driver + executor nodes."""
        if not conf or len(conf.keys()) == 0:
            raise ValueError("Need to provide a non-empty map with secrets")
        if not self.get_job_params()["driver"].get("secrets"):
            self.get_job_params()["driver"]["secrets"] = {}
        self.get_job_params()["driver"]["secrets"].update(conf)
        if not self.get_job_params()["executor"].get("secrets"):
            self.get_job_params()["executor"]["secrets"] = {}
        self.get_job_params()["executor"]["secrets"].update(conf)
        return self

    def setup_xcom_sidecar_container(self):
        """
            Sets up the xcom sidecar container in spark drive pod as a sidecar container, as such:

            volumes:
                - name: xcom
                  emptyDir: {}
            driver:
                [...]

               volumeMounts:
                 - name: xcom
                   mountPath: /airflow/xcom
               sidecars:
                 - name: airflow-xcom-sidecar
                   image: alpine
                   command: [ "sh", "-c", 'trap "echo {} > /airflow/xcom/return.json; exit 0" INT; while true; do sleep 1; done;' ]
                   volumeMounts:
                     - name: xcom
                       mountPath: /airflow/xcom
                   resources:
                     requests:
                       cpu: "1m"
                       memory: "10Mi"

            Addresses issue: https://github.com/apache/airflow/issues/39184

        """
        if self._xcom_sidecar_container_updated:
            return self
        update_volumes = {
            "name": "xcom",
            "emptyDir": {},
        }
        update_volume_mounts = {
            "name": "xcom",
            "mountPath": "/airflow/xcom",
        }
        update_sidecars = {
            "image": "alpine",
            "name": "airflow-xcom-sidecar",
            "command": [
                "sh",
                "-c",
                'trap "echo {} > /airflow/xcom/return.json; exit 0" INT; while true; do sleep 1; done;',
            ],
            "volumeMounts": [
                {
                    "name": "xcom",
                    "mountPath": "/airflow/xcom",
                }
            ],
            "resources": {
                "requests": {
                    "cpu": "1m",
                    "memory": "10Mi",
                },
            }
        }
        if not self.get_job_params()["driver"].get("volumeMounts"):
            self.get_job_params()["driver"]["volumeMounts"] = []
        existing_volume_mounts = self.get_job_params()["driver"].get("volumeMounts", [])
        existing_volume_mounts.append(update_volume_mounts)
        self.get_job_params()["driver"]["volumeMounts"] = existing_volume_mounts

        if not self.get_job_params()["driver"].get("sidecars"):
            self.get_job_params()["driver"]["sidecars"] = []
        existing_sidecars = self.get_job_params()["driver"].get("sidecars", [])
        existing_sidecars.append(update_sidecars)
        self.get_job_params()["driver"]["sidecars"] = existing_sidecars

        if not self.get_job_params().get("volumes"):
            self.get_job_params()["volumes"] = []
        existing_volumes = self.get_job_params().get("volumes", [])
        existing_volumes.append(update_volumes)
        self.get_job_params()["volumes"] = existing_volumes

        self._xcom_sidecar_container_updated = True
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
        self.get_job_params()["driver"]["env"] = value
        return self

    def _validate_task_id(self):
        if not self._task_id:
            raise ValueError("Need to provide a task id")

    def _validate_job_name(self):
        if (
                not self.get_job_params()["jobName"]
                or self.get_job_params()["jobName"] == OVERRIDE_ME
        ):
            raise ValueError("Need to provide a job name")

    def _validate_dag(self):
        if not self._dag:
            raise ValueError("Need to provide a DAG")

    def _validate_job_spec(self):
        if (
                not self.get_job_params()["dockerImage"]
                or self.get_job_params()["dockerImage"] == OVERRIDE_ME
        ):
            raise ValueError("Need to provide a docker image")
        if (
                not self.get_job_params()["dockerImageTag"]
                or self.get_job_params()["dockerImageTag"] == OVERRIDE_ME
        ):
            raise ValueError("Need to provide a docker image tag")
        if (
                not self.get_job_params()["namespace"]
                or self.get_job_params()["namespace"] == OVERRIDE_ME
        ):
            raise ValueError("Need to provide a namespace")
        if (
                not self.get_job_params()["mainClass"]
                or self.get_job_params()["mainClass"] == OVERRIDE_ME
        ):
            raise ValueError("Need to provide a docker image (`docker_img` param in builder constructor)")
        if (
                not self.get_job_params()["mainApplicationFile"]
                or self.get_job_params()["mainApplicationFile"] == OVERRIDE_ME
        ):
            raise ValueError("Need to provide 'main_application_file' param in builder constructor")
        if (
                not self.get_job_params()["driver"]["serviceAccount"]
                or self.get_job_params()["driver"]["serviceAccount"] == OVERRIDE_ME
        ):
            raise ValueError("Need to provide a service account (`service_account` param in builder constructor)")
        if (
                not self.get_job_params()["executor"]["serviceAccount"]
                or self.get_job_params()["driver"]["serviceAccount"] == OVERRIDE_ME
        ):
            raise ValueError("Need to provide a service account")

        self._validate_cores()

    def _validate_cores(self):
        driver_cores = self._cast_cores_to_int(
            self.get_driver_cores(), self.get_driver_cores_limit(), "driver"
        )
        if driver_cores[0] is not None and driver_cores[1] is not None \
                and driver_cores[0] > driver_cores[1]:
            raise ValueError("Driver cores should be less than or equal to the limit of cores")

        executor_cores = self._cast_cores_to_int(
            self.get_executor_cores(), self.get_executor_cores_limit(), "executor"
        )
        if executor_cores[0] is not None and executor_cores[1] is not None \
                and executor_cores[0] > executor_cores[1]:
            raise ValueError("Executor cores should be less than or equal to the limit of cores")

    @staticmethod
    def _cast_cores_to_int(requested_cores: str, cores_limit: str, node_type: str) -> Tuple[
        Optional[int], Optional[int]
    ]:
        try:
            requested_cores = int(requested_cores)
            max_cores = int(cores_limit)
            return requested_cores, max_cores
        except ValueError as e:
            logging.warning(f"Unable to compare requested {node_type} cores against max {node_type}"
                            "core number: %s", e)
            return None, None

    def _validate_build(self):
        self._validate_task_id()
        self._validate_job_name()
        self._validate_dag()
        self._validate_job_spec()

    def build(self, **kwargs) -> List[BaseOperator]:
        """Constructs and returns the SparkKubernetesOperator instance."""
        self._validate_build()

        task = CustomizableSparkKubernetesOperator(
            task_id=self._task_id,
            params=self.get_job_params(),
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
            sensor = SparkKubernetesSensor(
                task_id="{}_monitor".format(self._task_id),
                namespace=self._namespace,
                application_name="{{ task_instance.xcom_pull(task_ids='"
                                 + self._task_id
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

    def build_dag_params(self, extra_params: Dict[str, Any]) -> Dict[str, Any]:
        return self.get_job_params() | extra_params
