import copy
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict

import pendulum
from airflow import DAG
from airflow.utils import yaml
from jinja2 import Environment, StrictUndefined

from airflow_spark_on_k8s_job_builder.constants import (
    DEFAULT_SPARK_CONF,
    SPARK_JOB_SPEC_TEMPLATE,
)
from airflow_spark_on_k8s_job_builder.core import SparkK8sJobBuilder


class TestSparkK8sJobBuilder(unittest.TestCase):

    def setUp(self):
        self.mock_dag = DAG(
            dag_id="test_dag",
            default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
            start_date=datetime(2023, 1, 1),
        )

        self.task_id = "test_task_id"
        self.job_name = "test_job"
        self.docker_img = "docker_img"
        self.docker_img_tag = "1.2.3"
        self.namespace = "my_namespace"
        self.service_account = "my_service_account"
        self.main_class = "my-class"
        self.main_application_file = "my-app-file"
        self.sut = self._get_sut()
        repo_root = Path().resolve()
        while not (repo_root / ".git").exists():
            # recurse up to find the repo root independent of where PYTHON_PATH is set
            repo_root = repo_root.parent

        self.repo_root = repo_root

    @staticmethod
    def _add_airflow_default_inject_jinja_params(
        params: Dict[str, Any], nodash: str = "mock-nodash-value"
    ):
        params["ts_nodash"] = nodash
        params["task_instance"] = {}
        params["task_instance"]["try_number"] = 1
        return params

    def _load_yaml_template(self):
        yaml_content = self.sut._application_file
        env = Environment(undefined=StrictUndefined)
        template = env.from_string(yaml_content)
        return template

    def _get_sut(self) -> SparkK8sJobBuilder:
        """factory for system under test"""
        return SparkK8sJobBuilder(
            dag=self.mock_dag,
            task_id=self.task_id,
            docker_img=self.docker_img,
            docker_img_tag=self.docker_img_tag,
            job_name=self.job_name,
            namespace=self.namespace,
            service_account=self.service_account,
            main_class=self.main_class,
            main_application_file=self.main_application_file,
            use_sensor=False,
        )

    def test_spark_k8s_yaml_file_is_yaml_renderable(self):
        # given: The default spark k8s app file
        template = self._load_yaml_template()

        params = copy.deepcopy(SPARK_JOB_SPEC_TEMPLATE)
        params["ts_nodash"] = "mock-value"
        params["task_instance"] = {}
        params["task_instance"]["try_number"] = 1
        # when: it renders with the default config into a yaml string
        rendered_content = template.render(params)

        # then: it should be able to be parsed without failures
        res = yaml.safe_load(rendered_content)
        self.assertEqual("sparkoperator.k8s.io/v1beta2", res.get("apiVersion"))
        self.assertEqual("SparkApplication", res.get("kind"))
        self.assertEqual("TODO_OVERRIDE_ME-mock-value-1", res.get("metadata").get("name"))

    def test_spark_k8s_yaml_file_is_replaced_correctly(self):
        # given: The default spark k8s app file
        template = self._load_yaml_template()

        params = {"params": copy.deepcopy(self.sut.get_job_params())}
        nodash = "mock-nodash-value"
        params["ts_nodash"] = nodash
        params["task_instance"] = {}
        params["task_instance"]["try_number"] = 1
        # when: it renders with the default config into a yaml string
        rendered_content = template.render(params)

        # then: it should be able to be parsed without failures
        res = yaml.safe_load(rendered_content)
        self.assertEqual("sparkoperator.k8s.io/v1beta2", res.get("apiVersion"))
        self.assertEqual("SparkApplication", res.get("kind"))
        self.assertEqual(f"{self.job_name}-{nodash}-1", res.get("metadata").get("name"))
        self.assertEqual(self.namespace, res.get("metadata").get("namespace"))

        spec = res.get("spec")
        # then: sparkConf spec should be correctly set
        spark_conf = spec.get("sparkConf")
        self.assertEqual(
            "true",
            spark_conf.get("spark.kubernetes.driver.service.deleteOnTermination"),
        )

        # then: high level specs should be the defaults
        self.assertEqual("Scala", spec.get("type"))
        self.assertEqual("cluster", spec.get("mode"))
        self.assertEqual("docker_img:1.2.3", spec.get("image"))

        driver = spec.get("driver")
        executor = spec.get("executor")
        # then: service account should be the same for both
        self.assertEqual(driver.get("serviceAccount"), self.service_account)
        self.assertEqual(executor.get("serviceAccount"), self.service_account)

        # then: driver & executor should have cores defined
        self.assertEqual(driver.get("cores"), 1)
        self.assertEqual(executor.get("cores"), 2)

        # then: driver & executor should have memory defined
        self.assertEqual(driver.get("memory"), "2g")
        self.assertEqual(executor.get("memory"), "4g")

        # then: executor should have nr of instances defined
        self.assertEqual(executor.get("instances"), 2)

        # then: the driver should not have the xcom sidecar container setup by default
        self.assertEqual(driver.get("sidecars"), [])
        self.assertEqual(driver.get("volumeMounts"), [])

    def test_spark_k8s_yaml_file_add_xcom_sidecar_config_correctly(self):
        # given: The default spark k8s app file
        template = self._load_yaml_template()

        # given a mutated builder spark spec template
        self.sut.setup_xcom_sidecar_container()

        params = {"params": copy.deepcopy(self.sut.get_job_params())}
        nodash = "xcom-sidecar-nodash-value"
        params = self._add_airflow_default_inject_jinja_params(params, nodash=nodash)

        # when: it renders with the default config into a yaml string
        rendered_content = template.render(params)

        # then: it should be able to be parsed without failures
        res = yaml.safe_load(rendered_content)
        self.assertEqual("sparkoperator.k8s.io/v1beta2", res.get("apiVersion"))
        self.assertEqual("SparkApplication", res.get("kind"))
        self.assertEqual(f"{self.job_name}-{nodash}-1", res.get("metadata").get("name"))
        self.assertEqual(self.namespace, res.get("metadata").get("namespace"))

        spec = res.get("spec")
        # then: sparkConf spec should be correctly set
        spark_conf = spec.get("sparkConf")
        self.assertEqual(
            "true",
            spark_conf.get("spark.kubernetes.driver.service.deleteOnTermination"),
        )

        # then: high level specs should be the defaults
        self.assertEqual("Scala", spec.get("type"))
        self.assertEqual("cluster", spec.get("mode"))
        self.assertEqual("docker_img:1.2.3", spec.get("image"))

        driver = spec.get("driver")
        executor = spec.get("executor")
        # then: service account should be the same for both
        self.assertEqual(driver.get("serviceAccount"), self.service_account)
        self.assertEqual(executor.get("serviceAccount"), self.service_account)

        # then: the driver should have the xcom sidecar container setup
        self.assertEqual(len(driver.get("sidecars")), 1)
        sidecars = driver.get("sidecars")[0]
        self.assertEqual(sidecars.get("image"), "alpine")
        self.assertEqual(sidecars.get("name"), "airflow-xcom-sidecar")
        self.assertEqual(sidecars.get("volumeMounts")[0].get("name"), "xcom")
        self.assertEqual(sidecars.get("volumeMounts")[0].get("mountPath"), "/airflow/xcom")
        self.assertEqual(sidecars.get("resources").get("requests").get("cpu"), "1m")
        self.assertEqual(sidecars.get("resources").get("requests").get("memory"), "10Mi")
        self.assertEqual(len(driver.get("volumeMounts")), 1)
        volume_mounts = driver.get("volumeMounts")[0]
        self.assertEqual(volume_mounts.get("name"), "xcom")
        self.assertEqual(volume_mounts.get("mountPath"), "/airflow/xcom")

    def test_set_driver_cores_with_invalid_value_should_fail(self):
        # given: a standard SUT
        # when: Setting SUT with invalid value
        # then: It should raise a ValueError for invalid value
        with self.assertRaises(ValueError):
            self.sut.set_driver_cores(0)

    def test_set_driver_cores_should_succeed(self):
        # given: a standard SUT
        # when: Setting SUT with valid cores value
        expected = 4
        self.sut.set_driver_cores(expected)
        # then: It should correctly assign that value of cores
        self.assertEqual(expected, self.sut._job_spec["params"]["driver"]["cores"])

    def test_set_driver_memory_with_invalid_value_should_fail(self):
        # given: a standard SUT
        # when: Setting SUT with invalid value
        # then: It should raise a ValueError for invalid value
        with self.assertRaises(ValueError):
            self.sut.set_driver_memory("")

    def test_set_driver_memory_should_succeed(self):
        # given: a standard SUT
        # when: Setting SUT with valid memory value
        expected = "8g"
        self.sut.set_driver_memory(expected)
        # then: It should correctly assign that value of memory
        self.assertEqual(expected, self.sut._job_spec["params"]["driver"]["memory"])

    def test_set_driver_memory_should_produce_correct_spark_k8s_yaml_file(self):
        # given: The default spark k8s app file
        template = self._load_yaml_template()

        # when: Setting SUT with valid memory value
        expected = "8000g"
        self.sut.set_driver_memory(expected)

        params = {"params": copy.deepcopy(self.sut.get_job_params())}
        params = self._add_airflow_default_inject_jinja_params(params)
        # when: airflow renders the result job params from builder
        rendered_content = template.render(params)

        # then: it should be able to be parsed without failures
        res = yaml.safe_load(rendered_content)
        # then: it should have mutated driver memory value

        result = res.get("spec", {}).get("driver", {}).get("memory")

        # then: service account should be the same for both
        self.assertEqual(expected, result)

    def test_set_driver_cores_without_limit_should_produce_correct_spark_k8s_yaml_file(
        self,
    ):
        # given: The default spark k8s app file
        template = self._load_yaml_template()

        # when: Setting SUT with valid cores value
        expected = 50
        self.sut.set_driver_cores(expected)
        self.sut.set_driver_cores_limit(None)

        params = {"params": copy.deepcopy(self.sut.get_job_params())}
        params = self._add_airflow_default_inject_jinja_params(params)
        # when: airflow renders the result job params from builder
        rendered_content = template.render(params)

        # then: it should be able to be parsed without failures
        res = yaml.safe_load(rendered_content)

        driver = res.get("spec", {}).get("driver", {})

        # then: It should correctly assign that value of cores
        driver_cores = driver.get("cores")
        self.assertEqual(expected, driver_cores)

        # then: It should not also automatically change cores limit
        driver_cores_limit = driver.get("coreLimit")
        self.assertEqual(
            None,
            driver_cores_limit,
            "core limit should not be set unless specifically requested",
        )

    def test_set_driver_cores_with_limit_should_produce_correct_spark_k8s_yaml_file(
        self,
    ):
        # given: The default spark k8s app file
        template = self._load_yaml_template()

        # when: Setting SUT with valid cores
        expected = 50
        self.sut.set_driver_cores(expected)
        self.sut.set_driver_cores_limit(expected + 10)

        params = {"params": copy.deepcopy(self.sut.get_job_params())}
        params = self._add_airflow_default_inject_jinja_params(params)
        # when: airflow renders the result job params from builder
        rendered_content = template.render(params)

        # then: it should be able to be parsed without failures
        res = yaml.safe_load(rendered_content)
        # then: it should have mutated driver CPU settings

        cores = res.get("spec", {}).get("driver", {}).get("cores")
        self.assertEqual(expected, cores, "driver cores request should be set")

        cores_limit = res.get("spec", {}).get("driver", {}).get("coreLimit")
        self.assertEqual(str(expected + 10), cores_limit, "driver cores limit should be set")

    def test_set_executor_cores_with_invalid_value_should_fail(self):
        # given: a standard SUT
        # when: Setting SUT with invalid value
        # then: It should raise a ValueError for invalid value
        with self.assertRaises(ValueError):
            self.sut.set_executor_cores(0)

    def test_set_executor_cores_should_succeed(self):
        # given: a standard SUT
        # when: Setting SUT with valid cores value
        expected = 4
        self.sut.set_executor_cores(expected)
        # then: It should correctly assign that value of cores
        self.assertEqual(expected, self.sut._job_spec["params"]["executor"]["cores"])
        # then: It should not also automatically change cores limit
        self.assertEqual(None, self.sut._job_spec["params"]["executor"].get("coreLimit"))

    def test_set_executor_cores_without_limit_should_produce_correct_spark_k8s_yaml_file(
        self,
    ):
        # given: The default spark k8s app file
        template = self._load_yaml_template()

        # when: Setting SUT with valid cores value
        expected = 50
        self.sut.set_executor_cores(expected)
        self.sut.set_executor_cores_limit(None)

        params = {"params": copy.deepcopy(self.sut.get_job_params())}
        params = self._add_airflow_default_inject_jinja_params(params)
        # when: airflow renders the result job params from builder
        rendered_content = template.render(params)

        # then: it should be able to be parsed without failures
        res = yaml.safe_load(rendered_content)

        executor = res.get("spec", {}).get("executor", {})

        # then: It should correctly assign that value of cores
        executor_cores = executor.get("cores")
        self.assertEqual(expected, executor_cores)

        # then: It should not also automatically change cores limit
        executor_cores_limit = executor.get("coreLimit")
        self.assertEqual(
            None,
            executor_cores_limit,
            "core limit should not be set unless specifically requested",
        )

    def test_set_executor_cores_with_limit_should_produce_correct_spark_k8s_yaml_file(
        self,
    ):
        # given: The default spark k8s app file
        template = self._load_yaml_template()

        # when: Setting SUT with valid cores value
        expected = 50
        self.sut.set_executor_cores(expected)
        self.sut.set_executor_cores_limit(expected + 10)

        params = {"params": copy.deepcopy(self.sut.get_job_params())}
        params = self._add_airflow_default_inject_jinja_params(params)
        # when: airflow renders the result job params from builder
        rendered_content = template.render(params)

        # then: it should be able to be parsed without failures
        res = yaml.safe_load(rendered_content)

        executor = res.get("spec", {}).get("executor", {})

        # then: It should correctly assign that value of cores
        executor_cores = executor.get("cores")
        self.assertEqual(expected, executor_cores, "executor requested cores should be set")

        # then: It should correctly set the cores limit
        executor_cores_limit = executor.get("coreLimit")
        self.assertEqual(
            str(expected + 10),
            executor_cores_limit,
            "executor core limit should be set",
        )

    def test_set_executor_cores_limit_to_zero_should_fail(self):
        # given: a standard SUT
        # when: Setting SUT with invalid value
        # then: It should raise a ValueError for invalid value
        with self.assertRaises(ValueError):
            self.sut.set_executor_cores_limit(0)

    def test_set_executor_cores_limit_to_none_should_succeed(self):
        # given: a standard SUT
        # when: Setting SUT with valid cores value
        self.sut.set_executor_cores_limit(None)
        # then: It should correctly assign that value of cores
        self.assertEqual(None, self.sut._job_spec["params"]["executor"]["coreLimit"])

    def test_set_executor_cores_limit_should_succeed(self):
        # given: a standard SUT
        # when: Setting SUT with valid cores value
        expected = 4
        self.sut.set_executor_cores_limit(expected)
        # then: It should correctly assign that value of cores
        self.assertEqual(expected, self.sut._job_spec["params"]["executor"]["coreLimit"])

    def test_validate_cores_with_defaults_should_succeed(self):
        # given: a standard SUT
        # then: It should correctly validate the cores
        self.sut._validate_cores()

    def test_validate_cores_should_succeed(self):
        # given: a standard SUT
        # when: Setting SUT with cores value
        self.sut.set_executor_cores(2)
        self.sut.set_executor_cores_limit(4)

        # then: It should have updated cores
        self.assertEqual(2, self.sut.get_executor_cores())
        # then: It should have updated cores limit
        self.assertEqual(4, self.sut.get_executor_cores_limit())
        # then: It should correctly validate the cores
        self.sut._validate_cores()

    def test_validate_cores_should_fail(self):
        # given: a standard SUT
        # when: Setting SUT with cores value
        self.sut.set_executor_cores(2)
        self.sut.set_executor_cores_limit(1)
        # then: It should raise a ValueError for empty value
        with self.assertRaises(ValueError):
            self.sut._validate_cores()

    def test_set_executor_memory_with_invalid_value_should_fail(self):
        # given: a standard SUT
        # when: Setting SUT with invalid memory value
        # then: It should raise a ValueError for empty value
        with self.assertRaises(ValueError):
            self.sut.set_executor_memory("")

    def test_set_executor_memory_should_succeed(self):
        # given: a standard SUT
        # when: Setting SUT with valid memory value
        expected = "16g"
        self.sut.set_executor_memory(expected)
        # then: It should correctly assign that value of memory
        self.assertEqual(expected, self.sut._job_spec["params"]["executor"]["memory"])

    def test_set_executor_instances_with_invalid_value_should_fail(self):
        # given: a standard SUT
        with self.assertRaises(ValueError):
            self.sut.set_executor_instances(0)

    def test_set_executor_instances_should_succeed(self):
        # given: a standard SUT
        # when: Setting SUT with valid nr of instances
        expected = 5
        self.sut.set_executor_instances(expected)
        # then: It should correctly assign that value of instances
        self.assertEqual(expected, self.sut._job_spec["params"]["executor"]["instances"])

    def test_set_driver_affinity_with_invalid_value_should_fail(self):
        # given: a standard SUT
        # when: setting driver affinity with an invalid value
        # then: it should raise a ValueError for empty affinity
        with self.assertRaises(ValueError):
            self.sut.set_driver_affinity({})

    def test_set_driver_affinity_should_succeed(self):
        # given: a standard SUT
        expected_affinity = {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {"key": "key1", "operator": "In", "values": ["value1"]}
                            ]
                        }
                    ]
                }
            }
        }

        # when: setting driver affinity with a valid map
        self.sut.set_driver_affinity(expected_affinity)

        # then: it should correctly set the driver affinity
        self.assertEqual(expected_affinity, self.sut.get_job_params()["driver"]["affinity"])

    def test_update_driver_affinity_with_invalid_value_should_fail(self):
        # given: a standard SUT
        # when: updating driver affinity with an invalid value
        # then: it should raise a ValueError for empty affinity
        with self.assertRaises(ValueError):
            self.sut.update_driver_affinity({})

    def test_update_driver_affinity_should_succeed(self):
        # given: a standard SUT
        initial_affinity = {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {"key": "key1", "operator": "In", "values": ["value1"]}
                            ]
                        }
                    ]
                }
            }
        }
        self.sut.set_driver_affinity(initial_affinity)
        additional_affinity = {
            "podAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": [
                    {
                        "labelSelector": {
                            "matchExpressions": [
                                {"key": "key2", "operator": "In", "values": ["value2"]}
                            ]
                        }
                    }
                ]
            }
        }

        # when: updating driver affinity with a valid map
        self.sut.update_driver_affinity(additional_affinity)

        # then: it should correctly update the driver affinity
        expected_affinity = {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {"key": "key1", "operator": "In", "values": ["value1"]}
                            ]
                        }
                    ]
                }
            },
            "podAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": [
                    {
                        "labelSelector": {
                            "matchExpressions": [
                                {"key": "key2", "operator": "In", "values": ["value2"]}
                            ]
                        }
                    }
                ]
            },
        }
        self.assertEqual(expected_affinity, self.sut.get_job_params()["driver"]["affinity"])

    def test_set_executor_affinity_with_invalid_value_should_fail(self):
        # given: a standard SUT
        # when: setting executor affinity with an invalid value
        # then: it should raise a ValueError for empty affinity
        with self.assertRaises(ValueError):
            self.sut.set_executor_affinity({})

    def test_set_executor_affinity_should_succeed(self):
        # given: a standard SUT
        expected_affinity = {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {"key": "key1", "operator": "In", "values": ["value1"]}
                            ]
                        }
                    ]
                }
            }
        }

        # when: setting executor affinity with a valid map
        self.sut.set_executor_affinity(expected_affinity)

        # then: it should correctly set the executor affinity
        self.assertEqual(expected_affinity, self.sut.get_job_params()["executor"]["affinity"])

    def test_update_executor_affinity_with_invalid_value_should_fail(self):
        # given: a standard SUT
        # when: updating executor affinity with an invalid value
        # then: it should raise a ValueError for empty affinity
        with self.assertRaises(ValueError):
            self.sut.update_executor_affinity({})

    def test_update_executor_affinity_should_succeed(self):
        # given: a standard SUT
        initial_affinity = {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {"key": "key1", "operator": "In", "values": ["value1"]}
                            ]
                        }
                    ]
                }
            }
        }
        self.sut.set_executor_affinity(initial_affinity)
        additional_affinity = {
            "podAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": [
                    {
                        "labelSelector": {
                            "matchExpressions": [
                                {"key": "key2", "operator": "In", "values": ["value2"]}
                            ]
                        }
                    }
                ]
            }
        }

        # when: updating executor affinity with a valid map
        self.sut.update_executor_affinity(additional_affinity)

        # then: it should correctly update the executor affinity
        expected_affinity = {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {"key": "key1", "operator": "In", "values": ["value1"]}
                            ]
                        }
                    ]
                }
            },
            "podAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": [
                    {
                        "labelSelector": {
                            "matchExpressions": [
                                {"key": "key2", "operator": "In", "values": ["value2"]}
                            ]
                        }
                    }
                ]
            },
        }
        self.assertEqual(expected_affinity, self.sut.get_job_params()["executor"]["affinity"])

    def test_affinity_should_produce_correct_spark_k8s_yaml_file(self):
        # given: The default spark k8s app file
        template = self._load_yaml_template()

        # when: setting driver & executor affinities
        expected_driver_affinity = {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "driver",
                                    "operator": "In",
                                    "values": ["value1"],
                                }
                            ]
                        }
                    ]
                }
            }
        }
        expected_executor_affinity = {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "executor",
                                    "operator": "In",
                                    "values": ["value1"],
                                }
                            ]
                        }
                    ]
                }
            }
        }
        self.sut.set_driver_affinity(expected_driver_affinity)
        self.sut.set_executor_affinity(expected_executor_affinity)

        params = {"params": copy.deepcopy(self.sut.get_job_params())}
        params = self._add_airflow_default_inject_jinja_params(params)
        # when: airflow renders the result job params from builder
        rendered_content = template.render(params)

        # then: it should be able to be parsed without failures
        res = yaml.safe_load(rendered_content)

        driver = res.get("spec", {}).get("driver", {})
        executor = res.get("spec", {}).get("executor", {})

        # then: It should correctly assign that value of affinities
        driver_affinity = driver.get("affinity")
        self.assertEqual(expected_driver_affinity, driver_affinity)

        # then: It should correctly assign that value of affinities
        executor_affinity = executor.get("affinity")
        self.assertEqual(expected_executor_affinity, executor_affinity)

    def test_set_driver_tolerations_with_invalid_value_should_fail(self):
        # given: a standard SUT
        # when: setting driver tolerations with an invalid value
        # then: it should raise a ValueError for empty tolerations
        with self.assertRaises(ValueError):
            self.sut.set_driver_tolerations([])

    def test_set_driver_tolerations_should_succeed(self):
        # given: a standard SUT
        expected_tolerations = [
            {
                "key": "key1",
                "operator": "Equal",
                "value": "value1",
                "effect": "NoSchedule",
            }
        ]

        # when: setting driver tolerations with a valid list
        self.sut.set_driver_tolerations(expected_tolerations)

        # then: it should correctly set the list of driver tolerations
        self.assertEqual(expected_tolerations, self.sut.get_job_params()["driver"]["tolerations"])

    def test_set_executor_tolerations_with_invalid_value_should_fail(self):
        # given: a standard SUT
        # when: setting executor tolerations with an invalid value
        # then: it should raise a ValueError for empty tolerations
        with self.assertRaises(ValueError):
            self.sut.set_executor_tolerations([])

    def test_set_executor_tolerations_should_succeed(self):
        # given: a standard SUT
        expected_tolerations = [
            {
                "key": "key2",
                "operator": "Equal",
                "value": "value2",
                "effect": "NoExecute",
            }
        ]

        # when: setting executor tolerations with a valid list
        self.sut.set_executor_tolerations(expected_tolerations)

        # then: it should correctly set the list of executor tolerations
        self.assertEqual(expected_tolerations, self.sut.get_job_params()["executor"]["tolerations"])

    def test_set_driver_annotations_with_invalid_value_should_fail(self):
        # given: a standard SUT
        # when: setting driver annotations with an invalid value
        # then: it should raise a ValueError for empty annotations
        with self.assertRaises(ValueError):
            self.sut.set_driver_annotations({})

    def test_set_driver_annotations_should_succeed(self):
        # given: a standard SUT
        expected_annotations = {"annotation1": "value1"}

        # when: setting driver annotations with a valid map
        self.sut.set_driver_annotations(expected_annotations)

        # then: it should correctly set the list of driver annotations
        self.assertEqual(expected_annotations, self.sut.get_job_params()["driver"]["annotations"])

    def test_update_driver_annotations_with_invalid_value_should_fail(self):
        # given: a standard SUT
        # when: updating driver annotations with an invalid value
        # then: it should raise a ValueError for empty annotations
        with self.assertRaises(ValueError):
            self.sut.update_driver_annotations({})

    def test_update_driver_annotations_should_succeed(self):
        # given: a standard SUT
        initial_annotations = {"annotation1": "value1"}
        self.sut.set_driver_annotations(initial_annotations)
        additional_annotations = {"annotation2": "value2"}

        # when: updating driver annotations with a valid map
        self.sut.update_driver_annotations(additional_annotations)

        # then: it should correctly update the list of driver annotations
        expected_annotations = {"annotation1": "value1", "annotation2": "value2"}
        self.assertEqual(expected_annotations, self.sut.get_job_params()["driver"]["annotations"])

    def test_set_executor_annotations_with_invalid_value_should_fail(self):
        # given: a standard SUT
        # when: setting executor annotations with an invalid value
        # then: it should raise a ValueError for empty annotations
        with self.assertRaises(ValueError):
            self.sut.set_executor_annotations({})

    def test_set_executor_annotations_should_succeed(self):
        # given: a standard SUT
        expected_annotations = {"annotation1": "value1"}

        # when: setting executor annotations with a valid map
        self.sut.set_executor_annotations(expected_annotations)

        # then: it should correctly set the list of executor annotations
        self.assertEqual(expected_annotations, self.sut.get_job_params()["executor"]["annotations"])

    def test_update_executor_annotations_with_invalid_value_should_fail(self):
        # given: a standard SUT
        # when: updating executor annotations with an invalid value
        # then: it should raise a ValueError for empty annotations
        with self.assertRaises(ValueError):
            self.sut.update_executor_annotations({})

    def test_update_executor_annotations_should_succeed(self):
        # given: a standard SUT
        initial_annotations = {"annotation1": "value1"}
        self.sut.set_executor_annotations(initial_annotations)
        additional_annotations = {"annotation2": "value2"}

        # when: updating executor annotations with a valid map
        self.sut.update_executor_annotations(additional_annotations)

        # then: it should correctly update the list of executor annotations
        expected_annotations = {"annotation1": "value1", "annotation2": "value2"}
        self.assertEqual(expected_annotations, self.sut.get_job_params()["executor"]["annotations"])

    def test_update_driver_labels_should_not_accept_empty_dict_should_fail(self):
        # given: a standard SUT
        # when: Setting SUT with invalid labels
        # then: It should raise a ValueError for empty driver labels
        with self.assertRaises(ValueError):
            self.sut.update_driver_labels({})  # Empty dictionary

    def test_set_driver_labels_should_accept_labels_should_succeed(self):
        # given: a standard SUT
        labels = {"app": "test-app", "env": "dev", "version": "3.4.2"}
        # when: Setting SUT with valid labels
        self.sut.set_driver_labels(labels)
        # then: It should correctly assign that value of labels
        self.assertEqual(labels, self.sut._job_spec["params"]["driver"]["labels"])

    def test_update_executor_labels_should_not_accept_empty_dict_should_fail(self):
        # given: a standard SUT
        # when: Setting SUT with invalid labels
        # then: It should raise a ValueError for empty executor labels
        with self.assertRaises(ValueError):
            self.sut.update_executor_labels({})

    def test_set_executor_labels_should_accept_labels(self):
        # given: a standard SUT
        labels = {"app": "test-app", "env": "dev"}
        # when: Setting SUT with valid labels
        self.sut.set_executor_labels(labels)
        # then: It should correctly assign that value of labels
        self.assertEqual(labels, self.sut._job_spec["params"]["executor"]["labels"])

    def test_set_spark_conf_should_succeed(self):
        """Given a valid spark conf, When setting the spark conf, Then it should update the sparkConf."""
        # given: a standard SUT
        # when: Setting SUT with valid spark conf
        conf = {
            "spark.executor.memoryOverhead": "1024M",
            "spark.dynamicAllocation.enabled": "false",
        }
        self.sut.update_spark_conf(conf)

        # then: It should correctly assign that value of labels
        expected_conf = DEFAULT_SPARK_CONF.copy()
        expected_conf["spark.executor.memoryOverhead"] = "1024M"
        expected_conf["spark.dynamicAllocation.enabled"] = "false"
        self.assertEqual(expected_conf, self.sut._job_spec["params"]["sparkConf"])

    def test_get_dependencies_should_fail(self):
        # given: a standard SUT with dependencies
        expected_deps = {"invalid-key": ["dep1", "dep2"]}

        # when: updating dependencies
        # then: it should raise a ValueError for invalid keys dependencies
        with self.assertRaises(ValueError):
            self.sut.update_deps(expected_deps)

    def test_get_dependencies_should_succeed(self):
        # given: a standard SUT with dependencies
        expected_deps = {"jars": ["dep1", "dep2"]}
        self.sut.update_deps(expected_deps)

        # when: getting dependencies
        deps = self.sut.get_deps()

        # then: it should return the correct list of dependencies
        self.assertEqual(expected_deps, deps)

    def test_update_dependencies_with_invalid_value_should_fail(self):
        # given: a standard SUT
        # when: updating dependencies with an invalid value
        # then: it should raise a ValueError for empty dependencies
        with self.assertRaises(ValueError):
            self.sut.update_deps([])

    def test_update_dependencies_should_succeed(self):
        # given: a standard SUT
        expected_deps = {"files": ["dep1", "dep2"]}

        # when: updating dependencies with a valid list
        self.sut.update_deps(expected_deps)

        # then: it should correctly update the list of dependencies
        self.assertEqual(expected_deps, self.sut.get_deps())

    def test_set_main_class_should_succeed(self):
        """Given a valid main class name, When setting the main class, Then it should update the job spec."""
        # given: A valid main class name
        main_class = "org.example.MainClass"

        # when: Setting the main class
        self.sut.set_main_class(main_class)

        # then: The job spec should be updated with the correct main class
        self.assertEqual(main_class, self.sut._job_spec["params"]["mainClass"])

    def test_set_main_class_empty_string_should_fail(self):
        """Given an empty main class, When setting the main class, Then it should raise a ValueError."""
        # given: An empty main class
        main_class = ""

        # when: Setting the main class
        with self.assertRaises(ValueError) as context:
            self.sut.set_main_class(main_class)
        # then: It should raise a ValueError for the empty main class
        self.assertEqual(
            str(context.exception),
            "Need to provide a non-empty string for changing the job main class",
        )

    def test_set_main_application_file_should_succeed(self):
        """Given a valid main application file, When setting the file, Then it should update the job spec."""
        # given: A valid main application file
        main_app_file = "local:///opt/spark/app.jar"

        # when: Setting the main application file
        self.sut.set_main_application_file(main_app_file)

        # then: The job spec should be updated with the correct main application file
        self.assertEqual(main_app_file, self.sut._job_spec["params"]["mainApplicationFile"])

    def test_set_main_application_file_empty_string_should_fail(self):
        """Given an empty main application file, When setting the file, Then it should raise a ValueError."""
        # given: An empty main application file
        main_app_file = ""

        # when: Setting the main application file
        with self.assertRaises(ValueError) as context:
            self.sut.set_main_application_file(main_app_file)

        # then: It should raise a ValueError for the empty main application file
        self.assertEqual(
            "Need to provide a non-empty string for changing the main application file",
            str(context.exception),
        )

    def test_build_with_missing_task_id_should_fail(self):
        """Given no task_id, When building, Then it should raise a ValueError."""
        # given: A builder without a task_id
        builder = SparkK8sJobBuilder(
            dag=self.mock_dag,
            job_name=self.job_name,
            docker_img=self.docker_img,
            docker_img_tag=self.docker_img_tag,
            namespace=self.namespace,
            service_account=self.service_account,
            main_class=self.main_class,
            main_application_file=self.main_application_file,
        )

        # when: Building the operator
        with self.assertRaises(ValueError) as context:
            builder.build()
        # then: building should raise ValueError for missing task_id
        self.assertEqual("Need to provide a task id", str(context.exception))

    def test_build_with_missing_dag_should_fail(self):
        """Given no DAG, When building, Then it should raise a ValueError."""
        # given: A builder without a DAG
        builder = SparkK8sJobBuilder(
            task_id=self.task_id,
            job_name=self.job_name,
            docker_img=self.docker_img,
            docker_img_tag=self.docker_img_tag,
            namespace=self.namespace,
            service_account=self.service_account,
            main_class=self.main_class,
            main_application_file=self.main_application_file,
        )

        # when: Building the operator
        with self.assertRaises(ValueError) as context:
            builder.build()
        # then: Building should raise ValueError for missing DAG
        self.assertEqual("Need to provide a DAG", str(context.exception))

    def test_build_with_missing_job_name_should_fail(self):
        """Given no job_name, When building, Then it should raise a ValueError."""
        # given: A builder without a job name
        builder = SparkK8sJobBuilder(
            task_id=self.task_id,
            dag=self.mock_dag,
            docker_img=self.docker_img,
            docker_img_tag=self.docker_img_tag,
            namespace=self.namespace,
            service_account=self.service_account,
            main_class=self.main_class,
            main_application_file=self.main_application_file,
        )

        # when: Building the operator
        with self.assertRaises(ValueError) as context:
            builder.build()

        # then: it should raise ValueError for missing job name
        self.assertEqual("Need to provide a job name", str(context.exception))

    def test_valid_build_operator_should_succeed(self):
        # given: A valid SparkK8sJobBuilder setup

        # when: Building the operator
        tasks = self.sut.build()
        spark_operator = tasks[0]

        # then: Assert the operator is created with correct attributes
        self.assertEqual("CustomizableSparkKubernetesOperator", spark_operator.operator_name)
        self.assertEqual(self.job_name, spark_operator.params["jobName"])
        self.assertEqual(self.docker_img, spark_operator.params["dockerImage"])
        self.assertEqual(self.docker_img_tag, spark_operator.params["dockerImageTag"])
        self.assertEqual(self.service_account, spark_operator.params["driver"]["serviceAccount"])
        self.assertEqual(self.service_account, spark_operator.params["executor"]["serviceAccount"])

    def test_setup_xcom_sidecar_container(self):
        # given: a standard SUT

        # when: Building the operator
        builder = self.sut.setup_xcom_sidecar_container()

        # then: it should correctly set up the xcom sidecar container
        driver_spec = builder._job_spec["params"]["driver"]
        self.assertIn("volumeMounts", driver_spec)
        self.assertIn("sidecars", driver_spec)

        volume_mounts = driver_spec["volumeMounts"]
        sidecars = driver_spec["sidecars"]

        self.assertEqual(1, len(volume_mounts))
        self.assertEqual("xcom", volume_mounts[0]["name"])
        self.assertEqual("/airflow/xcom", volume_mounts[0]["mountPath"])

        self.assertEqual(1, len(sidecars))
        self.assertEqual("airflow-xcom-sidecar", sidecars[0]["name"])
        self.assertEqual("alpine", sidecars[0]["image"])
        self.assertEqual(
            [
                "sh",
                "-c",
                'trap "echo {} > /airflow/xcom/return.json; exit 0" INT; while true; do sleep 1; done;',
            ],
            sidecars[0]["command"],
        )
        self.assertEqual("xcom", sidecars[0]["volumeMounts"][0]["name"])
        self.assertEqual("/airflow/xcom", sidecars[0]["volumeMounts"][0]["mountPath"])
        self.assertEqual("1m", sidecars[0]["resources"]["requests"]["cpu"])
        self.assertEqual("10Mi", sidecars[0]["resources"]["requests"]["memory"])

    def test_global_volume_mount(self):
        # given: A valid SparkK8sJobBuilder setup

        # when: Building the operator
        builder = self.sut

        builder.add_global_persistent_volume(
            "spark-logs",
            "spark-logs-s3-pvc",
            "/tmp/spark/logs",
            True,
        )
        tasks = builder.build()
        spark_operator = tasks[0]

        # then: Assert the operator is created with correct volume config
        self.assertEqual(
            [
                {
                    "name": "spark-logs",
                    "persistentVolumeClaim": {"claimName": "spark-logs-s3-pvc"},
                }
            ],
            spark_operator.params["volumes"],
        )
        self.assertEqual(
            [
                {
                    "mountPath": "/tmp/spark/logs",
                    "name": "spark-logs",
                    "readOnly": True,
                }
            ],
            spark_operator.params["driver"]["volumeMounts"],
        )
        self.assertEqual(
            [
                {
                    "mountPath": "/tmp/spark/logs",
                    "name": "spark-logs",
                    "readOnly": True,
                }
            ],
            spark_operator.params["executor"]["volumeMounts"],
        )

    def test_re_rendering_template_should_leave_no_jinja_vars_dangling(self):
        # given: a standard SUT with the following special settings
        job_arguments = [
            "--run-date",
            "{{ ds }}",
            "--run-ts",
            "{{ ts }}",
            "--data-interval-end",
            "{{ data_interval_end }}",
        ]
        env_vars = [
            {"name": "RUN_DATE", "value": "{{ ds }}"},
            {"name": "RUN_TS", "value": "{{ ts }}"},
            {"name": "DATA_INTERVAL_END", "value": "{{ data_interval_end.timestamp() }}"},
            {
                "name": "DATA_INTERVAL_START",
                "value": "{{ data_interval_start.to_iso8601_string() }}",
            },
        ]
        # given: the SUT has instantiated the CustomizableSparkK8sOperator
        operator = self.sut.set_job_arguments(job_arguments).set_env_vars(env_vars).build()[0]
        # given: a mock airflow execution context
        start_hour = 14
        job_duration_in_hours = 1
        year = 2025
        month = "02"
        day = 14
        minute = 12
        second = 34
        test_date = date(year, int(month), day)
        execution_date = pendulum.parse(f"{year}-{month}-{day}T{start_hour}:12:34.000Z")
        ts_nodash = execution_date.format("YYYYMMDDTHHmmss")
        mock_context = {
            "task_instance": "super_dag_job",
            "execution_date": execution_date,
            "ds": execution_date.to_date_string(),
            "ds_nodash": execution_date.to_date_string().replace("-", ""),
            "data_interval_start": execution_date,
            "data_interval_end": execution_date.add(hours=job_duration_in_hours),
            "ts": execution_date.isoformat(),
            "ts_nodash": ts_nodash,
        }

        # when: re-rendering the template with context
        try:
            # note: for whatever reason, operator does not render the template outside the task instance context,
            # so we need to artificially execute it to trigger the rendering
            operator._re_render_application_file_template(context=mock_context)
            operator.execute(context=mock_context)
        except Exception as e:
            # then: it should only fail when trying to submit the job to spark-operator
            expected_error_msg = "Invalid kube-config file"
            if expected_error_msg not in str(e):
                raise AssertionError(f"Expected error message not found. Got: {e}")

        # then: it should double-render the application file
        res = operator.application_file

        # then: it should not leave any jinja variables dangling
        self.assertNotIn("{{ params.jobName }}", res)
        self.assertNotIn("{{ params.namespace }}", res)
        self.assertNotIn("{{ params.sparkConf }}", res)
        self.assertNotIn("{{ params.imagePullSecrets }}", res)
        self.assertNotIn("{{ params.language }}", res)
        self.assertNotIn("{{ params.dockerImage }}", res)
        self.assertNotIn("{{ params.dockerImageTag }}", res)
        self.assertNotIn("{{ params.mainApplicationFile }}", res)
        self.assertNotIn("{{ params.sparkVersion }}", res)
        self.assertNotIn("{{ params.jobArguments }}", res)
        self.assertNotIn("{{ params.volumes }}", res)
        self.assertNotIn("{{ params.driver.affinity }}", res)
        self.assertNotIn("{{ params.driver.tolerations }}", res)
        self.assertNotIn("{{ params.driver.annotations }}", res)
        self.assertNotIn("{{ params.driver.cores }}", res)
        self.assertNotIn("{{ params.driver.memory }}", res)
        self.assertNotIn("{{ params.driver.labels }}", res)
        self.assertNotIn("{{ params.driver.serviceAccount }}", res)
        self.assertNotIn("{{ params.driver.secrets }}", res)
        self.assertNotIn("{{ params.driver.env }}", res)
        self.assertNotIn("{{ params.driver.volumeMounts }}", res)
        self.assertNotIn("{{ params.driver.sideCars }}", res)

        self.assertNotIn("{{ params.executor.affinity }}", res)
        self.assertNotIn("{{ params.executor.tolerations }}", res)
        self.assertNotIn("{{ params.executor.annotations }}", res)
        self.assertNotIn("{{ params.executor.cores }}", res)
        self.assertNotIn("{{ params.executor.memory }}", res)
        self.assertNotIn("{{ params.executor.labels }}", res)
        self.assertNotIn("{{ params.executor.serviceAccount }}", res)
        self.assertNotIn("{{ params.executor.secrets }}", res)
        self.assertNotIn("{{ params.executor.env }}", res)
        self.assertNotIn("{{ params.executor.volumeMounts }}", res)
        self.assertNotIn("{{ params.executor.sideCars }}", res)

        # then: it should be able to be parsed without failures
        res = yaml.safe_load(res)
        spec = res.get("spec", {})
        env = spec.get("driver", {}).get("env")
        metadata = res.get("metadata", {})

        # then: it should have exactly the expected nr of job arguments
        expected_nr_args = 6
        job_params = res.get("spec", {}).get("arguments", [])
        self.assertEqual(len(job_params), expected_nr_args)

        # then: it should have the expected ds param parsed
        expected_ds_param = test_date.strftime("%Y-%m-%d")
        self.assertEqual(expected_ds_param, job_params[1])

        expected_job_name = f"{self.job_name}-{ts_nodash.lower()}-"
        self.assertEqual(expected_job_name, metadata.get("name"))

        # then: it should have the expected ts param parsed
        expected_ts_param = datetime(
            year, int(month), day, start_hour, minute, second, tzinfo=timezone.utc
        ).isoformat()
        self.assertEqual(expected_ts_param, job_params[3])

        # then: it should have the expected data-interval-end param parsed
        expected_data_interval_end_param = datetime(
            year,
            int(month),
            day,
            start_hour + job_duration_in_hours,
            minute,
            second,
            tzinfo=timezone.utc,
        )
        dt_str = expected_data_interval_end_param.strftime("%Y-%m-%d %H:%M:%S%z")
        expected_data_interval_end_str_param = f"{dt_str[:-2]}:{dt_str[-2:]}"
        self.assertEqual(expected_data_interval_end_str_param, job_params[5])

        # then: it should have the expected environment variables params parsed
        self.assertEqual(expected_ds_param, env[0]["value"])
        self.assertEqual(expected_ts_param, env[1]["value"])
        expected_timestamp = f"{expected_data_interval_end_param.timestamp()}"
        self.assertEqual(expected_timestamp, env[2]["value"])

        # then: the final result should be the same
        expected = [
            "--run-date",
            expected_ds_param,
            "--run-ts",
            expected_ts_param,
            "--data-interval-end",
            expected_data_interval_end_str_param,
        ]
        self.assertEqual(expected, job_params)
