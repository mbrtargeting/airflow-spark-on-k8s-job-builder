from airflow import DAG
from airflow.utils import yaml
from datetime import timedelta, datetime
from unittest import TestCase


from airflow_spark_on_k8s_job_builder.customizable_spark_k8s_operator import CustomizableSparkKubernetesOperator


class TestCustomizableSparkKubernetesOperator(TestCase):

    def setUp(self):
        self.mock_dag = DAG(
            dag_id="test_dag",
            default_args={"retries": 3, 'retry_delay': timedelta(minutes=5)},
            start_date=datetime(2023, 1, 1),
        )
        self.params = {"params": {}}
        self.task_id = "test_task_id"
        self.job_name = "test_job"
        self.namespace = "my_namespace"
        self.service_account = "my_service_account"
        self.task_id = "test_task_id"
        self.retries = 0
        self.execution_timeout = timedelta(minutes=5)
        self.do_xcom_push = True

    def test__re_render_application_file_template_should_evaluate_default_injected_params_in_job_args(self):
        # given: a hypothetical airflow instance, where a SparkK8sBuilder has been instantiated
        #       when CustomizableSparkKubernetesOperator is instantiated in SparkK8sBuilder, airflow will
        #       perform the first round of jinja template  application file rendering in the background
        #       which we emulate by passing already a semi-parsed application_file content

        # when: a spark_application_file is loaded, and passed to CustomizableSparkKubernetesOperator constructor
        ds = "ds-value"
        ts = "ts-value"
        data_interval_end = "2023-10-01 12:34:56"

        file_contents = self._test_spark_job_fixture_1()
        self.sut = CustomizableSparkKubernetesOperator(
            task_id=self.task_id,
            dag=self.mock_dag,
            params=self.params,
            retries=self.retries,
            execution_timeout=self.execution_timeout,
            namespace=self.namespace,
            do_xcom_push=self.do_xcom_push,
            application_file=file_contents,
            # extra params:
            template_field_ds=ds,
            template_field_ts=ts,
            data_interval_end=data_interval_end,
        )
        # when: CustomizableSparkKubernetesOperator call re-rendered method
        self.sut._re_render_application_file_template()
        res = self.sut.application_file

        # then: it should be able to be parsed without failures
        res = yaml.safe_load(res)

        job_params = res.get('spec', {}).get('arguments', [])

        self.assertEqual(job_params, [
            '--run-date',
            ds,
            '--run-ts',
            ts,
            '--data-interval-end',
            datetime(2023, 10, 1, 12, 34, 56),
        ])

    def _test_spark_job_fixture_1(self) -> str:
        return """
---
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: "test-job-{{ ds }}"
spec:
  sparkConf:
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
    "spark.kubernetes.authenticate.driver.serviceAccountName": "data-platform"
    "spark.kubernetes.authenticate.submission.caCertFile": "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    "spark.kubernetes.authenticate.submission.oauthTokenFile": "/var/run/secrets/kubernetes.io/serviceaccount/token"
    "spark.hadoop.fs.s3a.path.style.access": "true"
  timeToLiveSeconds: 172800
  imagePullSecrets:
    - eu-central-1-ecr-registry
  type: Scala
  mode: cluster
  image: "882811593048.dkr.ecr.eu-central-1.amazonaws.com/data-platform-job:latest"
  imagePullPolicy: Always
  mainClass: eu.m6r.dataplatform.Job
  mainApplicationFile: "local:///app/app.jar"
  sparkVersion: "3.4.2"
  arguments:
    - --run-date
    - {{ds}}
    - --run-ts
    - {{ts}}
    - --data-interval-end
    - {{ data_interval_end }}
  restartPolicy:
    type: Never
  volumes:
    - name: "test-volume"
      hostPath:
        path: "/tmp"
        type: Directory
  driver:
    cores: 3
    memory: "1g"
    labels:
      version: 3.4.2
    nodeSelector:
      "kubernetes.io/arch": arm64
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
            - matchExpressions:
                - key: topology.kubernetes.io/zone
                  operator: In
                  values:
                    - eu-central-1a
            - matchExpressions:
                - key: kubernetes.io/arch
                  operator: In
                  values:
                    - arm64
            - matchExpressions:
                - key: karpenter.sh/capacity-type
                  operator: In
                  values:
                    - on-demand
    annotations:
      "karpenter.sh/do-not-evict": "true"
      "karpenter.sh/do-not-consolidate": "true"
    serviceAccount: data-platform
    env:
      - name: "INPUT_VAR_1"
        value: "false"
  executor:
    cores: 3
    instances: 2
    memory: "15g"
    labels:
      version: 3.2.0
    nodeSelector:
      "kubernetes.io/arch": arm64
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
            - matchExpressions:
                - key: kubernetes.io/arch
                  operator: In
                  values:
                    - arm64
            - matchExpressions:
                - key: karpenter.sh/capacity-type
                  operator: In
                  values:
                    - spot
    annotations:
      "karpenter.sh/do-not-evict": "true"
      "karpenter.sh/do-not-consolidate": "true"
    serviceAccount: data-platform
        """
