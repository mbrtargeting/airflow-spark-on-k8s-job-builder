from airflow import DAG
from airflow.utils import yaml
from datetime import timedelta, datetime, date, timezone
import pendulum
from unittest import TestCase


from airflow_spark_on_k8s_job_builder.customizable_spark_k8s_operator import CustomizableSparkKubernetesOperator


class TestCustomizableSparkKubernetesOperator(TestCase):

    def setUp(self):
        self.mock_dag = DAG(
            dag_id="test_dag",
            default_args={"retries": 3, 'retry_delay': timedelta(minutes=5)},
            start_date=datetime(2023, 1, 1),
        )
        self.job_name = "some-job"
        self.params = {"params": {"jobName": self.job_name}}
        self.task_id = "test_task_id"
        self.namespace = "my_namespace"
        self.service_account = "my_service_account"
        self.task_id = "test_task_id"
        self.retries = 0
        self.execution_timeout = timedelta(minutes=5)
        self.do_xcom_push = True

    def _set_sut(self, file_contents: str) -> None:
        self.sut = CustomizableSparkKubernetesOperator(
            task_id=self.task_id,
            dag=self.mock_dag,
            params=self.params,
            retries=self.retries,
            execution_timeout=self.execution_timeout,
            namespace=self.namespace,
            do_xcom_push=self.do_xcom_push,
            application_file=file_contents,
        )

    def test__re_render_application_file_template_should_evaluate_default_injected_params_in_job_args(self):
        # given: a hypothetical airflow instance, where a SparkK8sBuilder has been instantiated
        #       when CustomizableSparkKubernetesOperator is instantiated in SparkK8sBuilder, airflow will
        #       perform the first round of jinja template  application file rendering in the background
        #       which we emulate by passing already a semi-parsed application_file content

        # when: a spark_application_file is loaded, and passed to CustomizableSparkKubernetesOperator constructor
        file_contents = self._test_spark_job_fixture_1()
        self._set_sut(file_contents=file_contents)
        # when: CustomizableSparkKubernetesOperator call re-rendered method with a provided airflow context
        start_hour = 14
        job_duration_in_hours = 1
        execution_date = pendulum.parse(f'2025-02-14T{start_hour}:12:34.000Z')
        mock_context = {
            'execution_date': execution_date,
            'ds': execution_date.to_date_string(),
            'ds_nodash': execution_date.to_date_string().replace('-', ''),
            'data_interval_start': execution_date,
            'data_interval_end': execution_date.add(hours=job_duration_in_hours),
            'ts': execution_date.isoformat(),
            'ts_nodash': execution_date.format('%Y%m%dT%H%M%S')
        }
        self.sut._re_render_application_file_template(context=mock_context)
        res = self.sut.application_file

        # then: it should be able to be parsed without failures
        res = yaml.safe_load(res)
        spec = res.get('spec', {})
        metadata = res.get('metadata', {})

        # then: it should have exactly the expected nr of job arguments
        expected_nr_args = 6
        job_params = spec.get('arguments', [])
        self.assertEqual(expected_nr_args, len(job_params))

        # then: it should have the expected ds param parsed
        expected_ds_param = date(2025, 2, 14)
        self.assertEqual(expected_ds_param, job_params[1])

        # then: it should have correctly inferred the job name
        expected_job_name = f"test-job-{mock_context.get('ds')}"
        self.assertEqual(expected_job_name, metadata.get('name'))

        # then: it should have the expected ts param parsed
        expected_ts_param = datetime(2025, 2, 14, start_hour, 12, 34, tzinfo=timezone.utc)
        self.assertEqual(expected_ts_param, job_params[3])

        # then: it should have the expected data-interval-end param parsed
        expected_data_interval_end_param = datetime(2025, 2, 14, start_hour + job_duration_in_hours, 12, 34, tzinfo=timezone.utc)
        self.assertEqual(expected_data_interval_end_param, job_params[5])

        # then: the final result should be the same
        expected = [
            '--run-date',
            expected_ds_param,
            '--run-ts',
            expected_ts_param,
            '--data-interval-end',
            expected_data_interval_end_param,
        ]
        self.assertEqual(expected, job_params)

    def test__re_render_application_file_template_should_evaluate_extra_params_in_env_vars(self):
        # given: a hypothetical airflow instance, where a SparkK8sBuilder has been instantiated
        #       when CustomizableSparkKubernetesOperator is instantiated in SparkK8sBuilder, airflow will
        #       perform the first round of jinja template  application file rendering in the background
        #       which we emulate by passing already a semi-parsed application_file content

        # when: a spark_application_file is loaded, and passed to CustomizableSparkKubernetesOperator constructor
        file_contents = self._test_spark_job_fixture_2()
        self._set_sut(file_contents=file_contents)

        # when: CustomizableSparkKubernetesOperator call re-rendered method with a provided airflow context
        start_hour = 14
        job_duration_in_hours = 1
        execution_date = pendulum.parse(f'2025-02-14T{start_hour}:12:34.000Z')
        mock_context = {
            'execution_date': execution_date,
            'ds': execution_date.to_date_string(),
            'ds_nodash': execution_date.to_date_string().replace('-', ''),
            'data_interval_start': execution_date,
            'data_interval_end': execution_date.add(hours=job_duration_in_hours),
            'ts': execution_date.isoformat(),
            'ts_nodash': execution_date.format('%Y%m%dT%H%M%S')
        }
        self.sut._re_render_application_file_template(context=mock_context)
        res = self.sut.application_file

        # then: it should be able to be parsed without failures
        res = yaml.safe_load(res)
        spec = res.get('spec', {})
        env = spec.get('driver', {}).get('env')
        metadata = res.get('metadata', {})

        # then: it should have exactly the expected nr of job arguments
        expected_nr_args = 2
        job_params = spec.get('arguments', [])
        self.assertEqual(expected_nr_args, len(job_params))

        # then: it should have the expected ds param parsed
        expected_ds_param = date(2025, 2, 14)
        self.assertEqual(expected_ds_param, job_params[1])

        # then: it should have correctly inferred the job name
        expected_job_name = f"test-job-{mock_context.get('ds')}"
        self.assertEqual(expected_job_name, metadata.get('name'))

        # then: the final result should be the same
        expected = [
            '--run-date',
            expected_ds_param,
        ]
        self.assertEqual(job_params, expected)

        # then: it should parse params.env
        expected = self.job_name
        self.assertEqual(env[0]['value'], expected)

    def test__re_render_application_file_template_should_evaluate_default_injected_params_everywhere_in_the_template(self):
        # given: a hypothetical airflow instance, where a SparkK8sBuilder has been instantiated
        #       when CustomizableSparkKubernetesOperator is instantiated in SparkK8sBuilder, airflow will
        #       perform the first round of jinja template  application file rendering in the background
        #       which we emulate by passing already a semi-parsed application_file content

        # when: a spark_application_file is loaded, and passed to CustomizableSparkKubernetesOperator constructor
        file_contents = self._test_spark_job_fixture_3()
        self._set_sut(file_contents=file_contents)

        # when: CustomizableSparkKubernetesOperator call re-rendered method with a provided airflow context
        start_hour = 14
        job_duration_in_hours = 1
        execution_date = pendulum.parse(f'2025-02-14T{start_hour}:12:34.000Z')
        mock_context = {
            'execution_date': execution_date,
            'ds': execution_date.to_date_string(),
            'ds_nodash': execution_date.to_date_string().replace('-', ''),
            'data_interval_start': execution_date,
            'data_interval_end': execution_date.add(hours=job_duration_in_hours),
            'ts': execution_date.isoformat(),
            'ts_nodash': execution_date.format('%Y%m%dT%H%M%S')
        }
        self.sut._re_render_application_file_template(context=mock_context)
        res = self.sut.application_file

        # then: it should be able to be parsed without failures
        res = yaml.safe_load(res)
        spec = res.get('spec', {})
        env = spec.get('driver', {}).get('env')
        metadata = res.get('metadata', {})

        # then: it should have exactly the expected nr of job arguments
        expected_nr_args = 6
        job_params = spec.get('arguments', [])
        self.assertEqual(len(job_params), expected_nr_args)

        # then: it should have the expected ds param parsed
        expected_ds_param = date(2025, 2, 14)
        self.assertEqual(job_params[1], expected_ds_param)
        expected = '2025-02-14'
        self.assertEqual(env[0]['value'], expected)

        # then: it should have correctly inferred the job name
        expected_job_name = f"test-job-2-{mock_context.get('ds')}"
        self.assertEqual(metadata.get('name'), expected_job_name)

        # then: it should have the expected ts param parsed
        expected_ts_param = datetime(2025, 2, 14, start_hour, 12, 34, tzinfo=timezone.utc)
        self.assertEqual(job_params[3], expected_ts_param)
        expected = f'2025-02-14T{start_hour}:12:34+00:00'
        self.assertEqual(env[1]['value'], expected)

        # then: it should have the expected data-interval-end param parsed
        expected_data_interval_end_param = datetime(2025, 2, 14, start_hour + job_duration_in_hours, 12, 34, tzinfo=timezone.utc)
        self.assertEqual(expected_data_interval_end_param, job_params[5])
        expected = f'2025-02-14 {start_hour + job_duration_in_hours}:12:34+00:00'
        self.assertEqual(env[2]['value'], expected)

        # then: the final result should be the same
        expected = [
            '--run-date',
            expected_ds_param,
            '--run-ts',
            expected_ts_param,
            '--data-interval-end',
            expected_data_interval_end_param,
        ]
        self.assertEqual(job_params, expected)

    def test__re_render_application_file_template_should_evaluate_methods_called_on_airflow_kwargs(self):
        # given: a hypothetical airflow instance, where a SparkK8sBuilder has been instantiated
        #       when CustomizableSparkKubernetesOperator is instantiated in SparkK8sBuilder, airflow will
        #       perform the first round of jinja template  application file rendering in the background
        #       which we emulate by passing already a semi-parsed application_file content

        # when: a spark_application_file is loaded, and passed to CustomizableSparkKubernetesOperator constructor
        file_contents = self._test_spark_job_fixture_4()
        self._set_sut(file_contents=file_contents)

        # when: CustomizableSparkKubernetesOperator call re-rendered method
        start_hour = 14
        job_duration_in_hours = 1
        execution_date = pendulum.parse(f'2025-02-14T{start_hour}:12:34.000Z')
        mock_context = {
            'execution_date': execution_date,
            'ds': execution_date.to_date_string(),
            'ds_nodash': execution_date.to_date_string().replace('-', ''),
            'data_interval_start': execution_date,
            'data_interval_end': execution_date.add(hours=job_duration_in_hours),
            'ts': execution_date.isoformat(),
            'ts_nodash': execution_date.format('%Y%m%dT%H%M%S')
        }
        self.sut._re_render_application_file_template(context=mock_context)
        res = self.sut.application_file

        # then: it should be able to be parsed without failures
        res = yaml.safe_load(res)
        spec = res.get('spec', {})
        env = spec.get('driver', {}).get('env')
        metadata = res.get('metadata', {})

        # then: it should have exactly the expected nr of job arguments
        expected_nr_args = 6
        job_params = res.get('spec', {}).get('arguments', [])
        self.assertEqual(len(job_params), expected_nr_args)

        # then: it should have the expected ds param parsed
        expected_ds_param = date(2025, 2, 14)
        self.assertEqual(job_params[1], expected_ds_param)

        expected_job_name = f"test-job-3-{mock_context.get('ds')}"
        self.assertEqual(expected_job_name, metadata.get('name'))

        # then: it should have the expected ts param parsed
        expected_ts_param = datetime(2025, 2, 14, start_hour, 12, 34, tzinfo=timezone.utc)
        self.assertEqual(job_params[3], expected_ts_param)

        # then: it should have the expected data-interval-end param parsed
        expected_data_interval_end_param = datetime(2025, 2, 14, start_hour + job_duration_in_hours, 12, 34, tzinfo=timezone.utc)
        self.assertEqual(expected_data_interval_end_param, job_params[5])
        expected = '1739545954'
        self.assertEqual(env[1]['value'], expected)
        expected = f'2025-02-14 {start_hour + job_duration_in_hours - 2}:12:34+00:00'
        self.assertEqual(env[0]['value'], expected)

        # then: the final result should be the same
        expected = [
            '--run-date',
            expected_ds_param,
            '--run-ts',
            expected_ts_param,
            '--data-interval-end',
            expected_data_interval_end_param,
        ]
        self.assertEqual(job_params, expected)

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

    def _test_spark_job_fixture_2(self) -> str:
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
        value: "{{ params.jobName }}"
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

    def _test_spark_job_fixture_3(self) -> str:
        return """
---
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: "test-job-2-{{ ds }}"
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
        value: "{{ ds }}"
      - name: "INPUT_VAR_2"
        value: "{{ ts }}"
      - name: "INPUT_VAR_3"
        value: "{{ data_interval_end }}"
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

    def _test_spark_job_fixture_4(self) -> str:
        return """
---
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: "test-job-3-{{ ds }}"
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
    - {{ data_interval_end.to_iso8601_string() }}
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
        value: "{{ data_interval_end.subtract(hours=2) }}"
      - name: "INPUT_VAR_2"
        value: "{{ data_interval_end.int_timestamp }}"
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
