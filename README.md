
# Airflow Spark-on-k8s Job Builder

Trying to avoid excessive boilerplate code duplication for building a DAG that will submit a Spark job to Spark-Operator in a Kubernetes cluster.

## Using this library

This library was only tested together with Airflow deployments versions `2.7.2` and `2.10.3`, and on AWS EKS. 

### airflow version 2.7.2
In version `2.7.2` we had to use the SparkSensor in order to follow progress and infer when a spark job was successfully completed. For that we pass `use_sensor=True` to the `SparkK8sJobBuilder` constructor.

Here is an example of how to use this library:
```python

from airflow import DAG
from datetime import datetime, timedelta

from airflow_spark_on_k8s_job_builder.core import SparkK8sJobBuilder

default_args = {
    "owner": "data-engineering-team",
    "retries": 3,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=2),
}

DAG_ID = "spark_pi"
TASK_ID = "spark-pi-submit"
TASK_NAMESPACE = "airflow"

builder = SparkK8sJobBuilder(
    task_id=TASK_ID,
    job_name="test-spark-on-k8s",
    main_class="org.apache.spark.examples.SparkPi",
    main_application_file="local:///opt/spark/examples/jars/spark-examples_2.12-3.4.1.jar",
    service_account=TASK_NAMESPACE,
    namespace=TASK_NAMESPACE,
    docker_img="apache/spark",
    docker_img_tag="3.4.1-scala2.12-java11-ubuntu",
    use_sensor=True,
)

dag_params = builder.build_dag_params(
    extra_params={
        "env": "dev",
    },
)

with (DAG(
        DAG_ID,
        default_args=default_args,
        description="An example spark k8s task to exemplify how one can build "
                    "spark airflow coordinates apps on top of k8s",
        params=dag_params,
        schedule_interval=timedelta(days=1),
        catchup=False,
        doc_md=__doc__,
        start_date=datetime(2024, 11, 21),
        dagrun_timeout=timedelta(hours=3),
        max_active_runs=1,
        template_searchpath=[
            # we need this to instruct airflow where it can find the spark yaml file
            "/home/airflow/.local/lib/python3.9/site-packages/airflow_spark_on_k8s_job_builder/",
            "/opt/airflow/dags/",
        ],
        tags=["tutorials", "hello-world"],
) as dag):
    spark_tasks = builder \
        .set_dag(dag) \
        .set_executor_cores(2) \
        .set_executor_instances(2) \
        .set_executor_memory("4g") \
        .build()
    spark_tasks[0] >> spark_tasks[1]
```


### airflow version 2.10.3
In version `2.10.3` the sensor was not needed anymore, but we had to [implement a workaround](https://github.com/apache/airflow/issues/39184) for `SparkKubernetesOperator` to be able to understand that spark job was successfully completed.
To use that fix, you can either call `.setup_xcom_sidecar_container()` method on the builder object, or, alternatively, instantiate the builder and pass the option `update_xcom_sidecar_container=True` (directly in `SparkK8sJobBuilder` constructor).

Here is an example of how to use this library:

```python

from datetime import datetime, timedelta

from airflow import DAG

from airflow_spark_on_k8s_job_builder.core import SparkK8sJobBuilder

default_args = {
    "owner": "data-engineering-team",
    "retries": 3,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=2),
}

DAG_ID = "spark_pi"
TASK_ID = "spark-pi-submit"
TASK_NAMESPACE = "airflow"

builder = SparkK8sJobBuilder(
    task_id=TASK_ID,
    job_name="test-spark-on-k8s",
    main_class="org.apache.spark.examples.SparkPi",
    main_application_file="local:///opt/spark/examples/jars/spark-examples_2.12-3.4.1.jar",
    service_account=TASK_NAMESPACE,
    namespace=TASK_NAMESPACE,
    docker_img="apache/spark",
    docker_img_tag="3.4.1-scala2.12-java11-ubuntu",
    use_sensor=False,
)

dag_params = builder.build_dag_params(
    extra_params={
        "env": "dev",
    },
)

with (DAG(
        DAG_ID,
        default_args=default_args,
        description="An example spark k8s task to exemplify how one can build "
                    "spark airflow coordinates apps on top of k8s",
        params=dag_params,
        schedule_interval=timedelta(days=1),
        catchup=False,
        doc_md=__doc__,
        start_date=datetime(2024, 11, 21),
        dagrun_timeout=timedelta(hours=3),
        max_active_runs=1,
        template_searchpath=[
            # we need this to instruct airflow where it can find the spark yaml file
            "/home/airflow/.local/lib/python3.9/site-packages/airflow_spark_on_k8s_job_builder/",
            "/opt/airflow/dags/",
        ],
        tags=["tutorials", "hello-world"],
) as dag):
    builder \
        .set_dag(dag) \
        .set_executor_cores(2) \
        .set_executor_instances(2) \
        .set_executor_memory("4g") \
        .setup_xcom_sidecar_container() \
        .build()
```
Note that the library also contains the yaml template for the spark job, which is used by the `SparkK8sJobBuilder` to build the DAG. So depending on where you install it, you'll need to reference that same path in the `template_searchpath` option.


## Development

This project uses python 3.9 for development, and pip-compile for dependency
management.

The following setup is a suggestion using pyenv to manage both python version,
and python virtual environment.

```shell

# install current project python version
pyenv install $(cat .python-version)
# confirm your pyenv is using this python version
pyenv which python
pyenv which pip

# create a virtualenv
pyenv virtualenv $(cat .python-version) airflowsparkk8sbuilder

# activate the local virtualenv
pyenv activate airflowsparkk8sbuilder

# make sure pip is up to date
pip install --upgrade pip

# install pip-tools for pip-compile and pip-sync features
pip install pip-tools

# install wheel
pip install wheel

# run pip-sync to install pip-compile generated requirements and dev requirements
pip-sync requirements.txt requirements-dev.txt

```


### Adding Dependencies
The `requirements.txt` and `requirements-dev.txt` files are generated using [pip-compile](https://github.com/jazzband/pip-tools) and should **not** be edited manually. To add new dependencies, simply add them to the respective `requirements.in` or `requirements-dev.in` files and update the `.txt` files by running:

```shell
pip-compile requirements.in --output-file requirements.txt
pip-compile requirements-dev.in --output-file requirements-dev.txt
```

To make sure your environment is up-to-date with the latest changes you added, run `pip-sync` command:
```shell
pip-sync requirements.txt requirements-dev.txt
```

*Note: The dev requirements are constrained by any dependencies in the requirements file.*

### Releasing

#### Releasing to test pypi

Update the library's version in `setup.py`. This should build your app in `./dist` directory.

Then:
```shell
# activate venv
pyenv activate airflowsparkk8sbuilder
# clean up previous builds
python setup.py clean --all && rm -rf dist && rm -rf airflow_spark_on_k8s_job_builder.egg-info
# build a package
python -m build
# upload to test pypi
twine upload --repository testpypi dist/*
```
