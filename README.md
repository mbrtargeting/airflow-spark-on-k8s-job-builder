
# Airflow Spark-on-k8s Job Builder

Trying to avoid excessive boilerplate code duplication for building a DAG that will submit a Spark job to Spark-Operator in a Kubernetes cluster.


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
pip-compile requirements.in
pip-compile requirements-dev.in
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
python setup.py clean --all
# build a package
python -m build
# upload to test pypi
twine upload --repository testpypi dist/*
```
