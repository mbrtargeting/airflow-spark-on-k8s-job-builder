from setuptools import find_packages, setup

setup(
    name="airflow-spark-on-k8s-job-builder",
    version="0.4.3",
    author="Stroeer Labs Data Engineering",
    author_email="diogo.aurelio@stroeer.de",
    description="A library to limit the amount of unnecessary boilerplate code required when launching Spark Jobs "
    "against k8s Spark-Operator in Airflow",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/my_library",
    packages=find_packages(),
    package_data={"airflow_spark_on_k8s_job_builder": ["default_spark_k8s_template.yaml"]},
    include_package_data=True,
    install_requires=[],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
