
from airflow.utils.context import Context
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from jinja2 import Template
import logging
from typing import Sequence


class CustomizableSparkKubernetesOperator(SparkKubernetesOperator):
    """
    A decorator that allows using airflow macros inside spark k8s template
    It does so by intercepting execute method with a sole purpose of rendering
    a second time the jinja values of the SparkApplication yaml manifest

    Ref docs:
        - Airflow macros: https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html

    """

    template_fields: Sequence[str] = (
        "application_file",
    )

    def __init__(
            self,
            *,
            application_file: str,
            **kwargs,
    ):
        self._job_spec_params = kwargs.get('params')
        super().__init__(application_file=application_file, **kwargs)

    def _re_render_application_file_template(self, context: Context) -> None:
        # merge airflow context w job spec params
        context.update(self._job_spec_params)
        template = Template(self.application_file)
        rendered_template = template.render(context)
        self.application_file = rendered_template
        logging.info(f"application file rendered is: \n{self.application_file}")

    def execute(self, context: Context):
        self._re_render_application_file_template(context)
        return super().execute(context)


