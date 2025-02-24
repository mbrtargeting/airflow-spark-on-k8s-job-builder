
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

    def _re_render_application_file_template(self) -> None:
        template_context = {
            "ds": self.template_field_ds,
            "ts": self.template_field_ts,
            "data_interval_end": self.data_interval_end,
        }
        template = Template(self.application_file)
        rendered_template = template.render(template_context)
        self.application_file = rendered_template
        logging.info(f"application file rendered is: \n{self.application_file}")

    def execute(self, context: Context):
        self._re_render_application_file_template()
        return super().execute(context)


