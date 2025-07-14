import ast
import copy
import logging
from typing import Any, Dict, List

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.utils.context import Context
from jinja2 import Template


class CustomizableSparkKubernetesOperator(SparkKubernetesOperator):
    """
    A decorator to allow performing "last minute fixes" - just before executing a given task
    instance - when airflow jinja template rendering decides to misbehave
    It addresses different issues we have encountered across different versions of Airflow

    It will allow for re-rendering the SparkApplication yaml manifest with the context object
    and replace airflow macros

    Ref docs:
        - Airflow macros: https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html

    """

    def __init__(
        self,
        *,
        application_file: str,
        sanitize_context: bool,
        rerender_template: bool,
        **kwargs,
    ):
        self._job_spec_params = {"params": kwargs.get("params")}
        self._sanitize_context = sanitize_context
        self._original_application_file = copy.deepcopy(application_file)
        self._rerender_template = rerender_template
        super().__init__(application_file=application_file, **kwargs)

    def _re_render_application_file_template(self, context: Context) -> None:
        # merge airflow context w job spec params
        context.update(self._job_spec_params)
        template = Template(self.application_file)
        rendered_template = template.render(context)
        self.application_file = rendered_template

    @staticmethod
    def _parse_string_to_dict(input_string):
        try:
            parsed_dict = ast.literal_eval(input_string)
            if isinstance(parsed_dict, dict):
                return parsed_dict
        except (ValueError, SyntaxError):
            pass
        return input_string

    def _process_string_array(
        self, key: str, core_dict: Dict[str, List[Any]]
    ) -> List[Dict[str, Any]]:
        objects = core_dict.get(key, [])
        if len(objects) > 0:
            new_objects = []
            for obj in objects:
                if isinstance(obj, str):
                    new_objects.append(self._parse_string_to_dict(obj))
            return new_objects
        return objects

    def _sanitize_context_value_types(self, context: Context) -> Context:
        """
        Workaround for when jinja parsing incorrectly parses maps as strings
        This issue was observed on Airflow version 2.7.2
        """
        if context.get("params", {}).get("volumes"):
            context["params"]["volumes"] = self._process_string_array("volumes", context["params"])
        if context.get("params", {}).get("driver", {}).get("volumeMounts"):
            context["params"]["driver"]["volumeMounts"] = self._process_string_array(
                "volumeMounts", context["params"]["driver"]
            )

        if context.get("params", {}).get("driver", {}).get("sidecars"):
            context["params"]["driver"]["sidecars"] = self._process_string_array(
                "sidecars", context["params"]["driver"]
            )
        return context

    def execute(self, context: Context):
        if self._sanitize_context:
            logging.debug(f"context before being updated is: \n{context}")
            context = self._sanitize_context_value_types(context)
            logging.debug(f"context after being updated is: \n{context}")

        logging.debug(
            f"application file before any rendering is: \n{self._original_application_file}"
        )
        logging.debug(
            f"application file after first (default) rendering is: \n{self.application_file}"
        )
        if self._rerender_template:
            self._re_render_application_file_template(context)

        logging.info(
            f"Submitting the following Spark Application to Spark Operator: \n{self.application_file}"
        )
        return super().execute(context)
