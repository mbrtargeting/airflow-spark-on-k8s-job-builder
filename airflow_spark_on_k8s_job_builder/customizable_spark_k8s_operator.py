
import ast
from airflow.utils.context import Context
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
import copy
from jinja2 import Template
import logging
from typing import Any, Dict, List


class CustomizableSparkKubernetesOperator(SparkKubernetesOperator):
    """
    A decorator that allows using airflow macros inside spark k8s template
    It does so by intercepting execute method with a sole purpose of rendering
    a second time the jinja values of the SparkApplication yaml manifest

    Ref docs:
        - Airflow macros: https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html

    """

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
        logging.info(f"context before being updated is: \n{context}")
        context.update(self._job_spec_params)
        context = self._sanatise_context_value_types(context)
        logging.info(f"context after being updated is: \n{context}")
        template = Template(self.application_file)
        rendered_template = template.render(context)
        self.application_file = rendered_template
        logging.info(f"application file rendered is: \n{self.application_file}")

    @staticmethod
    def _parse_string_to_dict(input_string):
        try:
            logging.warning(f"converting string {input_string} to dict")
            parsed_dict = ast.literal_eval(input_string)
            if isinstance(parsed_dict, dict):
                logging.warning(f"parsed string to dict - {input_string} - {parsed_dict}")
                return parsed_dict
        except (ValueError, SyntaxError):
            pass
        return input_string

    def _process_string_array(self, key: str, core_dict: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
        logging.warning(f"processing {key} - {core_dict}")
        objects = core_dict.get(key, [])
        if len(objects) > 0:
            new_objects = []
            for obj in objects:
                if isinstance(obj, str):
                    logging.warning(f'{key} is a string, converting to dict - {obj}')
                    new_objects.append(self._parse_string_to_dict(obj))
            return new_objects
        return objects

    def _sanatise_context_value_types(self, context: Context) -> Context:
        """
            Last adjustment addresses issue context map merge
        """
        if context.get("volumes"):
            params = copy.deepcopy(context)
            context["volumes"] = self._process_string_array("volumes", params)
        if context.get("driver", {}).get("volumeMounts"):
            driver = copy.deepcopy(context["driver"])
            context["driver"]["volumeMounts"] = self._process_string_array("volumeMounts", driver)

        if context.get("driver", {}).get("sidecars"):
            driver = copy.deepcopy(context["driver"])
            context["driver"]["sidecars"] = self._process_string_array("sidecars", driver)
        return context

    def execute(self, context: Context):
        self._re_render_application_file_template(context)
        return super().execute(context)


