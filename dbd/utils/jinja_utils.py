import os.path
from typing import Dict

from jinja2 import Environment, FileSystemLoader


def apply_template(file: str, params: Dict[str, str]) -> str:
    """
    Applies Jinja2 template
    :param str file: Jinja2 template file
    :param Dict[str, str] params: parameters
    :return: processed content
    :rtype: str
    """
    jinja_profile_env = Environment(loader=FileSystemLoader(os.path.dirname(file)))
    jinja_profile_env.globals.update()
    template = jinja_profile_env.get_template(os.path.basename(file))
    return template.render(params)
