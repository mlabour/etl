#!/usr/bin/env python3

import ast
import os
from pathlib import Path
import string
import types
import logging
import yaml

from c3p_core import service

logger = logging.getLogger(__name__)


def namespace_it_deep(value, env):
    """Turn a dictionary in an object with attributes as keys (deep algo).
    String values will be treated as templates and substituted using the env dict.
    """
    if type(value) is dict:
        for k, v in value.items():
            value[k] = namespace_it_deep(v, env)
        return types.SimpleNamespace(**value)
    elif type(value) is list:
        return [namespace_it_deep(i, env) for i in value]
    elif type(value) is str:
        substituted_string = string.Template(value).substitute(env)
        # Try to interpret the value as a non-string
        try:
            return ast.literal_eval(substituted_string)
        except:
            return substituted_string
    else:
        return value


def load_configuration(filepath: str = None) -> types.SimpleNamespace:
    """Load a YAML config file into the ENV global variable.

    The YAML keys are added as properties of ENV. For example: ENV.my_config_key.
    YAML arrays are mapped to lists and YAML hashes to dicts.
    """

    path = Path(filepath)

    logger.info(f"Loading config from {path}")
    with open(path) as yamlfile:
        app_conf = yaml.safe_load(yamlfile)
    service.ENV = namespace_it_deep(app_conf, os.environ)

    return service.ENV

