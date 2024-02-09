"""Utility code for working with configuration files"""
import yaml
from pathlib import Path
from typing import Dict, Any


def parse_yaml_cfg(cfg_file: str) -> Dict[str, Any]:
    """Parses the specified yaml file into a dictionary with
    some file transformations.

    If a key ends with path, script, or file,
    then in its value, '~' will be replaced with the user's home directory, and any relative
    paths will be resolved using the location of the cfg file. For example,
    if the config file is /a/b/cfg_file, it contains a_file: ../foo.txt, the
    value is transformed to /a/b/foo.txt

    Args:
        cfg_file: the path to the configuration file in yaml format.
    """
    with open(cfg_file) as fin:
        cfg_d = yaml.safe_load(fin)

    pd = Path(cfg_file).resolve().parent

    for k, v in cfg_d.items():
        if k.endswith('path') or k.endswith('script') or k.endswith('file'):
            p = Path(v).expanduser()
            if v.startswith('..') or v.startswith('.'):
                p = pd / v
            cfg_d[k] = str(p.resolve())

    return cfg_d
