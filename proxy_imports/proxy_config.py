import argparse
import pathlib
from string import Template
from typing import Optional, Any
import os
import importlib

import proxy_imports.default_config as default_config

def create_config(
    conf_dir: pathlib.Path,
    config_file: Optional[str] = None
):
    if conf_dir.exists():
        print(f"Configuration already exists at {conf_dir.name}")
        raise Exception("ConfigExists")

    config_file = pathlib.Path(config_file) if config_file else None
    if config_file is None:
        config_file = pathlib.Path(default_config.__file__)

    user_umask = os.umask(0o0077)
    os.umask(0o0077 | (user_umask & 0o0400))  # honor only the UR bit for dirs
    try:
        # pathlib.Path does not handle unusual umasks (e.g., 0o0111) so well
        # in the parents=True case, so temporarily change it.  This is nominally
        # only an issue for totally new users (no .globus_compute/!), but that is
        # also precisely the interaction -- the first one -- that should go smoothly
        conf_dir.mkdir(parents=True, exist_ok=True)
        config_target_path = conf_dir.joinpath("config.py")
        config_target_path.write_text(config_file.read_text())
    finally:
        os.umask(user_umask)

def read_config(conf_path: Optional[str] = None) -> dict[str, Any]:
    conf_path = pathlib.Path(conf_path) if conf_path else pathlib.Path.home() / ".proxy_modules/config.py"
    try:
        spec = importlib.util.spec_from_file_location("config", conf_path)
        if not (spec and spec.loader):
            raise Exception(f"Unable to import configuration (no spec): {conf_path}")
        config = importlib.util.module_from_spec(spec)
        if not config:
            raise Exception(f"Unable to import configuration (no config): {conf_path}")
        spec.loader.exec_module(config)
        return config.config

    except FileNotFoundError as err:
        raise Exception(f"Unable to import configuration (no file): {conf_path}."
                          "You must initialize proxy import with proxy_imports_init or explicitly provide a config file)")

def cli_init_proxy_imports() -> None:
    import argparse
    parser = argparse.ArgumentParser("proxy_imports_init")
    parser.add_argument("-d", "--config_dir", help="Directory to place config file", type= pathlib.Path, default=pathlib.Path.home() / ".proxy_modules/")
    parser.add_argument("-f", "--config_file", help="Config file to use a source", default=None)
    opts = parser.parse_args()

    create_config(opts.config_dir, opts.config_file)