import ast
import importlib
import inspect
import io
import os
import os.path
import shutil
import subprocess
import sys
import tarfile
from types import ModuleType
from typing import Optional, Any, Union
from pathlib import Path
import zipfile

from.proxy_config import read_config

from proxystore.proxy import Proxy
from proxystore.store import Store, get_store, register_store
from proxystore.connectors.dim import utils

from PyInstaller.utils.hooks import collect_dynamic_libs, conda_support
from PyInstaller.compat import is_pure_conda

def _serialize_module(m: ModuleType) -> dict[str, bytes]:
    """ Method used to turn module into serialized bitstring"""
    
    extract = False
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as fzip:
        try:
            module_path = inspect.getfile(m)
            if os.path.basename(module_path) == "__init__.py":
                dirpath = Path(module_path).parent.absolute()
                basedir = os.path.dirname(dirpath) + '/' 
                for root, dirs, files in os.walk(dirpath):
                    if os.path.basename(root)[0] == '.':
                        continue #skip hidden directories        
                    dirname = root.replace(basedir, '')
                    for f in files:
                        if f[-1] == '~' or (f[0] == '.' and f != '.htaccess'):
                            #skip backup files and all hidden files except .htaccess
                            continue
                        if f.endswith(".so") or f.endswith(".pyd"):
                            extract = True

                        fzip.write(root + '/' + f, dirname + '/' + f)
            else:
                fzip.write(module_path, os.path.basename(module_path))
                if module_path.endswith(".so") or module_path.endswith(".pyd"):
                    extract = True
        except:
            module_path = m.__path__[0]
            fzip.write(module_path, os.path.basename(module_path))
            if module_path.endswith(".so") or module_path.endswith(".pyd"):
                extract = True

    # Convert to string so can easily serialize
    module_bytes = zip_buffer.getvalue()
    zip_buffer.close()

    # Possible solution for libraries, but seems to be overly inclusive?
    libraries = collect_dynamic_libs(m.__name__)
    if is_pure_conda:
        try:
            libraries.extend(conda_support.collect_dynamic_libs(m.__name__, dependencies=False))
        except ModuleNotFoundError:
            print(f"{m.__name__} is not a conda package or was not installed with conda. Cannot find all shared libraries.")

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as fzip:
        for path, _ in libraries:
            fzip.write(path, os.path.basename(path))

    # Convert to string so can easily serialize
    library_bytes = zip_buffer.getvalue()
    zip_buffer.close()

    print(f"Serialize: {m.__name__}")
    print(f"\tmodule zip file length: {len(module_bytes)}")
    print(f"\tlibrary zip file length: {len(library_bytes)}")

    return {"module": module_bytes, "extract": extract, "libraries": library_bytes}

def load_config(config: Optional[Union[dict[str, Any], str]] = None):
    if config is None or type(config) == str:
        config = read_config(config)
    return config

def create_store_from_config(ps_config: dict[str, Any]) -> Store:
    name = ps_config["name"]
    store = get_store(name)

    if store is None:
        store = Store.from_config(ps_config)
        register_store(store)

    return store

# Create a global cached of proxied modules
proxied_modules = {}
def store_modules(modules: str | list, trace: bool = True, config: Optional[Union[dict[str, Any], str]] = None) -> dict[str, Proxy]:
    """Reads module and proxies it into the FileStore, including the
    dependencies if requested. This is a best effort approach. If a 
    specific submodule is needed, pass that into this function for more
    accurate dependency resolution.

    Args:
        module_name (str): the module to proxy.
        trace (bool): try to determine and include necessary dependents. 
    """
    config = load_config(config)
    store = create_store_from_config(config["module_store_config"])

    if type(modules) != list:
        modules = [modules]

    if trace:
        args = ["import_tracer.py"] + modules
        env = os.environ.copy()
        env["PYTHONPATH"] = f"{sys.path[0]}:{env.get('PYTHONPATH', '')}"
        # Run as a subprocess to collect full depedencies without messing with module cache
        completed = subprocess.run(
                args,
                env=env,
                capture_output=True, 
                text=True
            )
        modules = completed.stdout.split("\n")[:-1]

    results = dict()
    for module_name in modules:
        if module_name not in proxied_modules:
            if module_name in sys.builtin_module_names or module_name in sys.stdlib_module_names:
                print(f"Built in or standard module {module_name} skipped")
                continue

            try:
                module = importlib.import_module(module_name)
            except:
                print(f"Could not import {module_name}, skipping")
                continue
            module_tar = _serialize_module(module)
            proxied_modules[module_name] = store.proxy(module_tar)

        results[module_name] = proxied_modules[module_name]
    return results


def analyze_func_and_create_proxies(func, config: Optional[Union[dict[str, Any], str]] = None):
    config = load_config(config)
    def _strip_dots(pkg):
        if pkg.startswith('.'):
            raise ImportError('On {}, imports from the current module are not supported'.format(pkg))
        return pkg.split('.')[0]

    src = inspect.getsource(func)
    code = ast.parse(src)
    
    # Adapted from: https://github.com/cooperative-computing-lab/cctools/blob/master/poncho/src/poncho/package_analyze.py
    imports = set()
    for stmt in ast.walk(code):
        if isinstance(stmt, ast.Import):
            for a in stmt.names:
                imports.add(_strip_dots(a.name))
        elif isinstance(stmt, ast.ImportFrom):
            if stmt.level != 0:
                raise ImportError('On {}, imports from the current module are not supported'.format(stmt.module or '.'))
            imports.add(_strip_dots(stmt.module))

    func_module = inspect.getmodule(func)
    if func_module and func_module.__name__ != "__main__":
        imports.add(_strip_dots(func_module.__name__))
    
    return store_modules(list(imports), config)
