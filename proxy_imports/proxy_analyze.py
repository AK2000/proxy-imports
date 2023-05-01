import ast
import importlib
import inspect
import io
import os
import subprocess
import sys
import tarfile
from types import ModuleType
from pathlib import Path

from proxystore.proxy import Proxy
from proxystore.store import Store, get_store, register_store
from proxystore.connectors.dim import utils

from PyInstaller.utils.hooks import collect_dynamic_libs, conda_support
from PyInstaller.compat import is_pure_conda

def _serialize_module(m: ModuleType) -> dict[str, bytes]:
    """ Method used to turn module into serialized bitstring"""
    try:
        module_path = inspect.getfile(m)
        if os.path.basename(module_path) == "__init__.py":
            module_path = Path(module_path).parent.absolute()
    except:
        module_path = m.__path__[0]
        
    tar = io.BytesIO()

    with tarfile.open(fileobj=tar, mode="w|") as f:
        f.add(module_path, arcname=os.path.basename(module_path))

    # Convert to string so can easily serialize
    module_tar = tar.getvalue()
    tar.close()

    # Possible solution for libraries, but seems to be overly inclusive?
    libraries = collect_dynamic_libs(m.__name__)
    if is_pure_conda:
        try:
            libraries.extend(conda_support.collect_dynamic_libs(m.__name__, dependencies=False))
        except ModuleNotFoundError:
            print(f"{m.__name__} is not a conda package or was not installed with conda. Cannot find all shared libraries.")

    tar = io.BytesIO()
    with tarfile.open(fileobj=tar, mode="w|") as f:
        for path, _ in libraries:
            f.add(path, arcname=os.path.basename(path))
    # Convert to string so can easily serialize
    library_tar = tar.getvalue()
    tar.close()

    print(f"Serialize: {m.__name__}")
    print(f"\tmodule tar file length: {len(module_tar)}")
    print(f"\tlibrary tar file length: {len(library_tar)}")

    return {"module": module_tar, "libraries": library_tar}

# Create a global cached of proxied modules
proxied_modules = {}
def store_modules(modules: str | list, trace: bool = True, connector: str = "redis") -> dict[str, Proxy]:
    """Reads module and proxies it into the FileStore, including the
    dependencies if requested. This is a best effort approach. If a 
    specific submodule is needed, pass that into this function for more
    accurate dependency resolution.

    Args:
        module_name (str): the module to proxy.
        trace (bool): try to determine and include necessary dependents. 
    """

    store = get_store("module_store")
    if store is None:
        if connector == "file":
            from proxystore.connectors.file import FileConnector
            connector = FileConnector("module-store")
        elif connector == "redis":
            from proxystore.connectors.redis import RedisConnector
            host = utils.get_ip_address("hsn0")
            connector = RedisConnector(host, 6379)
        elif connector == "zmq":
            from proxystore.connectors.dim.zmq import ZeroMQConnector
            connector = ZeroMQConnector("hsn0", 5555)
        elif connector == "multi":
            from proxystore.connectors.redis import RedisConnector
            from proxystore.connectors.file import FileConnector
            from proxystore.connectors.multi import MultiConnector, Policy
            host = utils.get_ip_address("hsn0")
            redis_connector = RedisConnector(host, 6379)
            file_connector = FileConnector("module-store")

            policies = {
                "redis": (redis_connector, Policy(max_size_bytes=1073741824)),
                "file_connector": (file_connector, Policy(min_size_bytes=1073741824))
            }

            connector = MultiConnector(policies)

        store = Store(
            "module_store",
            connector,
            cache_size=16
        )
        register_store(store)

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


def analyze_func_and_create_proxies(func, connector="file"):
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
    
    return store_modules(list(imports), connector=connector)
