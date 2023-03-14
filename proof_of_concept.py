import argparse
import faulthandler
import importlib
import inspect
import io
import tarfile
import os
import sys
import time
from collections import defaultdict
from importlib import abc
from pathlib import Path
from tqdm import tqdm

import parsl
from parsl.providers import LocalProvider
from parsl.providers import SlurmProvider

from proxystore.store.file import FileStore
from proxystore.proxy import extract
from proxystore.proxy import Proxy

import conda.cli.python_api
from conda.cli.python_api import Commands
import conda_pack

package_path = "proxied-site-packages"

def proxy_module(module_name: str) -> None:
    """Reads module and proxies it into the FileStore.

    Args:
        q (multiprocessing.Queue): The queue to store the module name
                                   and its proxy into.
    """
    exec(f"import {module_name} as m", globals())

    fs = FileStore("module_store", store_dir="module-store", cache_size=16)
    p = Path(inspect.getfile(m))
    module_dir = p.parent.absolute()

    tar = io.BytesIO()

    with tarfile.open(fileobj=tar, mode="w|") as f:
        f.add(module_dir, arcname=os.path.basename(module_dir))

    # Convert to string so can easily serialize
    module_tar = tar.getvalue()

    tar.close()

    module_proxy = fs.proxy(module_tar)
    return module_proxy

def setup_import(module_name: str, method: str = "file_system") -> dict[str, Proxy]:
    '''
    Create a parsl task that imports the specified module
    Must be done this way instead of inside the task so the code can then
    be analyzed for dependencies.

    '''
    if method == "file_system":
        code = \
            """
@parsl.python_app
def import_module(**kwargs):
    '''Parsl app that imports a module and accesses its name'''
    import time

    tic = time.perf_counter()
    import %s as m
    return time.perf_counter() - tic
""" % (module_name)
        exec(code, globals())
        return dict()

    elif method == "conda_pack":
        conda.cli.python_api.run_command(Commands.CREATE, "-n", "newenv", "python=3.9", module_name)
        conda_pack.pack(name="newenv")
        code = \
            """
@parsl.python_app
def import_module(**kwargs):
    '''Parsl app that imports a module and accesses its name'''

    tic = time.perf_counter()
    import %s as m
    return time.perf_counter() - tic
""" % (module_name)
        exec(code, globals())
        return dict()

    elif method == "lazy":
        # need to pass dictionary of all possible proxied modules
        # as input to ProxyImporter as the import statement cannot
        # pass a Proxy

        proxy = proxy_module(module_name)
        proxied_modules = {}
        proxied_modules[module_name] = proxy
        
        code = \
            """
@parsl.python_app
def import_module(**proxied_modules):
    '''Parsl app that imports a module and accesses its name'''

    import sys
    import importlib
    from importlib import abc
    from pathlib import Path
    import tarfile

    from proxystore.proxy import extract
    from proxystore.proxy import Proxy
    import io

    package_path = %s
    # Adapted from: https://gist.github.com/rmcgibbo/28bcf323ee0a0e482f52339701390f28
    class ProxyImporter(importlib.abc.MetaPathFinder, importlib.abc.Loader):

        _proxied_modules: dict[str, Proxy]
        _in_create_mode: bool

        def __init__(self, proxied_modules: dict[str, Proxy]):
            self._proxied_modules = proxied_modules
            self._in_create_module = False
            sys.path.insert(0, package_path)  # add proxied package path to PYTHONPATH

        def find_module(self, fullname, path=None):
            spec = self.find_spec(fullname, path)
            if spec is None:
                return None
            return spec

        def create_module(self, spec):
            self._in_create_module = True

            from importlib.util import find_spec, module_from_spec

            real_spec = importlib.util.find_spec(spec.name)

            real_module = module_from_spec(real_spec)
            real_spec.loader.exec_module(real_module)

            self._in_create_module = False
            return real_module

        def exec_module(self, module):
            try:
                _ = sys.modules.pop(module.__name__)
            except KeyError:
                log.error(f"module {module} is not in sys.modules", module.__name__)
            sys.modules[module.__name__] = module
            globals()[module.__name__] = module

        def find_spec(self, fullname, path=None, target=None):

            # If a Proxy object, untar the directory and save it on disc
            # NOTE: Python can read tar.gz modules, so maybe we just
            # save the tar archive to disc instead of untarring
            if fullname in self._proxied_modules:
                tar_str = io.BytesIO(extract(self._proxied_modules[fullname]))

                with tarfile.open(fileobj=tar_str, mode="r|") as f:
                    f.extractall(path=package_path)

            if self._in_create_module:
                return None

            spec = importlib.machinery.ModuleSpec(fullname, self)
            return spec

    sys.meta_path.insert(0, ProxyImporter(proxied_modules))

    tic = time.perf_counter()
    import %s as m
    return time.perf_counter() - tic
""" % (package_path, module_name)
        exec(code, globals())

        return proxied_modules

def cleanup(module_name: str, method: str = "file_system") -> None:
    if method == "conda_pack":
        conda.cli.python_api.run_command(Commands.REMOVE, "-n", "newenv", "--all")
        os.remove("newenv.tar.gz")
    elif method == "lazy":
        pass

def make_config(nodes:int = 0, method:str = "file_system"):
    '''
    Build a config for an executor.
    '''
    provider = LocalProvider(worker_init=f"source setup_{method}.sh")
    if nodes > 1:
        provider.launcher = parsl.launchers.SrunLauncher(overrides='-K0 -k --slurmd-debug=verbose')
        provider.nodes_per_block = opts.nnod
    executor = parsl.HighThroughputExecutor(provider=provider)

    config = parsl.config.Config(
       executors=[ executor ],
       strategy=None
    )

    return config

def run_tasks(ntasks: int = 1, proxied_modules: dict[str, Proxy] = None) -> dict :
    start_time = time.perf_counter()
    tsks = []
    for itsk in range(ntasks):
        tsks.append(import_module(**proxied_modules))
    launch_time = time.perf_counter()

    status_counts = defaultdict(int)
    cumulative_time = 0
    tasks_finished = 0
    with tqdm(total=len(tsks)) as t:
        while len(tsks):
            itsk = 0
            while itsk < len(tsks):
                tsk = tsks[itsk]
                if tsk.done():
                    tasks_finished += 1
                    status_counts[tsk.task_status()] += 1
                    if tsk.task_status() == "failed":
                        # Raise the error that was raised during task
                        print(tsk.result())
                    else:
                        cumulative_time += tsk.result()
                        
                    tsks.pop(itsk)
                    t.update(1)
                else:
                    itsk += 1
    finish_time = time.perf_counter()

    results = {
        "ntasks" : ntasks,
        "cumulative_time" : cumulative_time,
        "start_time": start_time,
        "launch_time": launch_time,
        "end_time": finish_time
    }

    return results

def main():
    # Parse command line
    parser = argparse.ArgumentParser()
    parser.add_argument("--ntsks", default=1, type=int, help="Number of tasks")
    parser.add_argument("--output", default="results.jsonl", help="File to output results")
    parser.add_argument("--nodes", default=0, type=int, help="Number of nodes")
    parser.add_argument("--method", default="file_system", choices=["conda_pack", "file_system", "lazy"])
    parser.add_argument("--module", default="numpy", help="Module to import inside of parsl task")
    opts = parser.parse_args()

    # Proxy/create tar for importing
    print("Setting up import task")
    kwargs = setup_import(opts.module, opts.method)

    # Setup parsl
    print("Making Parsl config")
    config = make_config(opts.nodes, opts.method)
    parsl.load(config)

    # Run tasks
    print("Running tasks")
    results = run_tasks(opts.ntsks, kwargs)

    # Cleanup
    print("Cleaning up run")
    cleanup(opts.module, opts.method)

    # Write results into file
    results["method"] = opts.method
    results["module"] = opts.module
    results["nodes"] = opts.nodes
    with open(opts.output, "a") as fp:
        fp.write(json.dumps(results) + "\n")


if __name__ == "__main__":
    main()
