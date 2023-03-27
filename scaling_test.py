import argparse
import faulthandler
import importlib
import inspect
import io
import json
import tarfile
import os
import shutil
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

from proxy_importer import ProxyImporter, store_module

package_path = "/dev/shm/proxied-site-packages"

def setup_import(module_name: str, method: str = "file_system", nodes: int = 1) -> dict[str, Proxy]:
    """ Create a parsl task that imports the specified module
    """

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
        base_env = os.path.join(os.getcwd(), "base_env")
        conda.cli.python_api.run_command(Commands.CREATE, f"--name=newenv-{nodes}", "--clone", base_env)
        if module_name not in ["tensorflow"]:
            conda.cli.python_api.run_command(Commands.INSTALL, "-n" f"newenv-{nodes}", module_name)
        else:
            conda.cli.python_api.run_command(Commands.RUN, "-n" f"newenv-{nodes}", "pip", "install", module_name)
        conda_pack.pack(name=f"newenv-{nodes}")
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

        proxied_modules = store_module(module_name, True)
        
        code = \
            """
@parsl.python_app
def import_module(**proxied_modules):
    '''Parsl app that imports a module and accesses its name'''
    import sys
    import os
    sys.path.insert(0, os.getcwd())
    from proxy_importer import ProxyImporter
    sys.meta_path.insert(0, ProxyImporter(proxied_modules, "%s"))

    tic = time.perf_counter()
    import %s as m
    return time.perf_counter() - tic
""" % (package_path, module_name)
        exec(code, globals())

        return proxied_modules

def cleanup(module_name: str, method: str = "file_system", nodes: int = 1) -> None:
    if method == "conda_pack":
        conda.cli.python_api.run_command(Commands.REMOVE, "-n", f"newenv-{nodes}", "--all")
        os.remove(f"newenv-{nodes}.tar.gz")
        shutil.rmtree("/dev/shm/local-envs", ignore_errors=True) # Path where environments are unpacked
    elif method == "lazy":
        shutil.rmtree(f"{package_path}", ignore_errors=True)

def make_config(nodes: int = 0, method: str = "file_system") -> parsl.config.Config:
    '''
    Build a config for an executor.
    '''
    provider = LocalProvider(worker_init=f"source setup_scripts/setup_{method}.sh")
    if nodes > 1:
        provider.launcher = parsl.launchers.SrunLauncher(overrides='-K0 -k --slurmd-debug=verbose')
        provider.nodes_per_block = nodes
    executor = parsl.HighThroughputExecutor(provider=provider)

    config = parsl.config.Config(
       executors=[ executor ],
       strategy=None
    )

    return config

def run_tasks(ntasks: int = 1, proxied_modules: dict[str, Proxy] = None) -> dict[str, float|list]:
    start_time = time.perf_counter()
    tsks = []
    for itsk in range(ntasks):
        tsks.append(import_module(**proxied_modules))
    launch_time = time.perf_counter() - start_time

    status_counts = defaultdict(int)
    cumulative_time = 0
    times = []
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
                        tsk_time = tsk.result()
                        times.append(tsk_time)
                        cumulative_time += tsk_time
                        
                    tsks.pop(itsk)
                    t.update(1)
                else:
                    itsk += 1
    finish_time = time.perf_counter() - start_time

    results = {
        "ntasks" : ntasks,
        "cumulative_time" : cumulative_time,
        "times" : times,
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
    tic = time.perf_counter()
    kwargs = setup_import(opts.module, opts.method, opts.nodes)
    setup_time = time.perf_counter() - tic

    # Setup parsl
    print("Making Parsl config")
    config = make_config(opts.nodes, opts.method)
    parsl.load(config)

    # Run tasks
    print("Running tasks")
    results = run_tasks(opts.ntsks, kwargs)

    # Cleanup
    print("Cleaning up run")
    cleanup(opts.module, opts.method, opts.nodes)

    # Write results into file
    results["method"] = opts.method
    results["module"] = opts.module
    results["nodes"] = opts.nodes
    results["setup"] = setup_time
    with open(opts.output, "a") as fp:
        fp.write(json.dumps(results) + "\n")


if __name__ == "__main__":
    main()
