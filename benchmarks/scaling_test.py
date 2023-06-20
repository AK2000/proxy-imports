import argparse
import faulthandler
import functools
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
import importlib
from pathlib import Path
import tempfile
from tqdm import tqdm

import parsl
from parsl_config import make_config_perlmutter

from proxystore.store.file import FileStore
from proxystore.proxy import extract
from proxystore.proxy import Proxy

import conda.cli.python_api
from conda.cli.python_api import Commands
import conda_pack

from proxy_imports import proxy_transform, analyze_func_and_create_proxies

def setup_import(
            module_name: str, 
            sleep_time: int = 0,
            method: str = "file_system",
            nodes: int = 1
        ) -> None:
    """ Create a parsl task that imports the specified module
    """

    if method == "file_system":
        code = \
            """
@parsl.python_app
def import_module():
    '''Parsl app that imports a module and accesses its name'''
    import time

    tic = time.perf_counter()
    import %s as m
    time.sleep(%d)
    return time.perf_counter() - tic
""" % (module_name, sleep_time)
        exec(code, globals())
        return

    elif method == "conda_pack":
        base_env = os.path.join(os.getcwd(), "base_env")
        conda.cli.python_api.run_command(Commands.CREATE, f"--name=newenv-{nodes}", "--clone", base_env)
        if module_name == "sim_pack":
            conda.cli.python_api.run_command(Commands.RUN, "-n" f"newenv-{nodes}", "pip", "install", "simulated_package/.")
        elif module_name not in ["tensorflow"]:
            conda.cli.python_api.run_command(Commands.INSTALL, "-n" f"newenv-{nodes}", module_name)
        else:
            conda.cli.python_api.run_command(Commands.RUN, "-n" f"newenv-{nodes}", "pip", "install", module_name)
        conda_pack.pack(name=f"newenv-{nodes}", force=True)
        code = \
            """
@parsl.python_app
def import_module():
    '''Parsl app that imports a module and accesses its name'''
    import time

    tic = time.perf_counter()
    import %s as m
    time.sleep(%d)
    return time.perf_counter() - tic
""" % (module_name, sleep_time)
        exec(code, globals()) 
        return

    elif method == "lazy":
        code = \
            """
def import_module():
    '''Parsl app that imports a module and accesses its name'''
    import time

    tic = time.perf_counter()
    import %s as m
    time.sleep(%d)
    m.__wrapped__ # Force resolution of proxy
    return time.perf_counter() - tic
""" % (module_name, sleep_time)
        with tempfile.NamedTemporaryFile(suffix='.py') as tmp:
            tmp.write(code.encode())
            tmp.flush()

            # Now load that file as a module
            spec = importlib.util.spec_from_file_location('tmp', tmp.name)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            proxy_modules = analyze_func_and_create_proxies(module.import_module)
            import_module = parsl.python_app(module.import_module)
            import_module = functools.partial(import_module, modules=proxy_modules)
            globals()["import_module"] = import_module
            
        return

def cleanup(module_name: str, method: str = "file_system", nodes: int = 1) -> None:
    if method == "conda_pack":
        conda.cli.python_api.run_command(Commands.REMOVE, "-n", f"newenv-{nodes}", "--all")
        os.remove(f"newenv-{nodes}.tar.gz")
        shutil.rmtree("/dev/shm/local-envs", ignore_errors=True) # Path where environments are unpacked

def run_tasks(ntasks: int = 1) -> dict[str, float|list]:
    start_time = time.perf_counter()
    tsks = []
    for itsk in range(ntasks):
        tsks.append(import_module())
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
    parser.add_argument("--sleep", default=0, type=int, help="Number of seconds to sleep after import")
    parser.add_argument("--run_info", default=None, help="Add additional information to results")
    opts = parser.parse_args()

    # Proxy/create tar for importing
    print("Setting up import task")
    tic = time.perf_counter()
    setup_import(opts.module, opts.sleep, opts.method, opts.nodes)
    setup_time = time.perf_counter() - tic

    # Setup parsl
    print("Making Parsl config")
    config = make_config_perlmutter(opts.nodes, opts.method)
    parsl.load(config)

    # Run tasks
    print("Running tasks")
    results = run_tasks(opts.ntsks)

    # Cleanup
    print("Cleaning up run")
    cleanup(opts.module, opts.method, opts.nodes)

    # Write results into file
    results["method"] = opts.method
    results["module"] = opts.module
    results["nodes"] = opts.nodes
    results["sleep"] = opts.sleep
    results["setup"] = setup_time

    if opts.run_info is not None:
        run_info = json.loads(opts.run_info)
        for key, value in run_info.items():
            results[key] = value

    with open(opts.output, "a") as fp:
        fp.write(json.dumps(results) + "\n")


if __name__ == "__main__":
    main()
