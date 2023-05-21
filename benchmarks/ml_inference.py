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
import importlib
from pathlib import Path
import tempfile
from tqdm import tqdm

import parsl
from parsl.providers import LocalProvider
from parsl.providers import SlurmProvider

import proxystore as ps
import proxystore.connectors.file
import proxystore.store

import conda.cli.python_api
from conda.cli.python_api import Commands
import conda_pack

from proxy_imports import proxy_transform

from transformers import AutoTokenizer, TFAutoModelForSequenceClassification
from datasets import load_dataset


@parsl.python_app
def inference(model_name, dataset_name, start, end):
    import transformers
    from datasets import load_dataset
    from transformers.pipelines.base import KeyDataset
    data = datasets.load_dataset(dataset_name, split=f"test[{start}%:{end}%]")

    pipe = transformers.pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)
    results = [out for out in pipe(transformers.pipelines.base.KeyDataset(data, "text"), batch_size=8, truncation="only_first")]
    return results

@parsl.python_app
@proxy_transform
def inference_transformed(model_name, dataset_name, start, end):
    import transformers
    from datasets import load_dataset
    from transformers.pipelines.base import KeyDataset
    data = datasets.load_dataset(dataset_name, split=f"test[{start}%:{end}%]")

    pipe = transformers.pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)
    results = [out for out in pipe(transformers.pipelines.base.KeyDataset(data, "text"), batch_size=8, truncation="only_first")]
    return results
    
def cleanup(module_name: str, method: str = "file_system", nodes: int = 1) -> None:
    if method == "conda_pack":
        conda.cli.python_api.run_command(Commands.REMOVE, "-n", f"newenv-{nodes}", "--all")
        os.remove(f"newenv-{nodes}.tar.gz")
        shutil.rmtree("/dev/shm/local-envs", ignore_errors=True) # Path where environments are unpacked

def make_config(nodes: int = 0, method: str = "file_system") -> parsl.config.Config:
    '''
    Build a config for an executor.
    '''
    provider = LocalProvider(worker_init=f"source setup_scripts/setup_{method}.sh")
    if nodes > 1:
        provider.launcher = parsl.launchers.SrunLauncher(overrides='-K0 -k')
        provider.nodes_per_block = nodes
    executor = parsl.HighThroughputExecutor(provider=provider)

    config = parsl.config.Config(
       executors=[ executor ],
       strategy=None
    )

    return config

def run_tasks(nworkers, model_name, dataset_name, method: str = "file_system") -> dict[str, float|list]:
    #model = TFAutoModelForSequenceClassification.from_pretrained(model_name)
    #tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

    # Pass arguments by reference as well
    connector = ps.connectors.file.FileConnector("argument_store")
    store = ps.store.Store("arg_store", connector, cache_size=16)
    #model = store.proxy(model)
    #tokenizer = store.proxy(tokenizer)

    chunk_size = 100 // nworkers
    chunk_start = 0

    start_time = time.perf_counter()
    tsks = []
    for itsk in range(nworkers):
        if not method == "lazy":
            future = inference(model_name, dataset_name, chunk_start, chunk_start + chunk_size)
        else:
            future = inference_transformed(model_name, tokenizer, dataset_name, chunk_start, chunk_start + chunk_size)
        
        tsks.append(future)
        chunk_start += chunk_size
    launch_time = time.perf_counter() - start_time

    status_counts = defaultdict(int)
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
                        
                    tsks.pop(itsk)
                    t.update(1)
                else:
                    itsk += 1
    finish_time = time.perf_counter() - start_time

    results = {
        "ntasks" : ntasks,
        "launch_time": launch_time,
        "end_time": finish_time
    }

    return results

def main():
    # Parse command line
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", default=1, type=int, help="Number of tasks")
    parser.add_argument("--nodes", default=0, type=int, help="Number of nodes")
    parser.add_argument("--method", default="file_system", choices=["conda_pack", "file_system", "lazy"])
    parser.add_argument("--dataset", default="imdb")
    parser.add_argument("--model", default="distilbert-base-uncased-finetuned-sst-2-english")
    parser.add_argument("--output", default="results.jsonl", help="File to output results")
    parser.add_argument("--run_info", default=None, help="Add additional information to results")
    opts = parser.parse_args()

    # Setup parsl
    print("Making Parsl config")
    config = make_config(opts.nodes, opts.method)
    parsl.load(config)

    # Run tasks
    print("Running tasks")
    results = run_tasks(opts.workers, opts.model, opts.dataset, opts.method)

    # Cleanup
    print("Cleaning up run")
    cleanup(opts.module, opts.method, opts.nodes)

    # Write results into file
    results["method"] = opts.method
    results["module"] = opts.module
    results["nodes"] = opts.nodes_per_block
    results["workers"] = opts.workers

    if opts.run_info is not None:
        run_info = json.loads(opts.run_info)
        for key, value in run_info.items():
            results[key] = value

    with open(opts.output, "a") as fp:
        fp.write(json.dumps(results) + "\n")


if __name__ == "__main__":
    main()
