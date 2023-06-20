import argparse
import functools
import io
import json
import tarfile
import sys
import time
from collections import defaultdict
import importlib
from tqdm import tqdm
import tarfile

import parsl
from parsl_config import make_config_theta

import proxystore as ps
import proxystore.connectors.file
import proxystore.store
from proxystore.serialize import serialize, deserialize

import conda.cli.python_api
from conda.cli.python_api import Commands
import conda_pack

from proxy_imports import analyze_func_and_create_proxies

import tensorflow as tf
import tensorflow_datasets as tfds
import tensorflow_hub as hub


#@parsl.python_app
#def inference(model, index, workers):
#    import tensorflow as tf
#    import tensorflow_datasets as tfds
#    from proxystore.serialize import deserialize
#    import io
#    import tarfile
#    from pathlib import Path

#    print("Imported modules")
#    def deserialize_model(serialized_data):
#        try:
#            Path(f"/dev/shm/mobilenet_model.tmp").touch(exist_ok=False)
#            tar_files = io.BytesIO(deserialize(serialized_data))
#            with tarfile.open(fileobj=tar_files, mode="r|") as f:
#                f.extractall(path="/dev/shm/")
#            Path(f"/dev/shm/mobilenet_model_done.tmp").touch()
#        except FileExistsError as e:
#            while not Path(f"/dev/shm/mobilenet_model_done.tmp").exists():
#                time.sleep(1)
#        
#       # from tensorflow import keras
#        model = keras.models.load_model('/dev/shm/mobilenet_model')
#        return model
#    
#    model.__factory__.deserializer = deserialize_model
#    
#    print("Set model deserializer")
#    def preprocess(image, label, width=160, height=160):
#        image = tf.image.convert_image_dtype(image, tf.float32)
#        image = tf.image.resize(image, [width, height])
#        image = tf.expand_dims(image, 0)
#        return image, label
#    
#    print("Downloading data")
#    splits = tfds.even_splits('train', n=workers, drop_remainder=True)
#    split = splits[index]
#    dataset= tfds.load('tf_flowers', split=split, as_supervised=True)
#    
#    print("Preprocessing Data")
#    dataset = dataset.map(preprocess)
#
#    print("Performing Inference")
#    labels = []
#    for img, label in dataset:
#        labels.append(model.predict(img))

#    return labels

def inference_pretransformed(model, index, workers):
    import tensorflow as tf
    import tensorflow_datasets as tfds
    from proxystore.serialize import deserialize
    import io
    import tarfile
    from pathlib import Path

    print("Imported modules")
    def deserialize_model(serialized_data):
        try:
            Path(f"/dev/shm/mobilenet_model.tmp").touch(exist_ok=False)
            tar_files = io.BytesIO(deserialize(serialized_data))
            with tarfile.open(fileobj=tar_files, mode="r|") as f:
                f.extractall(path="/dev/shm/")
            Path(f"/dev/shm/mobilenet_model_done.tmp").touch()
        except FileExistsError as e:
            while not Path(f"/dev/shm/mobilenet_model_done.tmp").exists():
                time.sleep(1)

        from tensorflow import keras
        model = keras.models.load_model('/dev/shm/mobilenet_model')
        return model

    print("Set model deserializer")
    model.__factory__.deserializer = deserialize_model

    def preprocess(image, label, width=160, height=160):
        image = tf.image.convert_image_dtype(image, tf.float32)
        image = tf.image.resize(image, [width, height])
        image = tf.expand_dims(image, 0)
        return image, label
    
    print("Downloading data")
    splits = tfds.even_splits('train', n=workers, drop_remainder=True)
    split = splits[index]
    dataset= tfds.load('tf_flowers', split=split, as_supervised=True)

    print("Preprocessing data")
    dataset = dataset.map(preprocess)
    
    print("Performing inference")
    labels = []
    for img, label in dataset:
        labels.append(model.predict(img))
    
    return labels

def serialize_model(model):
    model.save("mobilenet_model")
    tar = io.BytesIO()
    with tarfile.open(fileobj=tar, mode="w|") as f:
        f.add("mobilenet_model")

    model_tar = tar.getvalue()
    return serialize(model_tar)

def load_and_proxy_model(argument_store):
    m = tf.keras.Sequential([
        hub.KerasLayer("https://tfhub.dev/google/imagenet/mobilenet_v2_075_160/classification/5")
    ])
    m.build([None, 160, 160, 3])

    m = argument_store.proxy(m, serializer=serialize_model)
    return m

def run_tasks(nworkers, method: str = "file_system") -> dict[str, float|list]:
    if method == "lazy":
        proxy_modules = analyze_func_and_create_proxies(inference_pretransformed)
        inference = parsl.python_app(inference_pretransformed)
        inference = functools.partial(inference, modules = proxy_modules)
    else:
        inference = parsl.python_app(inference_pretransformed)

    # Pass arguments by reference as well
    connector = ps.connectors.file.FileConnector("argument_store")
    store = ps.store.Store("arg_store", connector, cache_size=16)

    model = load_and_proxy_model(store)
    start_time = time.perf_counter()
    tsks = []
    for itsk in range(nworkers):
        future = inference(model, itsk, nworkers)
        tsks.append(future)
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
        "ntasks" : nworkers,
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
    parser.add_argument("--output", default="results.jsonl", help="File to output results")
    parser.add_argument("--run_info", default=None, help="Add additional information to results")
    opts = parser.parse_args()

    # Setup parsl
    print("Making Parsl config")
    config = make_config_theta(opts.nodes, opts.method)
    parsl.load(config)

    # Run tasks
    print("Running tasks")
    results = run_tasks(opts.workers, opts.method)

    # Write results into file
    results["method"] = opts.method
    results["nodes"] = opts.nodes
    results["workers"] = opts.workers

    if opts.run_info is not None:
        run_info = json.loads(opts.run_info)
        for key, value in run_info.items():
            results[key] = value

    with open(opts.output, "a") as fp:
        fp.write(json.dumps(results) + "\n")


if __name__ == "__main__":
    main()
