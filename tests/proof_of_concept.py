import importlib
import inspect
import io
import tarfile
import os
import sys
from importlib import abc
from pathlib import Path
import multiprocessing

from proxy_imports import proxy_transform, analyze_func_and_create_proxies, ProxyImporter
import proxystore as ps
import proxystore.connectors.file
import proxystore.store
from proxystore.serialize import serialize, deserialize

package_path = "proxied-site-packages"

def test_func(model, index, workers):
    import tensorflow as tf
    import tensorflow_datasets as tfds

    def preprocess(image, label, width=160, height=160):
        image = tf.image.convert_image_dtype(image, tf.float32)
        image = tf.image.resize(image, [width, height])
        image = tf.expand_dims(image, 0)
        return image, label
    
    splits = tfds.even_splits('train', n=workers, drop_remainder=True)
    split = splits[index]
    dataset= tfds.load('tf_flowers', split=split, as_supervised=True)
    dataset = dataset.map(preprocess)
    labels = []
    for img, label in dataset.take(1):
        labels.append(model.predict(img))
    
    return labels

def serialize_model(model):
        model.save("mobilenet_model")
        tar = io.BytesIO()
        with tarfile.open(fileobj=tar, mode="w|") as f:
            f.add("mobilenet_model")

        model_tar = tar.getvalue()
        return serialize(model_tar)

def deserialize_model(serialized_data):
    tar_files = io.BytesIO(deserialize(serialized_data))
    with tarfile.open(fileobj=tar_files, mode="r|") as f:
        f.extractall(path="tests/")
    
    from tensorflow import keras
    model = keras.models.load_model('tests/mobilenet_model')
    return model

def load_and_proxy_model(queue):
    import tensorflow as tf
    import tensorflow_datasets as tfds
    import tensorflow_hub as hub

    m = tf.keras.Sequential([
        hub.KerasLayer("https://tfhub.dev/google/imagenet/mobilenet_v2_075_160/classification/5")
    ])
    m.build([None, 160, 160, 3])

    connector = ps.connectors.file.FileConnector("argument_store")
    store = ps.store.Store("arg_store", connector, cache_size=16)
    m = store.proxy(m, serializer=serialize_model, deserializer=deserialize_model)
    queue.put(m)
    print("Model stored and process exiting")
    return

def get_transformed_function(queue : multiprocessing.Queue):
    proxies = analyze_func_and_create_proxies(test_func)
    queue.put(proxies)
    return

def main():
    q = multiprocessing.Queue()
    p = multiprocessing.Process(target=load_and_proxy_model, args=(q,))
    p.start()
    model = q.get()
    print("Model recieved.")
    p.join()

    q = multiprocessing.Queue()
    p = multiprocessing.Process(target=get_transformed_function, args=(q,))
    p.start()
    proxied_modules = q.get()
    p.join()

    sys.meta_path.insert(0, ProxyImporter(proxied_modules, package_path))
    print(test_func(model, 0, 1000))

if __name__ == "__main__":
    main()
