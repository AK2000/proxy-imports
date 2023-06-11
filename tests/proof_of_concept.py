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
package_path = "/dev/shm/proxied-site-packages"

def test_func():
    import tensorflow
    tensorflow.__wrapped__
    return "Executed test function"

def get_transformed_function(queue : multiprocessing.Queue):
    proxies = analyze_func_and_create_proxies(test_func)
    queue.put(proxies)
    return

def main():
    q = multiprocessing.Queue()
    p = multiprocessing.Process(target=get_transformed_function, args=(q,))
    p.start()
    proxied_modules = q.get()
    p.join()

    sys.meta_path.insert(0, ProxyImporter(proxied_modules, package_path))
    print(test_func())

if __name__ == "__main__":
    main()
