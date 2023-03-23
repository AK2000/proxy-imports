import importlib
import inspect
import io
import tarfile
import os
import sys
from importlib import abc
from pathlib import Path

import multiprocessing

from proxy_importer import ProxyImporter, store_module

package_path = "proxied-site-packages"


def get_proxy_module(q: multiprocessing.Queue) -> None:
    """Reads module and proxies it into the FileStore.
    Args:
        q (multiprocessing.Queue): The queue to store the module name
                                   and its proxy into.
    """
    proxied_modules = store_module("numpy", trace=True)
    q.put(proxied_modules)
    print("Put proxy module onto queue")


def import_module() -> None:
    """Imports the desired proxied module and performs desired computation."""
    # import numpy.linalg as la
    import numpy as proxynp  # import proxied module and use
    from numpy.linalg import solve

    print("Imported library.")
    assert package_path in inspect.getfile(proxynp), inspect.getfile(
        proxynp
    )  # make sure correct numpy is being used

    A = proxynp.random.rand(5,5)
    b = proxynp.random.rand(5)
    arr = solve(A, b)
    print(arr)


def main():
    # Using multiprocessing here so :py:func:`import_module` does not
    # have a preloaded copy of the original module
    multiprocessing.set_start_method('spawn')
    q = multiprocessing.Queue()
    p = multiprocessing.Process(target=get_proxy_module, args=(q,))
    p.start()
    print("Getting value from queue")
    proxied_modules = q.get()
    print("Got value from queue")
    p.join()


    sys.meta_path.insert(0, ProxyImporter(proxied_modules, package_path))
    import_module()


if __name__ == "__main__":
    main()
