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
    np_proxy = store_module("numpy")
    q.put(("numpy", np_proxy))
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
    proxied_modules = {}

    # Using multiprocessing here so :py:func:`import_module` does not
    # have a preloaded copy of the original module
    q = multiprocessing.Queue()
    p = multiprocessing.Process(target=get_proxy_module, args=(q,))
    p.start()
    print("Getting value from queue")
    key, value = q.get()
    print("Got value from queue")
    p.join()

    # need to pass dictionary of all possible proxied modules
    # as input to ProxyImporter as the import statement cannot
    # pass a Proxy
    proxied_modules[key] = value
    sys.meta_path.insert(0, ProxyImporter(proxied_modules, package_path))

    import_module()


if __name__ == "__main__":
    main()
