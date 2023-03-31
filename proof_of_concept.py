import importlib
import inspect
import io
import tarfile
import os
import sys
from importlib import abc
from pathlib import Path

import multiprocessing

from proxy_imports import proxy_transform

package_path = "proxied-site-packages"

@proxy_transform(package_path=package_path, connector="file")
def import_module() -> None:
    """Imports the desired proxied module and performs desired computation."""
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
    sys.modules.pop("numpy")
    p = multiprocessing.Process(target=import_module)
    p.start()
    p.join()

if __name__ == "__main__":
    main()
