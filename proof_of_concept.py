import importlib
import inspect
import io
import tarfile
import os
import sys
from pathlib import Path

from proxystore.store.file import FileStore
from proxystore.proxy import extract
from proxystore.proxy import Proxy

package_path = "proxied-site-packages"


def proxy_module() -> Proxy:
    """Reads module and proxies it into the FileStore

    Returns:
        (Proxy) a proxied version of the module
    """
    import numpy as np

    fs = FileStore("module_store", store_dir="module-store", cache_size=16)
    p = Path(inspect.getfile(np))
    module_dir = p.parent.absolute()

    tar = io.BytesIO()

    with tarfile.open(fileobj=tar, mode="w|") as f:
        f.add(module_dir, arcname=os.path.basename(module_dir))

    # Convert to string so can easily serialize
    np_tar = tar.getvalue()

    tar.close()

    np_proxy = fs.proxy(np_tar)
    return np_proxy


def extract_proxied_mod(np_proxy: Proxy):
    np_tar = io.BytesIO(extract(np_proxy))

    # TODO: evict the proxy from the FileStore

    with tarfile.open(fileobj=np_tar, mode="r|") as f:
        f.extractall(path=package_path)


def import_module():
    sys.path.insert(0, package_path)  # add proxied package path to PYTHONPATH
    import numpy as proxynp  # import proxied module and use

    importlib.reload(
        proxynp
    )  # reload to as numpy was already loaded into memory by :`proxy_module`:
    assert package_path in inspect.getfile(proxynp), inspect.getfile(
        proxynp
    )  # make sure correct numpy is being used
    arr = proxynp.random.rand(5)
    print(arr)


def main():
    mod = proxy_module()
    extract_proxied_mod(mod)
    import_module()


if __name__ == "__main__":
    main()
