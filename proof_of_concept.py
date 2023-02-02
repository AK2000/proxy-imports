import importlib
import inspect
import io
import tarfile
import os
import sys
from importlib import abc
from pathlib import Path

import multiprocessing

from proxystore.store.file import FileStore
from proxystore.proxy import extract
from proxystore.proxy import Proxy

package_path = "proxied-site-packages"

# Adapted from: https://gist.github.com/rmcgibbo/28bcf323ee0a0e482f52339701390f28
class ProxyImporter(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def __init__(self, proxied_modules: dict[str, Proxy]):
        self._proxied_modules = proxied_modules
        self._in_create_module = False
        sys.path.insert(0, package_path)  # add proxied package path to PYTHONPATH

    def find_module(self, fullname, path=None):
        spec = self.find_spec(fullname, path)
        if spec is None:
            return None
        return spec

    def create_module(self, spec):
        self._in_create_module = True

        from importlib.util import find_spec, module_from_spec
        real_spec = importlib.util.find_spec(spec.name)

        real_module = module_from_spec(real_spec)
        real_spec.loader.exec_module(real_module)

        self._in_create_module = False
        return real_module

    def exec_module(self, module):
        try:
            _ = sys.modules.pop(module.__name__)
        except KeyError:
            log.error("module %s is not in sys.modules", module.__name__)
        sys.modules[module.__name__] = module
        globals()[module.__name__] = module

    def find_spec(self, fullname, path=None, target=None):
        if fullname in self._proxied_modules:
            tar_str = io.BytesIO(extract(self._proxied_modules[fullname]))

            with tarfile.open(fileobj=tar_str, mode="r|") as f:
                f.extractall(path=package_path)

        if self._in_create_module:
            return None

        spec = importlib.machinery.ModuleSpec(fullname, self)
        return spec


def proxy_module(q: multiprocessing.Queue) -> None:
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
    q.put(('numpy', np_proxy))


def extract_proxied_mod(np_proxy: Proxy):
    pass

def import_module():
    import numpy as proxynp  # import proxied module and use

    assert package_path in inspect.getfile(proxynp), inspect.getfile(
        proxynp
    )  # make sure correct numpy is being used
    arr = proxynp.random.rand(5)
    print(arr)


def main():
    proxied_modules = {}
    
    q = multiprocessing.Queue()
    p = multiprocessing.Process(target=proxy_module, args=(q,))
    p.start()
    key, value = q.get()
    p.join()
    proxied_modules[key] = value

    sys.meta_path.insert(0, ProxyImporter(proxied_modules))

    #extract_proxied_mod(mod)
    import_module()


if __name__ == "__main__":
    main()
