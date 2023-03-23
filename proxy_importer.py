import sys
import importlib
from importlib import abc
import inspect
import io
import os
from pathlib import Path
import tarfile
from types import ModuleType
import time

from proxystore.proxy import Proxy, extract, is_resolved
from proxystore.store import Store, get_store, register_store
from proxystore.connectors.file import FileConnector
from proxystore.store.utils import resolve_async

class ProxyModule(Proxy):
    def __init__(self, _proxy: Proxy, name: str, package_path: str):
        object.__setattr__(self, '_proxy', _proxy)
        object.__setattr__(self, '_name', None)
        object.__setattr__(self, '__path__', None)
        object.__setattr__(self, "package_path", package_path)
        object.__setattr__(self, "submodules", dict())

        package, _, submod = name.partition('.')
        if submod:
            # Parent must be imported seperately, so we don't have to set the deserializer
            object.__setattr__(self, '__factory__', lambda: self.load_module(name))
        else:
            # Is a package
            _proxy.__factory__.deserializer =  lambda b : self.unpack(b, name)
            resolve_async(_proxy)
            object.__setattr__(self, '__factory__', lambda: self.load_package(name))

    @property
    def __name__(self):
        return self._name

    @__name__.setter
    def __name__(self, value):
        object.__setattr__(self, "_name", value)

    def __setattr__(self, name, value, __setattr__=object.__setattr__):
        print("Setting attribute:", name)
        if self.__resolved__:
            super().__setattr__(name, value, __setattr__)
        elif isinstance(value, Proxy):
            __setattr__(self, name, value)
            self.submodules[name] = value
        elif name in ["__name__", "__loader__", "__package__", "__spec__", "__path__", "__file__", "__cached__"]:
            __setattr__(self, name, value)
        else:
            super().__setattr__(name, value, __setattr__)

    def __getattr__(self, name):
        if self.__resolved__:
            return super().__getattr__(name)
        
        if name in ["__name__", "__loader__", "__package__", "__spec__", "__path__", "__file__", "__cached__"]:
            return object.__getattribute__(self, name)

        # Don't resolve proxy when getting an attribute, instead return proxy
        # This  handles "from (proxied module) import x" statements lazily
        print("Creating proxy for:", name)
        def resolve_attr():
            extract(self)
            return getattr(self, name)
        return Proxy(resolve_attr)

    def unpack(self, b: bytes, name: str) -> None:
        print("Load package called")
        try: # TODO: Make this more robust, i.e. to a process failure
            # Prevent multiple tasks from extracting proxy
            Path(f"{self.package_path}/{name}.tmp").touch(exist_ok=False)
            tar_str = io.BytesIO(b)
            with tarfile.open(fileobj=tar_str, mode="r|") as f:
                f.extractall(path=self.package_path)
            Path(f"{self.package_path}/{name}_done.tmp").touch()

        except FileExistsError:
            # Wait for package to finish extracting before continuing?
            while True:
                try:
                    with open(f"{self.package_path}/{name}_done.tmp", "r") as _:
                        break
                except IOError:
                    time.sleep(1)
        return 1 # This value should(?) not be used, must return some True value
    
    def load_package(self, name: str):
        extract(self._proxy) # Ensure module is resolved

        from importlib.util import module_from_spec
        loader = importlib.machinery.SourceFileLoader(name, f"{self.package_path}/{name}/__init__.py")
        real_spec = importlib.util.spec_from_loader(name, loader)
        real_module = module_from_spec(real_spec)

        sys.modules[real_module.__name__] = real_module # TODO: Not sure if this is thread safe
        self.remove_submodules() # TODO: Really not sure if this is thread safe 
        importlib.reload(real_module)

        globals()[real_module.__name__] = real_module
        return real_module

    def remove_submodules(self):
        for name, submod in self.submodules.items():
            if isinstance(submod, ProxyModule):
                sys.modules.pop(submod.__name__)
                submod.remove_submodules()

    def load_module(self, name: str) -> ModuleType:
        print("Load module called")
        extract(self._proxy)
        
        # This has been removed from sys.modules, so it is safe to call this
        module = importlib.import_module(name)
        return module

# Adapted from: https://gist.github.com/rmcgibbo/28bcf323ee0a0e482f52339701390f28
class ProxyImporter(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    _proxied_modules: dict[str, Proxy]
    _in_create_module: bool

    def __init__(self, proxied_modules: dict[str, Proxy], package_path: str):
        self._proxied_modules = proxied_modules
        self._in_create_module = False
        self._in_load_proxy = False
        
        Path(package_path).mkdir(parents=True, exist_ok=True)
        sys.path.insert(0, package_path)
        self.package_path = package_path

    def find_module(self, fullname, path=None):
        spec = self.find_spec(fullname, path)
        if spec is None:
            return None
        return spec

    def create_module(self, spec):
        self._in_create_module = True
        print("Create module called", spec.name)

        from importlib.util import find_spec
        package, _, submod = spec.name.partition('.')
        if submod:
            real_spec = spec
        else:
            real_spec = importlib.util.find_spec(spec.name)
            

        if package in self._proxied_modules:
            proxy = self._proxied_modules[package]
            proxy = ProxyModule(proxy, real_spec.name, self.package_path)
            importlib._bootstrap._init_module_attrs(spec, proxy, override=True)
            self._in_create_module = False
            return proxy            
        
        self._in_create_module = False
        return None

    def exec_module(self, module):
        print("Exec module called")
        try:
            _ = sys.modules.pop(module.__name__)
        except KeyError:
            log.error(f"module {module} is not in sys.modules", module.__namle__)
        sys.modules[module.__name__] = module
        globals()[module.__name__] = module

    def find_spec(self, fullname, path=None, target=None):
        print(f"Find spec called for module {fullname}")
        if self._in_create_module:
            return None

        package, _, submod = fullname.partition('.')
        if os.path.exists(f"{self.package_path}/{package}_done.tmp"):
            # If the package already exists in the desired location, import it
            return None

        if package in self._proxied_modules:
            spec = importlib.machinery.ModuleSpec(fullname, self)
            return spec

        return None

def store_module(module_name: str) -> Proxy:
    """Reads module and proxies it into the FileStore.

    Args:
        module_name (str): the module to proxy
    """

    store = get_store("module_store")
    if store is None:
        store = Store(
            "module_store",
            FileConnector("module-store"),
            cache_size=16
        )
        register_store(store)

    def serialize(m: ModuleType) -> bytes:
        p = Path(inspect.getfile(m))
        module_dir = p.parent.absolute()

        tar = io.BytesIO()

        with tarfile.open(fileobj=tar, mode="w|") as f:
            f.add(module_dir, arcname=os.path.basename(module_dir))

        # Convert to string so can easily serialize
        module_tar = tar.getvalue()
        tar.close()

        print("Created tar file, length: ", len(module_tar))
        return module_tar

    module = importlib.import_module(module_name)
    module_proxy = store.proxy(module, serializer=serialize)
    return module_proxy