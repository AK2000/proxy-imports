"""Implementation of lazy importing ad moving via proxies"""
import ast
from threading import Thread
import sys
import importlib
from importlib import abc
from importlib._bootstrap import _ModuleLockManager
from importlib.util import module_from_spec
import inspect
import io
import os
from pathlib import Path
import tarfile
from types import ModuleType
import time
import zipfile
import zipimport
import asyncio
from asyncio import Future

from proxystore.proxy import Proxy, resolve, is_resolved
from proxystore.store import Store, get_store, register_store
from proxystore.serialize import deserialize

import lazy_object_proxy.slots as lop

class ProxyModule(lop.Proxy):
    """ Wraps a proxy of a tar of a module to behave like the proxy of a module
    Adds the necessary features to avoid resolving the module unnecessarily.
    """

    def __init__(self, file_future: Future, name: str, package_path: str):
        object.__setattr__(self, '__path__', None)
        object.__setattr__(self, 'name', None)
        object.__setattr__(self, "package_path", package_path)
        object.__setattr__(self, "submodules", dict())

        package, _, submod = name.partition('.')
        if submod:
            # Parent must be imported seperately, so we don't have to set the deserializer
            object.__setattr__(self, '__factory__', lambda: self.load_module(name, package))
        else:
            # Is a package
            object.__setattr__(self, "file_unpack", file_future)
            object.__setattr__(self, '__factory__', lambda: self.load_package(name))

    @property
    def __name__(self):
        """ __name__ property needed to overide lop.Proxy"""
        return self.name

    @__name__.setter
    def __name__(self, value):
        """ __name__ setter needed to overide lop.Proxy"""
        object.__setattr__(self, "name", value)

    def __setattr__(self, name, value, __setattr__=object.__setattr__):
        """ Overrides the __setattr__ function of lop.Proxy to avoid resolving the module
        while the metadata is being initialized
        """
        if self.__resolved__:
            super().__setattr__(name, value, __setattr__)
        elif isinstance(value, lop.Proxy):
            __setattr__(self, name, value)
            self.submodules[name] = value
        elif name in ["__name__", "__loader__", "__package__", "__spec__", "__path__", "__file__", "__cached__"]:
            __setattr__(self, name, value)
        else:
            super().__setattr__(name, value, __setattr__)

    def __getattr__(self, name):
        """ Overrides the __getattr__ function to avoid resolving the proxy at all on get"""
        if self.__resolved__:
            return super().__getattr__(name)
        
        if name in ["__name__", "__loader__", "__package__", "__spec__", "__path__", "__cached__"]:
            return object.__getattribute__(self, name)
        return super().__getattr__(name)
    
    def load_package(self, name: str):
        """Factory method for a package"""
        self.file_unpack.result()
        
        if os.path.isfile(f"{self.package_path}/{name}/__init__.py"):
            module_path = f"{self.package_path}/{name}/__init__.py"
            loader = importlib.machinery.SourceFileLoader(name, module_path)
            spec = importlib.util.spec_from_loader(name, loader)
        elif os.path.isfile(f"{self.package_path}/{name}.py"):
            module_path = f"{self.package_path}/{name}.py"
            loader = importlib.machinery.SourceFileLoader(name, module_path)
            spec = importlib.util.spec_from_loader(name, loader)
        elif len(list(Path(f"{self.package_path}/").glob(f"{name}.*.so"))) > 0:
            module_path = str(next(Path(f"{self.package_path}/").glob(f"{name}.*.so")))
            loader = importlib.machinery.ExtensionFileLoader(name, module_path)
            spec = importlib.util.spec_from_loader(name, loader)
        else:
            raise ModuleNotFoundError(f"Could not find file for module {name}")

        # FIXME: There seems to be some weirdness around trying to avoid this in the LazyLoader
        # I can't seem to find anything that talks about this, and I don't know where it is 
        # documented. This seems to work, so I'm not going to worry about it too much
        with _ModuleLockManager(spec.name):
            spec._initializing = True
            module = module_from_spec(spec)
            sys.modules[module.__name__] = module

            self.remove_submodules()
            spec.loader.exec_module(module)
            self.add_submodules(module)

            spec._initializing = False

        globals()[module.__name__] = module
        return module

    def remove_submodules(self):
        """Remove submodules from sys.modules. Since this is a ProxyModule, all
        must also be proxy modules. If they are called while loading the parent module
        they will try to resolve the parent module before executing, causing an infinite
        recursion. If we remove them from sys.module, they can only be accessed as a attribute
        of the proxy. Reimporting them will result in the actual module being returned.
        """
        for name, submod in self.submodules.items():
            sys.modules.pop(submod.__name__)
            submod.remove_submodules()

    def add_submodules(self, module: ModuleType):
        """Add submodules of the proxy as attributes of actual module
        For the case where you 
            (1) Resolve a module
            (2) Access without the proxy
            (3) Access a previously imported (proxied) submodule (w/o) reimporting the submodule
        
        i.e:

        ```
        import scipy as sp # Proxy Module
        import scipy.sparse # Proxy module as well
        resolve(sp)
        ...
        import scipy as sp # Not a proxy anymore
        sp.sparse # Module pointing to a proxy
        ```
        """
        for name, submod in self.submodules.items():
            setattr(module, name, submod)

    def load_module(self, name: str, package_name: str) -> ModuleType:
        """Factory method for a module which is not a package"""
        package = importlib.import_module(package_name) # In sys.modules, so should be fast
        if isinstance(package, ProxyModule):
            package.__wrapped__ # Force resolution of package
        
        # This has been removed from sys.modules, so it is safe to call this
        # Just import the module again, the parent is already resolved
        # So this should point to the correct thing without a problem
        module = importlib.import_module(name)
        self.add_submodules(module)
        return module


async def unpack(proxy: Proxy, name: str, package_path: str) -> None:
    """Unpacks the tar file into the correct place"""
    def deserialize_and_untar(b: bytes):
        zip_files = deserialize(b)

        module_bytes = zip_files["module"]
        module_buffer = io.BytesIO(module_bytes)
        with tarfile.open(fileobj=module_buffer, mode="r") as f:
            f.extractall(path=package_path)

        library_buffer = io.BytesIO(zip_files["libraries"])
        library_path = os.path.join(package_path, "libraries")
        with tarfile.open(fileobj=library_buffer, mode="r|") as f:
            for file_ in f:
                try:
                    f.extract(file_, path=library_path)
                except IOError as e:
                    pass

        return "Done"

    started_file = Path(f"{package_path}/{name}.tmp")
    finished_file = Path(f"{package_path}/{name}_done.tmp")
    try:
        # Prevent multiple tasks from extracting proxy
        started_file.touch(exist_ok=False)
        proxy.__factory__.deserializer = deserialize_and_untar
        resolve(proxy)
        finished_file.touch()
    except FileExistsError as e:
        # Wait for package to finish extracting before continuing
        while (not finished_file.exists()):
            await asyncio.sleep(0.2)
    
    return "Done"

async def stop_loop(futures, loop):
    for f in futures:
        await asyncio.wrap_future(f)
    loop.stop()

# Adapted from: https://gist.github.com/rmcgibbo/28bcf323ee0a0e482f52339701390f28
class ProxyImporter(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    _proxied_modules: dict[str, Proxy]

    def __init__(self, proxied_modules: dict[str, Proxy], package_path: str):
        Path(package_path).mkdir(parents=True, exist_ok=True)
        sys.path.insert(0, package_path)
        self.package_path = package_path

        # Path for shared libraries, must be added to LD_LIBRARY_PATH at startup
        # This can't be done from inside python because the environment has already
        # been cached in the linker by this point
        library_path = os.path.join(package_path, "libraries")
        Path(library_path).mkdir(exist_ok=True)

        self.loop  = asyncio.new_event_loop()
        def run_loop(loop):
            asyncio.set_event_loop(loop)
            loop.run_forever()
        self.unpack_thread = Thread(target=run_loop, args=(self.loop,))
        self.unpack_thread.start()

        futures = dict()
        for name, proxy in proxied_modules.items():
            futures[name] = asyncio.run_coroutine_threadsafe(unpack(proxy, name, package_path), self.loop)
        self.end = asyncio.run_coroutine_threadsafe(stop_loop(list(futures.values()), self.loop), self.loop)
        self._proxied_modules = futures     

    def find_module(self, fullname, path=None):
        spec = self.find_spec(fullname, path)
        if spec is None:
            return None
        return spec

    def create_module(self, spec):
        package, _, submod = spec.name.partition('.')            

        if package in self._proxied_modules:
            proxy = self._proxied_modules[package]
            proxy = ProxyModule(proxy, spec.name, self.package_path)
            importlib._bootstrap._init_module_attrs(spec, proxy, override=True)
            self._in_create_module = False
            return proxy

        return None

    def exec_module(self, module):
        try:
            _ = sys.modules.pop(module.__name__)
        except KeyError:
            log.error(f"module {module} is not in sys.modules", module.__namle__)
        sys.modules[module.__name__] = module
        globals()[module.__name__] = module

    def find_spec(self, fullname, path=None, target=None):
        if path is not None:
            return None

        package, _, submod = fullname.partition('.')
        if package in self._proxied_modules:
            spec = importlib.machinery.ModuleSpec(fullname, self)
            return spec

        return None
