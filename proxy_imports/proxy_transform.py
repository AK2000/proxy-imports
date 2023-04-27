from functools import wraps
from typing import Any

from proxystore.proxy import Proxy
from .proxy_analyze import analyze_func_and_create_proxies
from dill import dumps # Would rather use pickle or parsl, but breaks when decorator is not used.

def proxy_transform(f=None, connector="redis", package_path="/dev/shm/proxied-site-packages"):
    """Transforms a function to extract all the module imports, proxy the necessary modules
    and returns a function that accepts the proxied module, sets up the imports, and 
    and calls the transformed function.
    """
    def decorator(wrapped_func):
        proxies = analyze_func_and_create_proxies(wrapped_func, connector=connector)
        payload = dumps(wrapped_func)

        def wrapped(*args: list[Any], serialized_func: str = payload, proxied_modules: dict[str, Proxy] = proxies, package_path: str = package_path, **kwargs: dict[str, Any]) -> Any:
            import sys
            from dill import loads
            from .proxy_importer import ProxyImporter
            sys.meta_path.insert(0, ProxyImporter(proxied_modules, package_path))
            func = loads(serialized_func)
            return func(*args, **kwargs)

        return wrapped

    if f is None:
        return decorator
    else:
        return decorator(f)
