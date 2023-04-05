import ast
from functools import wraps
import inspect
import sys
from typing import Any

from proxystore.proxy import Proxy
from .proxy_importer import store_modules

def _strip_dots(pkg):
    if pkg.startswith('.'):
        raise ImportError('On {}, imports from the current module are not supported'.format(pkg))
    return pkg.split('.')[0]

def proxy_transform(f=None, connector="redis", package_path="/dev/shm/proxied-site-packages"):
    """Transforms a function to extract all the module imports, proxy the necessary modules
    and returns a function that accepts the proxied module, sets up the imports, and 
    and calls the transformed function.
    """

    def decorator(func):
        src = inspect.getsource(func)
        code = ast.parse(src)
        
        # Adapted from: https://github.com/cooperative-computing-lab/cctools/blob/master/poncho/src/poncho/package_analyze.py
        imports = set()
        for stmt in ast.walk(code):
            if isinstance(stmt, ast.Import):
                for a in stmt.names:
                    imports.add(_strip_dots(a.name))
            elif isinstance(stmt, ast.ImportFrom):
                if stmt.level != 0:
                    raise ImportError('On {}, imports from the current module are not supported'.format(stmt.module or '.'))
                imports.add(_strip_dots(stmt.module))
        
        proxies = store_modules(list(imports), connector=connector)
        @wraps(func)
        def wrapped(*args: list[Any], proxied_modules: dict[str, Proxy] = proxies, package_path: str = package_path, **kwargs: dict[str, Any]) -> Any:
            import sys
            #from .proxy_importer import ProxyImporter
            sys.meta_path.insert(0, ProxyImporter(proxied_modules, package_path))
            return func(*args, **kwargs)

        return wrapped

    if f is None:
        return decorator
    else:
        return decorator(f)
