import argparse
import sys
import importlib
from importlib import abc
import os

class TracingFinder(importlib.abc.MetaPathFinder):
    """ Finder to trace the imports of a module.

    Modules already appearing in sys.modules will not appear here.
    This may (?) be desirable behavior if we assume modules not 
    part of this trace will either be part of the base environment, or have
    been imported by another module that was traced and proxied
    """
    
    _packages: set[str]

    def __init__(self):
        self._packages = set()

    def find_module(self, fullname, path=None):
        spec = self.find_spec(fullname, path)
        if spec is None:
            return None
        return spec

    def find_spec(self, fullname, path=None, target=None):
        package, _, submod = fullname.partition('.')
        self._packages.add(package)
        return None
    
    def get_packages(self):
        return self._packages

    def clear(self):
        self._packages = set()

def collect_modules_and_dependencies(modules: list):
    """ Imports a list of modules and collects all packages that are also
    imported. Needs to be run in a clean interpreter to correctly obtain all
    dependencies.
    """

    cwd = os.getcwd()
    sys.path.insert(0, cwd)

    finder = TracingFinder()
    sys.meta_path.insert(0, finder)
    for module_name in modules:
        importlib.import_module(module_name)

    for m in finder.get_packages():
        print(m)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('modules', metavar='m', type=str, nargs='+',
                    help='modules to trace imports for')
    args = parser.parse_args()
    collect_modules_and_dependencies(args.modules)

if __name__ == "__main__":
    main()