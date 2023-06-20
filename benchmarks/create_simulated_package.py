import argparse
import os
import shutil

def create_init(parent: str, submodules: list[str], prefix: str = ""):
    """ Create an __init__.py file that will import all submodules"""

    file_path = os.path.join(parent, "__init__.py")
    with open(file_path, "w") as fp:
        for submodule in submodules:
            fp.write(f"import {prefix}.{submodule}\n")

def create_setup(parent: str, package_name: str):
    code = \
f"""
from setuptools import setup, find_packages

setup(
    name='{package_name}',
    version='0.0.1',
    packages=find_packages(),
    description='Package with bunch of empty files for testing'
)
"""
    path = os.path.join(parent, "setup.py")
    with open(path, "w") as fp:
        fp.write(code)

def create_package(name: str, nfolders: int, level: int, path: str = "./", nfiles: int = 100, prefix: str = ""):
    """ Recursive method to create a package with a certain number of folders"""

    module_path = os.path.join(path, name)
    os.mkdir(module_path)
    if len(prefix) > 0:
        prefix = f"{prefix}.{name}"
    else:
        prefix = name
    submodules = []
    if level == 0:
        for i in range(nfiles):
            file_path = os.path.join(module_path, f"file_{i}.py")
            with open(file_path, "w") as fp:
                pass
            submodules.append(f"file_{i}")
    else:
        for i in range(nfolders):
            create_package(f"module_{i}", nfolders, level-1, module_path, nfiles, prefix)
            submodules.append(f"module_{i}")
        
    create_init(module_path, submodules, prefix=prefix)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("nfolders", type=int, help="Number of files to include in simulated package")
    parser.add_argument("--files", type=int, default=1000, help="Number of files per subfolder")
    parser.add_argument("--name", type=str, default="sim_pack", help="Name of package")
    parser.add_argument("--path", type=str, default="./", help="Path to put package at")
    opts = parser.parse_args()

    path = os.path.join(opts.path, "simulated_package")
    shutil.rmtree(path, ignore_errors=True)
    os.mkdir(path)
    create_package(opts.name, opts.nfolders, 1, path, opts.files)
    create_setup(path, opts.name)

if __name__ == "__main__":
    main()
