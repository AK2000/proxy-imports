import importlib
import inspect
import io
import os
import os.path
import shutil
import subprocess
import sys
import tarfile
import time
from types import ModuleType
from typing import Optional, Any, Union
from pathlib import Path
import zipfile


def package(m: ModuleType, method:str = "tar"):
    if method == "tar":
        try:
            module_path = inspect.getfile(m)
            if os.path.basename(module_path) == "__init__.py":
                module_path = Path(module_path).parent.absolute()
        except:
            module_path = m.__path__[0]
        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w|") as f:
            f.add(module_path, arcname=os.path.basename(module_path))
        
        module_bytes = tar_buffer.getvalue()
        tar_buffer.close()

    elif method == "zip":
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as fzip:
            try:
                module_path = inspect.getfile(m)
                if os.path.basename(module_path) == "__init__.py":
                    dirpath = Path(module_path).parent.absolute()
                    basedir = os.path.dirname(dirpath) + '/' 
                    for root, dirs, files in os.walk(dirpath):
                        if os.path.basename(root)[0] == '.':
                            continue #skip hidden directories        
                        dirname = root.replace(basedir, '')
                        for f in files:
                            if f[-1] == '~' or (f[0] == '.' and f != '.htaccess'):
                                #skip backup files and all hidden files except .htaccess
                                continue

                            fzip.write(root + '/' + f, dirname + '/' + f)
                else:
                    fzip.write(module_path, os.path.basename(module_path))
            except:
                module_path = m.__path__[0]
                fzip.write(module_path, os.path.basename(module_path))   

        # Convert to string so can easily serialize
        module_bytes = zip_buffer.getvalue()
        zip_buffer.close()

    return module_bytes

def unpack(buffer:bytes, method:str = "tar"):
    if method == "tar":
        tar_str = io.BytesIO(buffer)
        with tarfile.open(fileobj=tar_str, mode="r|") as f:
            f.extractall("./proxied-site-packages/tensorflow")
    elif method == "zip":
        with open(f"tensorflow.zip",'wb', 100*(2**20)) as local_archive:
            local_archive.write(buffer)
    elif method == "zip:extract":
        zip_buffer = io.BytesIO(buffer)
        with zipfile.ZipFile(zip_buffer, "r") as fzip:
            fzip.extractall("./proxied-site-packages/tensorflow")

if __name__ == "__main__":
    import tensorflow

    #module_bytes = package(tensorflow, "tar")
    
    #start = time.perf_counter()
    #unpack(module_bytes, "tar")
    #finish = time.perf_counter() - start
    #print(f"Time for Tar Method: {finish}")

    module_bytes = package(tensorflow, "zip")

    start = time.perf_counter()
    unpack(module_bytes, "zip")
    finish = time.perf_counter() - start
    print(f"Time for Zip Method: {finish}")

    start = time.perf_counter()
    unpack(module_bytes, "zip:extract")
    finish = time.perf_counter() - start
    print(f"Time for Zip Method w/ Extract: {finish}")
