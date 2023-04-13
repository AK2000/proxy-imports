from parsl.executors import HighThroughputExecutor
from parsl.app.python import PythonApp
from parsl.app.app import python_app
from parsl.config import Config
import parsl
import inspect
import dill

from proxy_imports import proxy_transform

import test_module

dill.detect.trace(True)
inc = proxy_transform(test_module.inc, connector="file", package_path = "proxied-site-packages")
inc = parsl.python_app(inc)

config = Config(
    executors=[HighThroughputExecutor(
        max_workers=4, # Allows a maximum of two workers
        cpu_affinity='block' # Prevents workers from using the same cores
    )]
)
parsl.load(config)
print(inc(1).result())