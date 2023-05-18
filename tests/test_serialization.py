from parsl.executors import HighThroughputExecutor
from parsl.app.python import PythonApp
from parsl.app.app import python_app
from parsl.config import Config
import parsl
import inspect
import dill

from proxy_imports import proxy_transform

import test_module

# inc = proxy_transform(test_module.inc, connector="file", package_path="proxied-site-packages")
# inc = parsl.python_app(inc)


import pdb; pdb.set_trace()
@proxy_transform
def test_function(a: int):
    import numpy as np
    return np.array([a,]) + 1

decorator = proxy_transform()
def test_function_2(a: int):
    import numpy as np
    return np.array([a,]) + 1
    
test_function_2 = decorator(test_function_2)

test_function = parsl.python_app(test_function)
test_function_2 = parsl.python_app(test_function_2)

config = Config(
    executors=[HighThroughputExecutor(
        max_workers=4, # Allows a maximum of two workers
        cpu_affinity='block' # Prevents workers from using the same cores
    )]
)
parsl.load(config)

print(test_function(1).result())
print(test_function_2(1).result())