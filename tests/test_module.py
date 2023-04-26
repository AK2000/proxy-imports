import inspect
import numpy as np
from numpy.linalg import *

def inc(a):
    assert "proxied-site-packages" in inspect.getfile(np), inspect.getfile(
        np
    )  # make sure correct numpy is being used

    A = np.random.rand(5,5)
    b = np.random.rand(5)
    arr = solve(A, b)
    return a + 1