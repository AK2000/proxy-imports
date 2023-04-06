# Lazy Imports

A simple proof-of-concept example where ProxyStore is used to lazily import modules at runtime.

### Dependencies
- conda
- numpy
- proxystore
- tensorflow
- PyInstaller


### How to install
To set up the necessary environments, run `source install.sh` which will create 2 conda environments in the current directory. Currently proxystore (https://github.com/proxystore/proxystore) has to to be installed from source into the test environment. The hope is that this that the proxystore PyPI wheel gets updated.

To set up the environment after the initial installation, run `source setup.sh`. 

Once the environments are set up, you can run `pip install -e .` from the root of the project directory. 

### How to run
To run a simple proof of concept:
`python proof_of_concept.py`

Larger experiment scripts are in `experiments/`. For example, to run the scaling experiments on Perlmutter,
you can use `experiments/submit.sh`
