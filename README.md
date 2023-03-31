# Lazy Imports

A simple proof-of-concept example where ProxyStore is used to lazily import modules at runtime.

### Dependencies
- conda
- numpy
- proxystore
- tensorflow
- PyInstaller

### How to install
To set up the necessary environments, run `source install.sh` which will create 2 conda environments in 
the current directory.

To set up the environment after the initial installation, run `source setup.sh`. 

### How to run
To run a simple proof of concept:
`python proof_of_concept.py`

Larger experiment scripts are in `experiments/`. For example, to run the scaling experiments on Perlmutter,
you can use `experiments/submit.sh`
