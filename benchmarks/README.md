# Proxy Imports Benchmarks
This folder contains the information necessary to reproduce the results from an upcoming paper. 

## Setup
The benchmarks are setup to run in a conda environment. Make sure that conda is either installed on the system or available to load as a module. Then run:
```bash
$ bash install.sh
```
This command may take a while (sometime nearly an hour) to resolve the environment. After it's finished, there will be two new conda environments created in this folder. `base_env` is a environment with minimal dependencies. It will be used with the `conda-pack` solution to create new environments with a subset of the packages. `test_env` will be the working conda environment where we run our experiments.

Two other packages must be installed from source in the test env. To activate the test environment, run  First install Proxy Imports following the directions in the root of this repository. Then install a modified version of Parsl for the benchmarks:

```bash
$ export TEST_ENV=`pwd -P`/test_env
$ conda activate $TEST_ENV
$ git clone git@github.com:AK2000/parsl.git
$ cd parsl
$ git checkout proxy_imports
$ pip install .
```
Note: Proxy Imports can be used without modifying Parsl. See (upcoming) samples for examples. However, using the modified version of Parsl avoids some overhead leading to better performance.

## Running Experiments

In general the scripts for different experiments can be found as`scripts/\<system\>/\<experiment\>.sh. More details will follow later.

### Scaling Experiments

### Simulated Package Experiments

### Cloud Bursting Experiment
The cloud bursting experiment requires first deploying a chameleon container, setting some things up there, then deploying a globus-compute enpoint to that container. You must also modify globus-compute to use a serializer that is compatible with decorated functions. As of now, this is done in the package code, but this is known problem with globus-compute and should become a feature soon.

### ML Benchmark

## Graphing and Analysis
