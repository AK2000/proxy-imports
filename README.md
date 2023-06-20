# Proxy Imports

A pure library to analyze distributed functions, determine their dependencies, then package and move them to a remote endpoing for function execution

### Dependencies
- dill >= 0.3.6
- proxystore >= 0.5.1
- PyInstaller


### Installation
```bash
$ git clone git@github.com:AK2000/lazy-imports.git
$ cd lazy-imports
$ pip install .
```

To configure the default ProxyStore backend to use when storing modules, and the location to place incoming packages, run 
```bash
$ proxy-imports-init 
```
which will create a default configuration file at `~/.proxy_modules/config.py`. This file should be edited for system and user specific options.

### How to run
Samples are coming soon. To reproduce the results in the paper see the documentation in `benchmarks/`.