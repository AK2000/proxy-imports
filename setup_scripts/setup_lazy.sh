# !/bin/bash

# Needed for perlmutter?
conda init bash
source ~/.bashrc

# Run from shared file system
base_env=`pwd -P`/base_env
conda activate ${base_env}