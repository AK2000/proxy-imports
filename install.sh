# !/bin/bash

# Run from shared file system
base_env=`pwd -P`/base_env
conda env create --prefix ${base_env} --file base_environment.yml

test_env=`pwd -P`/test_env
conda env create --prefix ${test_env} --file base_environment.yml
conda activate ${test_env}
conda env update --file packages.yml
conda deactivate
