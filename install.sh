# !/bin/bash
module load conda

# Run from shared file system
base_env=`pwd -P`/base_env
conda env create --prefix ${base_env} --file base_environment.yml

test_env=`pwd -P`/test_env
conda env create --prefix ${test_env} --file base_environment.yml
conda env update --prefix ${test_env} --file packages.yml
