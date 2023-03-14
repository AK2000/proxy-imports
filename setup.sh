# !/bin/bash

# Run from shared file system
base_env=`pwd -P`/base_env
conda activate ${base_env}

test_env=`pwd -P`/test_env
conda activate --stack ${test_env}