# !/bin/bash

# Prepare conda
module load conda

# Run from shared file system
test_env=`pwd -P`/test_env
conda activate ${test_env}