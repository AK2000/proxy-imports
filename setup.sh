# !/bin/bash

# Prepare conda
conda init bash
source ~/.bashrc

# Run from shared file system
test_env=`pwd -P`/test_env
conda activate ${test_env}
