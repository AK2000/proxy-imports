# !/bin/bash

# Needed for perlmutter?
conda init bash
source ~/.bashrc

# Run from shared file system
test_env=`pwd -P`/test_env
conda activate ${test_env}

export LD_LIBRARY_PATH="/dev/shm/proxied-site-packages/libraries:$LD_LIBRARY_PATH"