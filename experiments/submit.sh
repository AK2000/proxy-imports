# !/bin/bash

# Script to accquire and run allocations

for nodes in 1 2 4 8 16 32 64; do
    sbatch -N ${nodes} -C cpu -q regular -J lazy-import-${nodes} -t 01:00:00 experiments/run_experiment.sh
done
