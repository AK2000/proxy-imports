# !/bin/bash

# Script to accquire and run allocations

for nodes in 64; do
    sbatch -N ${nodes} -C cpu -q regular -J lazy-import-${nodes} -t 02:00:00 scripts/perlmutter/run_experiment.sh
done

sbatch -N 64 -C cpu -q regular -J lazy-import-sim -t 02:00:00 scripts/perlmutter/simulated_package_exp.sh
