# !/bin/bash

# Script to accquire and run allocations

for nodes in 128; do
    qsub -n ${nodes} -t 02:00:00 -A CSC249ADCD08 -O lazy-import-${nodes} scripts/run_experiment.sh
done

qsub -n 128 -t 02:00:00 -A CSC249ADCD08 -O lazy-import-simulated-pkg scripts/simulated_package_exp.sh
qsub -n 128 -t 02:00:00 -A CSC249ADCD08 -O lazy-import-xtb scripts/ml_experiment.sh
