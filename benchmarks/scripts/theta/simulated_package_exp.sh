#!/bin/bash

# Script to run all experiments on a single allocation
cd /lus/swift/home/alokvk2/lazy-imports/benchmarks
source setup.sh
export BLOCKSIZE=$COBALT_JOBSIZE

tasks_per_node=64
tasks=$((${tasks_per_node} * ${COBALT_JOBSIZE}))
for package_size in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
    python create_simulated_package.py ${package_size}
    cd simulated_package
    pip install .
    cd ..
    for method in "lazy" "file_system" "conda_pack"; do
        echo "Running ${method} with package size ${package_size}"
        python scaling_test.py \
            --ntsks ${tasks} \
            --nodes ${COBALT_JOBSIZE} \
            --method ${method} \
            --module sim_pack \
            --sleep 0 \
            --run_info {\"package_size\":${package_size}} \
            --output results/results-simulated.jsonl
    done
done
