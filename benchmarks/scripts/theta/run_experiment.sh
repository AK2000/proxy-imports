#!/bin/bash

# Script to run all experiments on a single allocation
cd /lus/swift/home/alokvk2/lazy-imports/benchmarks
source setup.sh

export BLOCKSIZE=$COBALT_JOBSIZE
for tasks_per_node in 64 32 16 8 4 2 1; do
    for module in "tensorflow"; do
        for sleep in 10; do
            for method in "lazy" "file_system" "conda_pack"; do
                tasks=$((${tasks_per_node} * ${COBALT_JOBSIZE}))
                echo "Running ${tasks} with method ${method} and module ${module}"
                python scaling_test.py \
                    --ntsks ${tasks} \
                    --nodes ${COBALT_JOBSIZE} \
                    --method ${method} \
                    --module ${module} \
                    --sleep ${sleep} \
                    --output results/results-${COBALT_JOBSIZE}.jsonl
            done
        done 
    done
done
