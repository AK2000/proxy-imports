#!/bin/bash

# Script to run all experiments on a single allocation
cd /lus/swift/home/alokvk2/lazy-imports
source setup.sh

export BLOCKSIZE=$COBALT_BLOCKSIZE
for tasks_per_node in 64 32 16 8 4 2 1; do
    for module in "tensorflow"; do
        for sleep in 10; do
            for method in "lazy" "file_system" "conda_pack"; do
                tasks=$((${tasks_per_node} * ${COBALT_BLOCKSIZE}))
                echo "Running ${tasks} with method ${method} and module ${module}"
                python benchmarks/scaling_test.py \
                    --ntsks ${tasks} \
                    --nodes ${COBALT_BLOCKSIZE} \
                    --method ${method} \
                    --module ${module} \
                    --sleep ${sleep} \
                    --output results/results-${COBALT_BLOCKSIZE}.jsonl
            done
        done 
    done
done
