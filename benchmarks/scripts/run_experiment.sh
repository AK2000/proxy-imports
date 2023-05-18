#!/bin/bash

# Script to run all experiments on a single allocation

source setup.sh

for tasks_per_node in 64 32 16 8 4 2 1; do
    for module in "tensorflow"; do
        for sleep in 10; do
            for method in "lazy" "file_system" "conda_pack"; do
                tasks=$((${tasks_per_node} * ${SLURM_NNODES}))
                echo "Running ${tasks} with method ${method} and module ${module}"
                python benchmarks/scaling_test.py \
                    --ntsks ${tasks} \
                    --nodes ${SLURM_NNODES} \
                    --method ${method} \
                    --module ${module} \
                    --sleep ${sleep} \
                    --output results/results-${SLURM_NNODES}.jsonl
            done
        done 
    done
done
