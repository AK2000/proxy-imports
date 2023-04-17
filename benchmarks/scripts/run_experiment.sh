#!/bin/bash

# Script to run all experiments on a single allocation

source setup.sh

for method in "lazy" "file_system" "conda_pack"; do
    for module in "numpy" "tensorflow"; do
        for sleep in 0 10; do
            for tasks_per_node in 1 2 4 8 16 32 64; do
                tasks=$((${tasks_per_node} * ${SLURM_NNODES}))
                echo "Running ${tasks} with method ${method} and module ${module}"
                python benchmarks/scaling_test.py \
                    --ntsks ${tasks} \
                    --nodes ${SLURM_NNODES} \
                    --method ${method} \
                    --module ${module} \
                    --sleep ${sleep} \
                    --connector file \
                    --output results/results-${SLURM_NNODES}.jsonl
            done
        done 
    done
done
