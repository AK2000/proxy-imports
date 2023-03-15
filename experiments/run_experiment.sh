# !/bin/bash

# Script to run all experiments on a single allocation

source setup.sh

for method in "lazy" "file_system" "conda_pack"; do
    for module in "numpy" "tensorflow"; do
        for tasks_per_node in 1 2 4 8 16 32 64 do
            tasks=${tasks_per_node}*${SLURM_NNODES}
            python proof_of_concept.py \
                --ntsks ${tasks} \
                --nodes ${SLURM_NNODES} \
                --method ${method} \
                --module ${module} \
                --output results-${SLURM_NNODES}.jsonl