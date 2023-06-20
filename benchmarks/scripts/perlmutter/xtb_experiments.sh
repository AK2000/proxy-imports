#!/bin/bash

source setup.sh
export BLOCKSIZE=${SLURM_NNODES}

for i in 1 2 3 4 5 6 7 8 9 10; do
    for method in "lazy" "file_system"; do
	echo "Running ${method} repetition ${i}"
	python xtb/molecular_design.py \
	    --search_space xtb/data/QM9-search.tsv \
	    --nodes ${SLURM_NNODES} \
	    --method ${method} \
	    --initial 4096 \
	    --count 16384 \
	    --batch 4096 \
	    --output results/xtb_results.jsonl
    done
done
