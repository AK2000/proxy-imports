#!/bin/bash
cd /lus/swift/home/alokvk2/lazy-imports
source setup.sh
export BLOCKSIZE=$COBALT_BLOCKSIZE

for i in 1 2 3 4 5 6 7 8 9 10; do
    for method in "lazy" "file_system"; do
	echo "Running ${method} repetition ${i}"
	python benchmarks/xtb/molecular_design.py \
	    --search_space benchmarks/xtb/data/QM9-search.tsv \
	    --nodes ${COBALT_BLOCKSIZE} \
	    --method ${method} \
	    --initial 4096 \
	    --count 16384 \
	    --batch 4096 \
	    --output results/xtb_results.jsonl
    done
done
