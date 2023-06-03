#!/bin/bash
cd /lus/swift/home/alokvk2/lazy-imports
source setup.sh
export BLOCKSIZE=$COBALT_BLOCKSIZE

for nodes in 32 16 8 4 2 1; do
	for workers_per_node in 1 2 4 8 16 32 64; do
	    workers=$((${workers_per_node} * ${nodes}))
		if [ ${workers} -ge 64  ] && [ ${workers} -le 512 ]; then
			for method in "lazy" "file_system"; do
				echo "Running ${method} nodes ${nodes}, workers per node ${workers_per_node}"
				python benchmarks/ml_inference.py \
					--workers ${workers} \
					--nodes ${nodes} \
					--method ${method} \
					--output results/theta/ml_results.jsonl
			done
			rm /lus/swift/home/alokvk2/.proxy_modules/module-store/*
			rm argument_store/*
		fi
    done
done
