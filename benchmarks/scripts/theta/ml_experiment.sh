#!/bin/bash
cd /lus/swift/home/alokvk2/lazy-imports
source setup.sh
export BLOCKSIZE=$COBALT_BLOCKSIZE

for nodes in 16; do
	for workers_per_node in 4; do
	    workers=$((${workers_per_node} * ${nodes}))
		if [ ${workers} -ge 64  ] && [ ${workers} -le 512 ]; then
			for method in "file_system" "lazy"; do
				echo "Running ${method} nodes ${nodes}, workers per node ${workers_per_node}"
				for rep in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do 
				   	echo "Repition ${rep}"
					python benchmarks/ml_inference.py \
						--workers ${workers} \
						--nodes ${nodes} \
						--method ${method} \
						--output results/theta/ml_64_tasks_16_nodes.jsonl
					rm /lus/swift/home/alokvk2/.proxy_modules/module-store/*
					rm argument_store/*
				done
			done
		fi
	done
done
