#!/bin/bash

for package_size in 1 2 3 4 5 6 7 8 9 10; do
    python create_simulated_package.py ${package_size}
    cd simulated_package
    pip install .
    cd ..   
    for rep in 1 2 3 4 5 6 7 8 9 10; do
        python cloud_bursting.py --method lazy --run_info {\"package_size\":${package_size}}
	    ssh cc@192.5.87.204 'bash ~/lazy-imports/benchmarks/scripts/restart_endpoint.sh'
    done
done

for package_size in 1 2 3 4 5 6 7 8 9 10; do
    ssh cc@192.5.87.204 "bash ~/lazy-imports/benchmarks/scripts/install_sim_pack.sh ${package_size}"
    for rep in 1 2 3 4 5 6 7 8 9 10; do
        python benchmarks/cloud_bursting.py --method file_system --run_info {\"package_size\":${package_size}}
        ssh cc@192.5.87.204 'bash ~/lazy-imports/benchmarks/scripts/restart_endpoint.sh'
    done
done
