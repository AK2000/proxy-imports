# !/bin/bash

# Needed for perlmutter?
module load conda

package_path="/dev/shm/local-envs"
package_dir="newenv"
package_tar="newenv-${SLURM_NNODES}.tar.gz"

# Clean up environment directory
rm -rdf "${package_path}/${package_dir}"

# Create environment directory
mkdir -p "${package_path}/${package_dir}"

# Unpack tar
tar -xzf "${package_tar}" -C "${package_path}/${package_dir}"

# Activate environment
conda activate "${package_path}/${package_dir}"

# Remove temp files
conda-unpack
