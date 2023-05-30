#/bin/bash
conda create --yes --prefix="$PWD/clean_env" python=3.10 > conda_errors.txt
source activate "$PWD/clean_env" > conda_errors.txt

echo "Package,Files Accessed,Files Downloaded"
while read line; do
    a=( $line )
    package=${a[0]}
    import_name=$package
    if [ ${#a[@]} -gt 1 ]; then
	import_name=${a[1]}
    fi
    pip install $package > install_errors.txt
    echo "import $package" > import_module.py
    files_accessed=$(python -X importtime import_module.py |& wc -l)
    files_downloaded=$(find clean_env/lib/python3.10/site-packages/$import_name | wc -l)
    echo "$package,$files_accessed,$files_downloaded"
done < packages.txt

conda deactivate > conda_errors.txt
conda remove --yes -p $PWD/clean_env --all > conda_errors.txt
