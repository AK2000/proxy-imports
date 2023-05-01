import argparse
from chemfunctions import compute_vertical
from concurrent.futures import as_completed
from tqdm import tqdm
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider
from parsl.providers import SlurmProvider
from parsl.app.python import PythonApp
from parsl.app.app import python_app
from parsl.config import Config
from time import monotonic
from random import sample
from pathlib import Path
import pandas as pd
import numpy as np
import parsl
import os
import json

from proxy_imports import proxy_transform

def setup(nodes: int = 0, method: str = "file_system"):
    """Setup config for parsl"""

    provider = LocalProvider(worker_init=f"source setup_scripts/setup_{method}.sh")
    if nodes > 1:
        provider.launcher = parsl.launchers.SrunLauncher(overrides='-K0 -k')
        provider.nodes_per_block = nodes

    executor = parsl.HighThroughputExecutor(
        provider=provider,
        cpu_affinity='block'
    )

    config = parsl.config.Config(
       executors=[ executor ],
       strategy=None
    )

    parsl.load(config)

def train_model(train_data):
    """Train a machine learning model using Morgan Fingerprints.
    
    Args:
        train_data: Dataframe with a 'smiles' and 'ie' column
            that contains molecule structure and property, respectfully.
    Returns:
        A trained model
    """
    # Imports for python functions run remotely must be defined inside the function
    from chemfunctions import MorganFingerprintTransformer
    from sklearn.neighbors import KNeighborsRegressor
    from sklearn.pipeline import Pipeline
    
    
    model = Pipeline([
        ('fingerprint', MorganFingerprintTransformer()),
        ('knn', KNeighborsRegressor(n_neighbors=4, weights='distance', metric='jaccard', n_jobs=-1))  # n_jobs = -1 lets the model run all available processors
    ])
    
    return model.fit(train_data['smiles'], train_data['ie'])

def run_model(model, smiles):
    """Run a model on a list of smiles strings
    
    Args:
        model: Trained model that takes SMILES strings as inputs
        smiles: List of molecules to evaluate
    Returns:
        A dataframe with the molecules and their predicted outputs
    """
    import pandas as pd
    pred_y = model.predict(smiles)
    return pd.DataFrame({'smiles': smiles, 'ie': pred_y})

def combine_inferences(inputs=[]):
    """Concatenate a series of inferences into a single DataFrame
    Args:
        inputs: a list of the component DataFrames
    Returns:
        A single DataFrame containing the same inferences
    """
    import pandas as pd
    return pd.concat(inputs, ignore_index=True)

# compute_vertical_app = python_app(compute_vertical)
def training_loop(
            search_space: pd.DataFrame, 
            initial_count: int = 8, 
            search_count: int = 64, 
            batch_size: int = 4,
            method: str = "file_system",
            package_path: str = "/dev/shm/proxied-site-packages",
            connector: str = "multi"
        ):
    """ Run the active learning loop
    Args:
        search_space: candidate molecule smile strings as dataframe
        initial_count: number of molecules to sample to create initial training set
        search_count: total number of molecules to "discover"
        batch_size: size of batch between model training
        method: how to transfer inputs
        ...
    Returns:
        The time it took to run the entire workflow
    """

    # # Analyze and transform methods
    if method == "lazy":
        compute_vertical_app = python_app(proxy_transform(compute_vertical, package_path=package_path, connector=connector))
        train_model_app = python_app(proxy_transform(train_model, package_path=package_path, connector=connector))
        run_model_app = python_app(proxy_transform(run_model, package_path=package_path, connector=connector))
        combine_inferences_app = python_app(proxy_transform(combine_inferences, package_path=package_path, connector=connector))
    else:
        # Create Parsl Apps
        compute_vertical_app = python_app(compute_vertical)
        train_model_app = python_app(train_model)
        run_model_app = python_app(run_model)
        combine_inferences_app = python_app(combine_inferences)

    with tqdm(total=search_count) as prog_bar: # setup a graphical progress bar
        # Mark when we started
        start_time = monotonic()
        
        # Submit with some random guesses
        train_data = []
        init_mols = search_space.sample(initial_count, random_state=12345)['smiles']
        sim_futures = [compute_vertical_app(mol) for mol in init_mols]
        already_ran = set()
        
        # Loop until you finish populating the initial set
        while len(sim_futures) > 0: 
            # First, get the next completed computation from the list
            future = next(as_completed(sim_futures))

            # Remove it from the list of still-running tasks
            sim_futures.remove(future)

            # Get the input 
            smiles = future.task_def['args'][0]
            already_ran.add(smiles)

            # Check if the run completed successfully
            if future.exception() is not None:
                # If it failed, pick a new SMILES string at random and submit it    
                smiles = search_space.sample(1).iloc[0]['smiles'] # pick one molecule
                new_future = compute_vertical_app(smiles) # launch a simulation in Parsl
                sim_futures.append(new_future) # store the Future so we can keep track of it
            else:
                # If it succeeded, store the result
                prog_bar.update(1)
                train_data.append({
                    'smiles': smiles,
                    'ie': future.result(),
                    'batch': 0,
                    'time': monotonic() - start_time
                })
        
        # Create the initial training set as a 
        train_data = pd.DataFrame(train_data)
        
        # Loop until complete
        batch = 1
        while len(train_data) < search_count:
            
            # Train and predict as show in the previous section.
            train_future = train_model_app(train_data)
            inference_futures = [run_model_app(train_future, chunk) for chunk in np.array_split(search_space['smiles'], 64)]
            predictions = combine_inferences_app(inputs=inference_futures).result()

            # Sort the predictions in descending order, and submit new molecules from them
            predictions.sort_values('ie', ascending=False, inplace=True)
            sim_futures = []
            for smiles in predictions['smiles']:
                if smiles not in already_ran:
                    sim_futures.append(compute_vertical_app(smiles))
                    already_ran.add(smiles)
                    if len(sim_futures) >= batch_size:
                        break

            # Wait for every task in the current batch to complete, and store successful results
            new_results = []
            for future in as_completed(sim_futures):
                if future.exception() is None:
                    prog_bar.update(1)
                    new_results.append({
                        'smiles': future.task_def['args'][0],
                        'ie': future.result(),
                        'batch': batch, 
                        'time': monotonic() - start_time
                    })
                    
            # Update the training data and repeat
            batch += 1
            train_data = pd.concat((train_data, pd.DataFrame(new_results)), ignore_index=True)

        # Mark when we finished
        end_time = monotonic()

    return end_time - start_time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--search_space", type=str, default="data/QM9-search.tsv", help="File to load search space from")
    parser.add_argument("--nodes", type=int, default=0, help="Number of nodes to use")
    parser.add_argument("--initial", type=int, default=8, help="Initial number of molecules to sample")
    parser.add_argument("--count", type=int, default=64, help="Number of molecules to discover")
    parser.add_argument("--batch", type=int, default=4, help="Number of molecules to simulate per batch")
    parser.add_argument("--method", default="file_system", choices=["file_system", "lazy"])
    parser.add_argument("--output", default="results/xtb_results.jsonl", help="File to output results")
    opts = parser.parse_args()

    # Read in search space
    search_space = pd.read_csv(opts.search_space, delim_whitespace=True)  # Our search space of molecules

    # Setup Parsl
    setup(opts.nodes, opts.method)

    # Run training loop
    time = training_loop(search_space, opts.initial, opts.count, opts.batch, opts.method)

    results = {
        "method": opts.method,
        "nodes": opts.nodes,
        "initial": opts.initial,
        "count": opts.count,
        "batch": opts.batch,
        "time": time
    }

    with open(opts.output, "a") as fp:
        fp.write(json.dumps(results) + "\n")

if __name__ == "__main__":
    main()
