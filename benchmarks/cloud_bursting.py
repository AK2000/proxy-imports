import time
from globus_compute_sdk import Executor
import proxy_imports
import json
import argparse

@proxy_imports.proxy_transform(config_path="benchmarks/cloud_bursting_config.py")
def time_import():
    import time

    start = time.perf_counter()
    import sim_pack
    sim_pack.__wrapped__
    end = time.perf_counter() - start

    return end

def time_import_installed():
    import time

    start = time.perf_counter()
    import sim_pack
    end = time.perf_counter() - start

    return end

def cleanup():
    import shutil
    shutil.rmtree("/dev/shm/proxied-site-packages")
    return

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_info", default=None, help="Add additional information to results")
    parser.add_argument("--method", default="file_system", choices=["file_system", "lazy"])
    opts = parser.parse_args()

    # TODO: Make args
    endpoint_id = 'be4918db-2c33-4f53-afc9-0606ed7cb033'
    with Executor(endpoint_id=endpoint_id) as gce:
        start = time.perf_counter()
        if opts.method == "lazy":
            future = gce.submit(time_import)
        else:
            future = gce.submit(time_import_installed)

        import_time = future.result()
        rtt = time.perf_counter() - start

        if opts.method == "lazy":
            gce.submit(cleanup).result()

    results = {
        "import_time": import_time,
        "rtt": rtt,
        "method": opts.method
    }

    if opts.run_info is not None:
        run_info = json.loads(opts.run_info)
        for key, value in run_info.items():
            results[key] = value

    with open("results/cloud_bursting.jsonl", "a") as fp:
        fp.write(json.dumps(results) + "\n")
