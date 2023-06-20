import parsl
from parsl.providers import LocalProvider

def make_config_perlmutter(nodes: int = 0, method: str = "file_system") -> parsl.config.Config:
    '''
    Build a config for an executor.
    '''
    provider = LocalProvider(worker_init=f"source setup_scripts/setup_{method}.sh")
    if nodes > 1:
        provider.launcher = parsl.launchers.SrunLauncher(overrides='-K0 -k')
        provider.nodes_per_block = nodes
    executor = parsl.HighThroughputExecutor(provider=provider, proxy_modules = (method == "lazy"))

    config = parsl.config.Config(
       executors=[ executor ],
       strategy=None
    )

    return config

def make_config_theta(nodes: int = 0, method: str = "file_system") -> parsl.config.Config:
    '''
    Build a config for an executor.
    '''
    provider = LocalProvider(worker_init=f"source setup_scripts/setup_{method}.sh")
    if nodes > 1:
        provider.launcher = parsl.launchers.AprunLauncher(overrides='-d 64 --cc depth')
        provider.nodes_per_block = nodes
    executor = parsl.HighThroughputExecutor(
        cpu_affinity="block",
        max_workers=64,
        provider=provider,
        proxy_modules= (method == "lazy")
    )

    config = parsl.config.Config(
       executors=[ executor ],
       strategy=None
    )

    return config