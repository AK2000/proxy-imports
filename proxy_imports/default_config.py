config = {
    "package_path": "/dev/shm/proxied-site-packages",
    "module_store_config": {
        "name": "module-store",
        "connector_type": "file",
        "connector_config": {
            "store_dir": "~/.proxy_modules/module-store" # Should include python version?
        },
        "cache_size": 16  
    }
}