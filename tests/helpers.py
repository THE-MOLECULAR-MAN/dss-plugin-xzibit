"""Shared test helpers for the xzibit plugin test suite.

Import from this module in test files.  The conftest.py stubs must already be
registered in sys.modules before any call to load_connector(); this is
guaranteed because pytest loads conftest.py before importing test modules.
"""

import os
import sys
import importlib.util

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_connector(connector_folder: str):
    """Import and return a connector module from python-connectors/<folder>/connector.py.

    The loaded module is registered under a unique name so multiple connectors
    can be loaded in the same process without namespace collisions.
    """
    path = os.path.join(
        _REPO_ROOT, "python-connectors", connector_folder, "connector.py"
    )
    module_name = f"connector_{connector_folder}"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod  # register before exec to handle circular refs
    spec.loader.exec_module(mod)
    return mod
