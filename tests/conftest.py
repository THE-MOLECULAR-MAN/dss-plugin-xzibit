"""
Pytest configuration for the xzibit plugin test suite.

Loaded by pytest before any test module.  This file:
  1. Extends sys.path so ``import xzibit.*`` resolves to python-lib/.
  2. Extends sys.path so ``from helpers import load_connector`` works in tests.
  3. Registers lightweight stub modules for packages that are only available
     inside a live DSS instance (dataiku, dataikuapi, vermin, radon) so that
     tests can import the plugin code without a running DSS.
"""

import sys
import os
from unittest.mock import MagicMock

# ── 1. Extend sys.path ────────────────────────────────────────────────────
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PYTHON_LIB = os.path.join(_REPO_ROOT, "python-lib")
_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))

for _p in (_PYTHON_LIB, _TESTS_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)


# ── 2. Concrete stub classes needed for isinstance / except checks ──────────

class DataikuException(Exception):
    """Stand-in for dataikuapi.utils.DataikuException."""


class DSSDataset:
    """Stand-in for dataikuapi.dss.dataset.DSSDataset."""


class Connector:
    """Stand-in for dataiku.connector.Connector."""
    def __init__(self, config, plugin_config):
        self.config = config
        self.plugin_config = plugin_config


# ── 3. Build and register stub modules ────────────────────────────────────

def _register_stubs() -> None:
    mock_dataiku = MagicMock(name="dataiku")
    mock_dataiku_connector = MagicMock(name="dataiku.connector")
    mock_dataiku_connector.Connector = Connector

    mock_dataikuapi = MagicMock(name="dataikuapi")
    mock_dataikuapi_utils = MagicMock(name="dataikuapi.utils")
    mock_dataikuapi_utils.DataikuException = DataikuException
    mock_dataikuapi_dss = MagicMock(name="dataikuapi.dss")
    mock_dataikuapi_dss_dataset = MagicMock(name="dataikuapi.dss.dataset")
    mock_dataikuapi_dss_dataset.DSSDataset = DSSDataset

    # Wire attribute chains so `dataikuapi.dss.dataset.DSSDataset` resolves
    mock_dataikuapi.dss = mock_dataikuapi_dss
    mock_dataikuapi_dss.dataset = mock_dataikuapi_dss_dataset

    mock_vermin = MagicMock(name="vermin")
    mock_radon_cc = MagicMock(name="radon.complexity")
    mock_radon_mi = MagicMock(name="radon.metrics")

    stubs = {
        "dataiku": mock_dataiku,
        "dataiku.connector": mock_dataiku_connector,
        "dataikuapi": mock_dataikuapi,
        "dataikuapi.utils": mock_dataikuapi_utils,
        "dataikuapi.dss": mock_dataikuapi_dss,
        "dataikuapi.dss.dataset": mock_dataikuapi_dss_dataset,
        "vermin": mock_vermin,
        "radon": MagicMock(name="radon"),
        "radon.complexity": mock_radon_cc,
        "radon.metrics": mock_radon_mi,
    }
    for name, stub in stubs.items():
        sys.modules.setdefault(name, stub)


_register_stubs()
