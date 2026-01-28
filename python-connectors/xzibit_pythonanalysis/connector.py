"""A Dataiku DSS v12 connector to provide a DSS Dataset listing
all Python Code Recipes, and information about them.
OPTIMIZED: Uses batch processing to reduce subprocess overhead.
"""

import tempfile
import os
import logging
import json

# import ast
import subprocess

# import shutil
import hashlib
from typing import Dict, List, Any

from vermin import detect, Config

import radon.complexity as radon_cc
import radon.metrics as radon_mi

from dataiku import api_client
from dataiku.connector import Connector

# Define path to binaries in the managed code env
# Note: Adjust if your environment path differs
BINARY_PATH = "/data/dataiku/dss_data/code-envs/python/plugin_xzibit_managed/bin/"

from xzibit.utils import get_python_recipe_code_env, get_dss_base_url

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger()


def get_recipe_last_modifier_user(recipe_handle) -> str:
    """Fetches the last modifier user of a given recipe."""
    try:
        recipe_settings_handle = recipe_handle.get_settings()
        raw_data = recipe_settings_handle.get_recipe_raw_definition()
        last_modifier = (
            raw_data.get("versionTag", {}).get("lastModifiedBy", {}).get("login", None)
        )
        return last_modifier
    except Exception:
        return "Unknown"


def get_recipe_last_modified_timestamp(recipe_handle) -> str:
    """Fetches the last modified timestamp of a given recipe."""
    try:
        recipe_settings_handle = recipe_handle.get_settings()
        raw_data = recipe_settings_handle.get_recipe_raw_definition()
        last_modified_ts = raw_data.get("versionTag", {}).get("lastModifiedOn", None)
        return last_modified_ts
    except Exception:
        return "Unknown"


def get_tuples_only(input_list: list) -> list:
    """Filters a list to return only elements that are of type tuple."""
    return [item for item in input_list if isinstance(item, tuple)]


class ConnectorPythonAnalysis(Connector):
    """A Dataiku DSS v12 connector to provide a DSS Dataset listing
    all Python Code Recipes, and information about them."""

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.vermin_config = Config()
        self.vermin_config.set_verbose(0)
        self.__baseurl = get_dss_base_url()

        # Batch size configuration
        self.batch_size = 50

    def get_url(self, recipe_id, project_key):
        """Create a URL to the DSS object."""
        if any(v is None for v in (self.__baseurl, recipe_id, project_key)):
            return None
        return f"{self.__baseurl}/projects/{project_key}/recipes/{recipe_id}/"

    def get_python_recipes(self, project_handle):
        """Retrieves a list of all Python code recipes in the current project."""
        recipes = project_handle.list_recipes()
        return [r for r in recipes if r["type"] == "python"]

    def get_recipe_code(self, recipe_handle) -> str:
        """Fetches the actual Python script content from a specific recipe."""
        try:
            settings = recipe_handle.get_settings()
            return settings.get_code()
        except AttributeError:
            logger.warning(
                "Could not retrieve code. It may not be a standard code recipe."
            )
            return ""

    def get_code_env_python_version(self, code_env_name: str) -> str:
        """Fetches the Python version associated with a given code environment."""
        try:
            if not code_env_name:
                return ""
            # Note: get_code_env can fail if permissions are missing
            code_env = self.__client.get_code_env("PYTHON", code_env_name)

            settings = code_env.get_settings().get_raw()
            py_interp_version = settings.get("desc", {}).get(
                "pythonInterpreter", "Unknown"
            )

            # Convert PYTHON39 -> 3.9
            py_interp_version = py_interp_version.replace("PYTHON", "")
            if len(py_interp_version) >= 2:
                python_version_formatted = (
                    f"{py_interp_version[0]}.{py_interp_version[1:]}"
                )
                return str(python_version_formatted)
            return py_interp_version

        except Exception:
            # Often happens if user doesn't have read access to the code env
            return "Unknown"

    # -------------------------------------------------------------------------
    # In-Memory Analysis Tools (Fast)
    # -------------------------------------------------------------------------

    def get_code_sample(self, code: str, max_lines: int = 40) -> str:
        """Returns the first 'max_lines' lines of code as a sample."""
        if not code:
            return ""
        lines = code.splitlines()
        sample = "\n".join(lines[:max_lines])
        if len(lines) > max_lines:
            sample += "\n..."
        return sample

    def check_default_snippets_presence(self, code: str) -> bool:
        """
        Checks if any of the items in the list of default_code_snippets are found in "code".

        Performs newline normalization to ensure compatibility across different
        operating systems (handling \r\n vs \n).

        Args:
            code (str): The full contents of the Python script.
            default_code_snippets (List[str]): A list of strings, where each string
                                            contains one or more lines of code.

        Returns:
            bool: True if any snippet is found in the code, False otherwise.
        """
        default_code_snippets = [
            "_df = ... # Compute a Pandas dataframe to write into ",
            """# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

# Read recipe inputs""",
            """# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu



# Compute recipe outputs
# TODO: Write here your actual code that computes the outputs
# NB: DSS supports several kinds of APIs for reading and writing data. Please see doc.
""",
        ]
        if not code or not default_code_snippets:
            return False

        # Normalize newlines in the source code to standard Unix style (\n)
        # This prevents failures if the code has different line endings than the snippets.
        normalized_code = code.replace("\r\n", "\n")

        # Generator expression checking for existence of any snippet
        return any(
            snippet.replace("\r\n", "\n") in normalized_code
            for snippet in default_code_snippets
        )

    def _analyze_vermin(self, code: str) -> str:
        """Run Vermin to detect minimum Python version."""
        try:
            mins = detect(code, config=self.vermin_config)
            mins = get_tuples_only(mins)

            if mins:
                # Returns the highest minimum required version (e.g., 3.9)
                return max(mins)
            return "Unknown"
        except Exception as e:
            logger.warning(f"Vermin analysis failed: {e}")
            return "Error"

    def _analyze_radon(self, code: str) -> Dict[str, Any]:
        """Run Radon for Cyclomatic Complexity and Maintainability Index."""
        try:
            # Cyclomatic Complexity
            complexity = radon_cc.cc_visit(code)
            avg_complexity = (
                radon_cc.average_complexity(complexity) if complexity else 0
            )

            # Maintainability Index (score of 100 is best, 0 is worst)
            mi_score = radon_mi.mi_visit(code, multi=False)

            return {
                "radon_cc_avg": round(avg_complexity, 2),
                "radon_mi_score": round(mi_score, 2),
                "radon_rank": radon_mi.mi_rank(mi_score),
            }
        except Exception as e:
            # Syntax errors often cause Radon failure
            return {"radon_cc_avg": -1, "radon_mi_score": -1, "radon_rank": "Error"}

    # -------------------------------------------------------------------------
    # Batch Processing for Subprocess Tools (Pylint, Ruff)
    # -------------------------------------------------------------------------

    def _process_batch(self, batch_data: List[Dict[str, Any]]) -> None:
        """
        Runs Pylint and Ruff on a batch of files to amortize startup cost.
        Updates the dictionaries in batch_data in-place.
        """
        if not batch_data:
            return

        with tempfile.TemporaryDirectory(prefix="dss_batch_analysis_") as tmp_dir:
            # 1. Write all codes to temp files
            # Mapping: filename -> index in batch_data
            file_map = {}

            for idx, item in enumerate(batch_data):
                # Sanitize name for filesystem safety
                safe_name = "".join(
                    c
                    for c in item["recipe_name"]
                    if c.isalnum() or c in (" ", "_", "-")
                ).strip()
                safe_name = safe_name.replace(" ", "_")
                filename = f"{idx}_{safe_name}.py"
                file_path = os.path.join(tmp_dir, filename)

                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(item["_raw_code"])

                file_map[filename] = idx

            # 2. Run Ruff on the directory
            try:
                RUFF_PATH = os.path.join(BINARY_PATH, "ruff")
                # Run check on the whole directory
                cmd = [RUFF_PATH, "check", ".", "--output-format=json"]
                result = subprocess.run(
                    cmd, cwd=tmp_dir, capture_output=True, text=True
                )

                # Ruff returns exit code 1 if violations found, so ignore check=True
                ruff_data = json.loads(result.stdout)

                # Count violations per file
                # ruff output has "filename" which is absolute path
                counts = {i: 0 for i in range(len(batch_data))}

                for violation in ruff_data:
                    fname = os.path.basename(violation.get("filename", ""))
                    if fname in file_map:
                        idx = file_map[fname]
                        counts[idx] += 1

                # Update batch data
                for idx, count in counts.items():
                    batch_data[idx]["ruff_violations"] = count

            except Exception as e:
                logger.error(f"Batch Ruff failed: {e}")
                for item in batch_data:
                    item["ruff_violations"] = -1

            # 3. Run Pylint on the directory
            try:
                PYLINT_PATH = os.path.join(BINARY_PATH, "pylint")
                # --recursive=y ensures it looks at all .py files in dir
                cmd = [PYLINT_PATH, ".", "--output-format=json", "--recursive=y"]
                result = subprocess.run(
                    cmd, cwd=tmp_dir, capture_output=True, text=True
                )

                try:
                    pylint_data = json.loads(result.stdout)
                except json.JSONDecodeError:
                    # If pylint finds nothing or crashes, stdout might be empty or non-json
                    pylint_data = []

                # Count issues per file
                counts = {i: 0 for i in range(len(batch_data))}

                for issue in pylint_data:
                    # pylint "path" is usually the relative filename (e.g. "0_recipe.py")
                    fname = os.path.basename(issue.get("path", ""))
                    if fname in file_map:
                        idx = file_map[fname]
                        counts[idx] += 1

                # Update batch data
                for idx, count in counts.items():
                    batch_data[idx]["pylint_issues"] = count

            except Exception as e:
                logger.error(f"Batch Pylint failed: {e}")
                for item in batch_data:
                    item["pylint_issues"] = -1

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """Generator that yields analysis rows."""
        records_generated = 0
        current_batch = []

        # Loop through projects
        for project_key in self.__client.list_project_keys():
            if records_limit > 0 and records_generated >= records_limit:
                break

            try:
                project_handle = self.__client.get_project(project_key)
                python_recipes = self.get_python_recipes(project_handle)

                for recipe_meta in python_recipes:
                    if records_limit > 0 and records_generated >= records_limit:
                        break

                    name = recipe_meta["name"]

                    try:
                        recipe_handle = project_handle.get_recipe(name)
                        code = self.get_recipe_code(recipe_handle)

                        if not code:
                            logger.info(f"{name:<40} | {'Skipped (No Code)':<20}")
                            continue

                        # Prepare the row object
                        row = {
                            "project_key": project_key,
                            "recipe_name": name,
                            "url": self.get_url(recipe_handle.id, project_key),
                            "_raw_code": code,  # Temporary storage for batching
                        }

                        # Metadata
                        code_env_name = get_python_recipe_code_env(recipe_handle)
                        row["code_env_name"] = code_env_name
                        row["code_env_python_version"] = (
                            self.get_code_env_python_version(code_env_name)
                        )
                        row["num_lines_of_code"] = len(code.splitlines())

                        # the MD5 hash of the code

                        code_hashsum = hashlib.md5(code.encode("utf-8")).hexdigest()

                        row["code_hashsum"] = code_hashsum

                        row["last_modified_by_user"] = get_recipe_last_modifier_user(
                            recipe_handle
                        )

                        row["last_modified_timestamp"] = (
                            get_recipe_last_modified_timestamp(recipe_handle)
                        )

                        # Run In-Memory Analysis immediately (Vermin, Radon)

                        row["is_likely_unaltered_default_code"] = (
                            self.check_default_snippets_presence(code)
                        )

                        row["code_sample"] = self.get_code_sample(code)

                        mpvv = self._analyze_vermin(code)
                        row["min_python_version_from_Vermin"] = (
                            f"{mpvv[0]}.{mpvv[1]}"
                            if isinstance(mpvv, tuple)
                            else str(mpvv)
                        )

                        # Only run complexity checks if it looks like Python 3
                        # (Vermin returns tuple (major, minor) or int 0 if unknown)
                        is_py3 = (isinstance(mpvv, tuple) and mpvv[0] >= 3) or (
                            isinstance(mpvv, int) and mpvv >= 3
                        )

                        if is_py3:
                            radon_metrics = self._analyze_radon(code)
                            row.update(radon_metrics)
                        else:
                            row.update(
                                {
                                    "radon_cc_avg": None,
                                    "radon_mi_score": None,
                                    "radon_rank": "N/A",
                                }
                            )

                        # Add to batch
                        current_batch.append(row)

                        # Process Batch if full
                        if len(current_batch) >= self.batch_size:
                            self._process_batch(current_batch)
                            for processed_row in current_batch:
                                del processed_row["_raw_code"]  # Cleanup large string
                                records_generated += 1
                                yield processed_row
                            current_batch = []

                    except Exception as e:
                        logger.error(f"Error preparing {name} in {project_key}: {e}")
                        # Yield error row immediately
                        yield {
                            "project_key": project_key,
                            "recipe_name": name,
                            "error": str(e),
                        }
                        records_generated += 1

            except Exception as e:
                logger.warning(f"Error accessing project {project_key}: {e}")
                continue

        # Process remaining items in the final batch
        if current_batch:
            self._process_batch(current_batch)
            for processed_row in current_batch:
                del processed_row["_raw_code"]
                yield processed_row

    def get_read_schema(self):
        """Returns the read schema for TBD"""
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText, LongMeaning
        return {
            "columns": [
                {"meaning": "Text", "name": "project_key", "type": "string"},
                {"meaning": "Text", "name": "recipe_name", "type": "string"},
                {"meaning": "URL", "name": "url", "type": "string"},
                {"meaning": "Text", "name": "code_env_name", "type": "string"},
                {
                    "meaning": "Text",
                    "name": "code_env_python_version",
                    "type": "string",
                },
                {"meaning": "LongMeaning", "name": "num_lines_of_code", "type": "int"},
                {"meaning": "Text", "name": "last_modified_by_user", "type": "string"},
                {
                    "meaning": "LongMeaning",
                    "name": "last_modified_timestamp",
                    "type": "string",
                },
                {
                    "meaning": "Boolean",
                    "name": "is_likely_unaltered_default_code",
                    "type": "boolean",
                },
                {
                    "meaning": "Text",
                    "name": "min_python_version_from_Vermin",
                    "type": "string",
                },
                {"meaning": "DoubleMeaning", "name": "radon_cc_avg", "type": "double"},
                {
                    "meaning": "DoubleMeaning",
                    "name": "radon_mi_score",
                    "type": "double",
                },
                {"meaning": "Text", "name": "radon_rank", "type": "string"},
                {"meaning": "LongMeaning", "name": "ruff_violations", "type": "int"},
                {"meaning": "LongMeaning", "name": "pylint_issues", "type": "int"},
                {"meaning": "FreeText", "name": "code_sample", "type": "string"},
            ]
        }

    def get_records_count(self, partitioning=None, partition_id=None):
        return None

    def get_partitioning(self):
        raise NotImplementedError

    def list_partitions(self, partitioning):
        return []

    def partition_exists(self, partitioning, partition_id):
        raise NotImplementedError
