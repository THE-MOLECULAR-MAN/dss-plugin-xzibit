"""A Dataiku DSS v12 connector that provides a dataset listing all Python Code
Recipes across every project, together with static-analysis metrics for each."""

import tempfile
import os
import json
import subprocess
import hashlib
from typing import Dict, List, Any

from vermin import detect, Config

import radon.complexity as radon_cc
import radon.metrics as radon_mi

from dataiku import api_client
from dataiku.connector import Connector

from xzibit.utils import get_python_recipe_code_env, get_dss_base_url

# Default path to the managed code-env bin directory used by the plugin.
# Fleet Manager deployments typically place it here; override via connector
# param if your instance uses a different layout.
_DEFAULT_BINARY_PATH = (
    "/data/dataiku/dss_data/code-envs/python/plugin_xzibit_managed/bin/"
)


def get_recipe_last_modifier_user(recipe_handle) -> str:
    """Fetches the login of the last user to modify a recipe."""
    try:
        recipe_settings_handle = recipe_handle.get_settings()
        raw_data = recipe_settings_handle.get_recipe_raw_definition()
        return raw_data.get("versionTag", {}).get("lastModifiedBy", {}).get("login", None)
    except Exception:
        return "Unknown"


def get_recipe_last_modified_timestamp(recipe_handle) -> str:
    """Fetches the epoch-millisecond timestamp of the last modification to a recipe."""
    try:
        recipe_settings_handle = recipe_handle.get_settings()
        raw_data = recipe_settings_handle.get_recipe_raw_definition()
        return raw_data.get("versionTag", {}).get("lastModifiedOn", None)
    except Exception:
        return "Unknown"


def get_tuples_only(input_list: list) -> list:
    """Filters a list and returns only elements that are of type tuple."""
    return [item for item in input_list if isinstance(item, tuple)]


class ConnectorPythonAnalysis(Connector):
    """Yields one row per Python Code Recipe, annotated with static-analysis
    metrics from Vermin, Radon, Ruff, and Pylint."""

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.vermin_config = Config()
        self.vermin_config.set_verbose(0)
        self.__baseurl = get_dss_base_url()

        # Configurable via connector param; falls back to the known default path.
        self.__binary_path = config.get("binary_path", _DEFAULT_BINARY_PATH)

        self.batch_size = 50

    def get_url(self, recipe_id, project_key):
        """Returns the DSS UI URL for the recipe, or None if inputs are missing."""
        if any(v is None for v in (self.__baseurl, recipe_id, project_key)):
            return None
        return f"{self.__baseurl}/projects/{project_key}/recipes/{recipe_id}/"

    def get_python_recipes(self, project_handle):
        """Returns a list of recipe metadata dicts for all Python recipes in a project."""
        recipes = project_handle.list_recipes()
        return [r for r in recipes if r["type"] == "python"]

    def get_recipe_code(self, recipe_handle) -> str:
        """Fetches the Python script body from a recipe. Returns '' on failure."""
        try:
            settings = recipe_handle.get_settings()
            return settings.get_code()
        except AttributeError:
            print("WARNING: Could not retrieve code. Recipe may not be a standard code recipe.")
            return ""

    def get_code_env_python_version(self, code_env_name: str) -> str:
        """Returns the Python version string (e.g. '3.9') for a named code environment."""
        try:
            if not code_env_name:
                return ""
            code_env = self.__client.get_code_env("PYTHON", code_env_name)
            settings = code_env.get_settings().get_raw()
            py_interp_version = settings.get("desc", {}).get("pythonInterpreter", "Unknown")

            # Convert PYTHON39 → 3.9
            py_interp_version = py_interp_version.replace("PYTHON", "")
            if len(py_interp_version) >= 2:
                return f"{py_interp_version[0]}.{py_interp_version[1:]}"
            return py_interp_version
        except Exception:
            return "Unknown"

    # -------------------------------------------------------------------------
    # In-Memory Analysis Tools (Fast)
    # -------------------------------------------------------------------------

    def get_code_sample(self, code: str, max_lines: int = 40) -> str:
        """Returns the first max_lines lines of code, with '...' appended if truncated."""
        if not code:
            return ""
        lines = code.splitlines()
        sample = "\n".join(lines[:max_lines])
        if len(lines) > max_lines:
            sample += "\n..."
        return sample

    def check_default_snippets_presence(self, code: str) -> bool:
        """Returns True if code contains any of the DSS default boilerplate snippets.

        Normalises line endings before comparison so the check is OS-independent.
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

        normalized_code = code.replace("\r\n", "\n")
        return any(
            snippet.replace("\r\n", "\n") in normalized_code
            for snippet in default_code_snippets
        )

    def _analyze_vermin(self, code: str) -> str:
        """Runs Vermin on code and returns the minimum required Python version tuple."""
        try:
            mins = detect(code, config=self.vermin_config)
            mins = get_tuples_only(mins)
            if mins:
                return max(mins)
            return "Unknown"
        except Exception as e:
            print(f"WARNING: Vermin analysis failed: {e}")
            return "Error"

    def _analyze_radon(self, code: str) -> Dict[str, Any]:
        """Runs Radon for Cyclomatic Complexity and Maintainability Index."""
        try:
            complexity = radon_cc.cc_visit(code)
            avg_complexity = radon_cc.average_complexity(complexity) if complexity else 0
            mi_score = radon_mi.mi_visit(code, multi=False)
            return {
                "radon_cc_avg": round(avg_complexity, 2),
                "radon_mi_score": round(mi_score, 2),
                "radon_rank": radon_mi.mi_rank(mi_score),
            }
        except Exception as e:
            print(f"WARNING: Radon analysis failed (likely a syntax error): {e}")
            return {"radon_cc_avg": -1, "radon_mi_score": -1, "radon_rank": "Error"}

    # -------------------------------------------------------------------------
    # Batch Processing for Subprocess Tools (Pylint, Ruff)
    # -------------------------------------------------------------------------

    def _process_batch(self, batch_data: List[Dict[str, Any]]) -> None:
        """Runs Ruff and Pylint on a batch of temp files to amortise startup cost.
        Updates each dict in batch_data in-place with violation/issue counts."""
        if not batch_data:
            return

        with tempfile.TemporaryDirectory(prefix="dss_batch_analysis_") as tmp_dir:
            # Write all recipe source files to the temp directory.
            file_map: Dict[str, int] = {}

            for idx, item in enumerate(batch_data):
                safe_name = "".join(
                    c for c in item["recipe_name"] if c.isalnum() or c in (" ", "_", "-")
                ).strip().replace(" ", "_")
                filename = f"{idx}_{safe_name}.py"
                file_path = os.path.join(tmp_dir, filename)
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(item["_raw_code"])
                file_map[filename] = idx

            # ── Ruff ──────────────────────────────────────────────────────
            try:
                ruff_path = os.path.join(self.__binary_path, "ruff")
                cmd = [ruff_path, "check", ".", "--output-format=json"]
                result = subprocess.run(cmd, cwd=tmp_dir, capture_output=True, text=True)
                ruff_data = json.loads(result.stdout)

                counts: Dict[int, int] = {i: 0 for i in range(len(batch_data))}
                for violation in ruff_data:
                    fname = os.path.basename(violation.get("filename", ""))
                    if fname in file_map:
                        counts[file_map[fname]] += 1
                for idx, count in counts.items():
                    batch_data[idx]["ruff_violations"] = count

            except Exception as e:
                print(f"ERROR: Batch Ruff failed: {e}")
                for item in batch_data:
                    item["ruff_violations"] = -1

            # ── Pylint ────────────────────────────────────────────────────
            try:
                pylint_path = os.path.join(self.__binary_path, "pylint")
                cmd = [pylint_path, ".", "--output-format=json", "--recursive=y"]
                result = subprocess.run(cmd, cwd=tmp_dir, capture_output=True, text=True)

                try:
                    pylint_data = json.loads(result.stdout)
                except json.JSONDecodeError:
                    pylint_data = []

                counts = {i: 0 for i in range(len(batch_data))}
                for issue in pylint_data:
                    fname = os.path.basename(issue.get("path", ""))
                    if fname in file_map:
                        counts[file_map[fname]] += 1
                for idx, count in counts.items():
                    batch_data[idx]["pylint_issues"] = count

            except Exception as e:
                print(f"ERROR: Batch Pylint failed: {e}")
                for item in batch_data:
                    item["pylint_issues"] = -1

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """Generator that yields one analysis-result dict per Python recipe."""
        records_generated = 0
        current_batch: List[Dict[str, Any]] = []

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
                            print(f"{name:<40} | Skipped (No Code)")
                            continue

                        row: Dict[str, Any] = {
                            "project_key": project_key,
                            "recipe_name": name,
                            "url": self.get_url(recipe_handle.id, project_key),
                            "_raw_code": code,
                        }

                        code_env_name = get_python_recipe_code_env(recipe_handle)
                        row["code_env_name"] = code_env_name
                        row["code_env_python_version"] = self.get_code_env_python_version(code_env_name)
                        row["num_lines_of_code"] = len(code.splitlines())
                        row["code_hashsum"] = hashlib.md5(code.encode("utf-8")).hexdigest()
                        row["last_modified_by_user"] = get_recipe_last_modifier_user(recipe_handle)
                        row["last_modified_timestamp"] = get_recipe_last_modified_timestamp(recipe_handle)
                        row["is_likely_unaltered_default_code"] = self.check_default_snippets_presence(code)
                        row["code_sample"] = self.get_code_sample(code)

                        mpvv = self._analyze_vermin(code)
                        row["min_python_version_from_Vermin"] = (
                            f"{mpvv[0]}.{mpvv[1]}" if isinstance(mpvv, tuple) else str(mpvv)
                        )

                        is_py3 = (isinstance(mpvv, tuple) and mpvv[0] >= 3) or (
                            isinstance(mpvv, int) and mpvv >= 3
                        )
                        if is_py3:
                            row.update(self._analyze_radon(code))
                        else:
                            row.update({"radon_cc_avg": None, "radon_mi_score": None, "radon_rank": "N/A"})

                        current_batch.append(row)

                        if len(current_batch) >= self.batch_size:
                            self._process_batch(current_batch)
                            for processed_row in current_batch:
                                del processed_row["_raw_code"]
                                records_generated += 1
                                yield processed_row
                            current_batch = []

                    except Exception as e:
                        print(f"ERROR: preparing {name} in {project_key}: {e}")
                        yield {"project_key": project_key, "recipe_name": name, "error": str(e)}
                        records_generated += 1

            except Exception as e:
                print(f"WARNING: Error accessing project {project_key}: {e}")
                continue

        if current_batch:
            self._process_batch(current_batch)
            for processed_row in current_batch:
                del processed_row["_raw_code"]
                yield processed_row

    def get_read_schema(self):
        """Returns the column schema for the output dataset."""
        return {
            "columns": [
                {"meaning": "Text", "name": "project_key", "type": "string"},
                {"meaning": "Text", "name": "recipe_name", "type": "string"},
                {"meaning": "URL", "name": "url", "type": "string"},
                {"meaning": "Text", "name": "code_env_name", "type": "string"},
                {"meaning": "Text", "name": "code_env_python_version", "type": "string"},
                {"meaning": "LongMeaning", "name": "num_lines_of_code", "type": "int"},
                {"meaning": "Text", "name": "last_modified_by_user", "type": "string"},
                {"meaning": "LongMeaning", "name": "last_modified_timestamp", "type": "string"},
                {"meaning": "Boolean", "name": "is_likely_unaltered_default_code", "type": "boolean"},
                {"meaning": "Text", "name": "min_python_version_from_Vermin", "type": "string"},
                {"meaning": "DoubleMeaning", "name": "radon_cc_avg", "type": "double"},
                {"meaning": "DoubleMeaning", "name": "radon_mi_score", "type": "double"},
                {"meaning": "Text", "name": "radon_rank", "type": "string"},
                {"meaning": "LongMeaning", "name": "ruff_violations", "type": "int"},
                {"meaning": "LongMeaning", "name": "pylint_issues", "type": "int"},
                {"meaning": "FreeText", "name": "code_sample", "type": "string"},
                {"meaning": "Text", "name": "code_hashsum", "type": "string"},
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
