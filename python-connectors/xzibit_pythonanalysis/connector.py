"""A Dataiku DSS v12 connector to provide a DSS Dataset listing
all Python Code Recipes, and information about them."""

import tempfile
import os
import logging
import json
import ast
import subprocess
from typing import Dict, List, Any, Tuple

from vermin import detect, Config
import radon.complexity as radon_cc
import radon.metrics as radon_mi

import dataiku
from dataiku import api_client
from dataiku.connector import Connector


from xzibit.utils import get_python_recipe_code_env

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger()


def get_tuples_only(input_list: list) -> list:
    """
    Filters a list to return only elements that are of type tuple.
    """
    return [item for item in input_list if isinstance(item, tuple)]


def format_version_tuple(version_tuple: tuple[int, int]) -> str:
    """
    Takes a tuple of two integers (a, b) and returns a string "a.b".
    """
    a, b = version_tuple
    return f"{a}.{b}"


class ConnectorPythonAnalysis(Connector):
    """A Dataiku DSS v12 connector to provide a DSS Dataset listing
    all Python Code Recipes, and information about them."""

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.vermin_config = Config()
        self.vermin_config.set_verbose(0)

        # Tools configuration can be extended here if we need specific flags
        self.tmp_dir = tempfile.mkdtemp(prefix="dss_recipe_analysis_")

    def get_python_recipes(self, project_handle):
        """
        Retrieves a list of all Python code recipes in the current project.
        """
        recipes = project_handle.list_recipes()
        return [r for r in recipes if r["type"] == "python"]

    def get_recipe_code(self, recipe_handle) -> str:
        """
        Fetches the actual Python script content from a specific recipe.
        """
        try:
            # recipe = project_handle.get_recipe(recipe_name)
            settings = recipe_handle.get_settings()
            return settings.get_code()
        except AttributeError:
            logger.warning(
                f"Could not retrieve code for {recipe_name}. It may not be a standard code recipe."
            )
            return ""

    # def get_recipe_code_env_name(self, recipe_handle) -> str:
    #     """
    #     Fetches the code environment name from a specific recipe.
    #     """
    #     try:
    #         # recipe = self.project.get_recipe(recipe_name)
    #         settings = recipe_handle.get_settings()
    #         return settings.get_code_env_name()
    #     except AttributeError:
    #         logger.warning(
    #             f"Could not retrieve code environment for {recipe_name}. It may not be a standard code recipe."
    #         )
    #         return ""

    def get_code_env_python_version(self, code_env_name: str) -> str:
        """
        Fetches the Python version associated with a given code environment.
        """
        try:
            if not code_env_name:
                return ""
            logger.info(f"Fetching handle for code env {code_env_name}")
            # next line is causing exception
            code_env = self.__client.get_code_env("PYTHON", code_env_name)
            logger.info(
                f"Successfully fetched handle for Python code env {code_env_name}"
            )
            return (
                code_env.get_settings()
                .get_raw()
                .get("desc", {})
                .get("pythonInterpreter", None)
            )

        except Exception:
            logger.warning(
                f"Could not retrieve Python version for code environment {code_env_name}."
            )
            return "Exception"

    def _analyze_vermin(self, code: str) -> str:
        """Run Vermin to detect minimum Python version."""
        try:
            # Vermin expects a path or logic to parse. We use its internal detect.
            # detect returns (mins, parsable, text)
            logger.info(f"_analyze_vermin start")
            mins = detect(code, config=self.vermin_config)
            mins = get_tuples_only(mins)
            # [(2, 0), None]
            # [None, (3, 6)]
            logger.info(f"detect returned data type: {str(type(mins))}")
            if mins:
                # logger.info(f"IF")
                # Returns something like "3.8"
                # WARNING Vermin analysis failed: '>' not supported between instances of 'NoneType' and 'tuple'
                m = format_version_tuple(max(mins))
                # print(mins)
                return m)
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

            # Maintainability Index (A score of 100 is best, 0 is worst)
            mi_score = radon_mi.mi_visit(code, multi=False)

            return {
                "radon_cc_avg": round(avg_complexity, 2),
                "radon_mi_score": round(mi_score, 2),
                "radon_rank": radon_mi.mi_rank(mi_score),
            }
        except Exception as e:
            logger.warning(f"Radon analysis failed: {e}")
            return {"radon_cc_avg": -1, "radon_mi_score": -1, "radon_rank": "Error"}

    def _analyze_dependencies(self, code: str) -> List[str]:
        """
        Extracts imported modules.
        This serves the purpose of dependency analysis (like Deptry/Pipreqs)
        but adapted for single-file scripts without project files.
        """
        imports = set()
        try:
            tree = ast.parse(code)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.add(alias.name.split(".")[0])
                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        imports.add(node.module.split(".")[0])
            return list(imports)
        except SyntaxError:
            return ["<SyntaxError>"]
        except Exception as e:
            logger.warning(f"Dependency analysis failed: {e}")
            return []

    def _run_subprocess_tool(self, cmd: List[str], code: str) -> str:
        """Helper to run CLI tools like Ruff/Pylint against code content."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", dir=self.tmp_dir, delete=False
        ) as tmp:
            tmp.write(code)
            tmp_path = tmp.name

        try:
            # Run the command against the temp file
            result = subprocess.run(
                cmd + [tmp_path],
                capture_output=True,
                text=True,
                check=False,  # We expect non-zero exits from linters
            )
            return result.stdout
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def _analyze_pylint(self, code: str) -> float:
        """Runs Pylint and extracts the global evaluation score."""
        # Pylint is talkative, so we format output to JSON or parse standard out
        # Using a minimal score-only regex or json output is best.
        # Note: 'pylint' must be installed in the code env.
        try:
            # We use a regex to extract the score from standard report if JSON fails,
            # but JSON is safer if available.
            # logger.info(f"_analyze_pylint started")
            # next line is causing exception
            # sudo dnf install pylint # Alma 8
            output = self._run_subprocess_tool(["pylint", "--output-format=json"], code)
            # logger.info(f"finished subprocess for pylint")
            data = json.loads(output)
            # logger.info(f"loaded json for pylint")
            # Pylint JSON export is a list of messages. It doesn't always contain the global score easily.
            # Fallback: Run with report enabled for score extraction is tricky in automation.
            # Strategy: Calculate a naive score or use simple violation count from JSON.
            # Standard Pylint formula: 10.0 - ((float(5 * error + warning + refactor + convention) / statement) * 10)

            # For simplicity in this connector, let's return the count of issues found
            return len(data)
        except json.JSONDecodeError:
            logger.error(f"_analyze_pylint - JSON decode error")
            return -1.0
        except Exception:
            logger.error(f"_analyze_pylint - General exception")
            return -1.0

    def _analyze_ruff(self, code: str) -> int:
        """Runs Ruff and returns total violation count."""
        try:
            output = self._run_subprocess_tool(
                ["ruff", "check", "--output-format=json"], code
            )
            data = json.loads(output)
            return len(data)
        except Exception:
            return -1

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """A generator function that yields rows for the dataset.
        Each row represents a Python code recipe with its analysis."""
        records_generated = 0

        # iterate through each project
        for project_key in self.__client.list_project_keys():
            if records_limit > 0 and records_generated >= records_limit:
                return
            try:
                logger.info(f"starting on project {project_key}")
                project_handle = self.__client.get_project(project_key)
                logger.info(f"fetching python_recipes for {project_key}")

                python_recipes = self.get_python_recipes(project_handle)

                for recipe_meta in python_recipes:
                    name = recipe_meta["name"]
                    logger.info(f"Analyzing Python code for recipe {name}...")

                    recipe_handle = project_handle.get_recipe(name)

                    code = self.get_recipe_code(recipe_handle)

                    if code:
                        # initialize the next row in case of exceptions
                        next_row = {
                            "project_key": project_key,
                            "recipe_name": name,
                        }
                        try:
                            # get additional Dataiku metadata
                            code_env_name = get_python_recipe_code_env(recipe_handle)
                            python_version = self.get_code_env_python_version(
                                code_env_name
                            )
                            next_row["code_env_name"] = code_env_name
                            next_row["python_version"] = python_version

                            # --- Analysis Tools ---

                            # 1. Vermin (Min Python Version)
                            # not working, all rows return "Error"
                            next_row["vermin_min_version"] = self._analyze_vermin(code)

                            # 2. Radon (Complexity)
                            # radon_rank is often returning Error
                            # radon_metrics = self._analyze_radon(code)
                            # next_row.update(radon_metrics)

                            # # 3. Dependencies (AST/Deptry logic)
                            # deps = self._analyze_dependencies(code)
                            # next_row["dependencies_list"] = ",".join(deps)
                            # # dependencies_list returns a syntax error
                            # next_row["dependencies_count"] = len(deps)

                            # # 4. Pylint (Quality) - Returns issue count
                            # FIXED, working.
                            # next_row["pylint_issues"] = self._analyze_pylint(code)

                            # # 5. Ruff (Speed/Style) - Returns violation count
                            # always returns -1.0
                            # next_row["ruff_violations"] = self._analyze_ruff(code)

                        except Exception as e:
                            logger.error(
                                f"Error analyzing {name} in {project_key}: {e}"
                            )
                            next_row["error"] = str(e)
                        finally:
                            # Yield the row with analysis results
                            records_generated += 1
                            yield next_row

                    else:
                        logger.info(f"{name:<40} | {'Skipped (No Code)':<20} | {'-'}")

            except Exception as e:
                # Handle project access permissions or other errors
                logger.warning(f"Error accessing project {project_key}: {e}")
                continue

    def get_read_schema(self):
        """Not needed for this connector."""
        return None

    def get_records_count(self, partitioning=None, partition_id=None):
        """Not needed for this connector."""
        return None

    def get_partitioning(self):
        """Not needed for this connector."""
        raise NotImplementedError

    def list_partitions(self, partitioning):
        """Not needed for this connector."""
        return []

    def partition_exists(self, partitioning, partition_id):
        """Not needed for this connector."""
        raise NotImplementedError
