"""TBD"""

import dataiku
import tempfile
import os
import logging
from typing import Dict, List, Any

from vermin import detect, Config

from dataiku import api_client
from dataiku.connector import Connector

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger()


class ConnectorPythonAnalysis(Connector):
    """A Dataiku DSS v12 connector to provide a DSS Dataset listing
    all Python Code Recipes, and information about them."""

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.vermin_config = Config()
        self.vermin_config.set_verbose(0)  # 0 usually suppresses most non-result output

    def get_python_recipes(self, project_handle):
        """
        Retrieves a list of all Python code recipes in the current project.
        """
        recipes = project_handle.list_recipes()
        return [r for r in recipes if r["type"] == "python"]

    def get_recipe_code(self, recipe_name: str) -> str:
        """
        Fetches the actual Python script content from a specific recipe.
        """
        try:
            recipe = self.project.get_recipe(recipe_name)
            settings = recipe.get_settings()
            return settings.get_code()
        except AttributeError:
            logger.warning(
                f"Could not retrieve code for {recipe_name}. It may not be a standard code recipe."
            )
            return ""

    def get_recipe_code_env_name(self, recipe_name: str) -> str:
        """
        Fetches the code environment name from a specific recipe.
        """
        try:
            recipe = self.project.get_recipe(recipe_name)
            settings = recipe.get_settings()
            return settings.get_code_env_name()
        except AttributeError:
            logger.warning(
                f"Could not retrieve code environment for {recipe_name}. It may not be a standard code recipe."
            )
            return ""

    def get_code_env_python_version(self, code_env_name: str) -> str:
        """
        Fetches the Python version associated with a given code environment.
        """
        try:
            code_env = self.__client.get_code_env(code_env_name)
            details = code_env.get_details()
            return details.get("pythonVersion", "Unknown")
        except Exception:
            logger.warning(
                f"Could not retrieve Python version for code environment {code_env_name}."
            )
            return "Unknown"

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
        for project_key in self.__client.list_projects():
            if records_limit > 0 and records_generated >= records_limit:
                return
            try:
                project_handle = self.__client.get_project(project_key)
                python_recipes = self.get_python_recipes(project_handle)

                for recipe_meta in python_recipes:
                    name = recipe_meta["name"]
                    code = self.get_recipe_code(name)

                    if code:
                        # initialize the next row in case of exceptions
                        next_row = {
                            "project_key": project_key,
                            "recipe_name": name,
                        }
                        try:
                            # vermin analysis for minimum Python versions
                            vermin_analysis = self.analyze_code_compatibility(code)

                            # pylint analysis for quality score
                            # ... (not implemented here, but would go similarly)

                            # ruff analysis for speed/style
                            # ... (not implemented here, but would go similarly)

                            # radon analysis for complexity
                            # ... (not implemented here, but would go similarly)

                            # deptry analysis for dependencies
                            # ... (not implemented here, but would go similarly)

                        except Exception as e:
                            logger.error(
                                f"Error analyzing {name} in {project_key}: {e}"
                            )
                        finally:
                            # Yield the row with analysis results
                            records_generated += 1
                            yield next_row

                    else:
                        logger.info(f"{name:<40} | {'Skipped (No Code)':<20} | {'-'}")

            except Exception as e:
                print(f"Error accessing project {project_key}: {e}")
                continue

    ####################################################################
    # Intentionally not implemented, not needed for this type
    ####################################################################

    def get_read_schema(self):
        """TBD"""
        return None

    def get_records_count(self, partitioning=None, partition_id=None):
        """This never runs for anything that I can find."""
        return None

    def get_partitioning(self):
        """TBD"""
        raise NotImplementedError

    def list_partitions(self, partitioning):
        """TBD"""
        return []

    def partition_exists(self, partitioning, partition_id):
        """TBD"""
        raise NotImplementedError
