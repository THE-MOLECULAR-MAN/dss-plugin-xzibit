"""Standalone utility class for analysing Python recipes in a DSS project.

This module is used for exploratory / debugging purposes outside of the main
connector flow.  Run it from a DSS notebook or scenario.
"""

import dataiku
import tempfile
import os
from typing import Dict, List, Any

try:
    from vermin import detect, Config
except ImportError:
    raise ImportError(
        "The 'vermin' package is not installed. Please add 'vermin' to your requirements."
    )


class RecipeAnalyzer:
    """Analyses all Python recipes in a DSS project for Python version compatibility."""

    def __init__(self, project_key: str = None):
        """Initialise the analyser.

        Args:
            project_key: DSS project key to scan. Defaults to the current project
                when None (uses the standard dataiku.api_client() context).
        """
        self.client = dataiku.api_client()
        if project_key:
            self.project = self.client.get_project(project_key)
        else:
            self.project = self.client.get_default_project()

        self.vermin_config = Config()
        self.vermin_config.set_verbose(0)

    def get_python_recipes(self) -> List[Dict[str, Any]]:
        """Returns metadata dicts for all Python code recipes in the project."""
        recipes = self.project.list_recipes()
        return [r for r in recipes if r["type"] == "python"]

    def get_recipe_code(self, recipe_name: str) -> str:
        """Fetches the Python source of a recipe by name. Returns '' on failure."""
        recipe = self.project.get_recipe(recipe_name)
        settings = recipe.get_settings()
        try:
            return settings.get_code()
        except AttributeError:
            print(f"WARNING: Could not retrieve code for {recipe_name}. "
                  "It may not be a standard code recipe.")
            return ""

    def analyze_code_compatibility(self, code_content: str) -> Dict[str, str]:
        """Runs Vermin on code_content and returns min/incompatible version strings."""
        if not code_content.strip():
            return {"min_versions": "N/A", "incompatible_versions": "N/A"}

        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as tmp_file:
                tmp_file.write(code_content)
                tmp_path = tmp_file.name

            vermin_results = detect(code_content, config=self.vermin_config)

            if isinstance(vermin_results, list):
                mins = vermin_results
                incomp = []
            elif isinstance(vermin_results, tuple):
                mins = vermin_results[0]
                incomp = vermin_results[1]
            else:
                mins = []
                incomp = []

            def _fmt_version(v):
                if v is None:
                    return None
                if isinstance(v, tuple) and len(v) >= 2:
                    return f"{v[0]}.{v[1]}"
                return str(v)

            min_ver_list = [_fmt_version(v) for v in (mins or []) if v is not None]
            incomp_ver_list = [_fmt_version(v) for v in (incomp or []) if v is not None]

            return {
                "min_versions": ", ".join(min_ver_list) if min_ver_list else "Any",
                "incompatible_versions": ", ".join(incomp_ver_list) if incomp_ver_list else "None",
            }

        except Exception as e:
            print(f"ERROR: Error analysing code: {e}")
            return {"min_versions": "Error", "incompatible_versions": "Error"}

        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    def run(self):
        """Scans all Python recipes in the project and prints a compatibility table."""
        print(f"Scanning project key: {self.project.project_key} for Python recipes...")
        python_recipes = self.get_python_recipes()

        if not python_recipes:
            print("No Python recipes found in this project.")
            return

        print(f"{'Recipe Name':<40} | {'Min Required':<20} | {'Incompatible':<20}")
        print("-" * 86)

        for recipe_meta in python_recipes:
            name = recipe_meta["name"]
            code = self.get_recipe_code(name)

            if code:
                result = self.analyze_code_compatibility(code)
                print(
                    f"{name:<40} | {result['min_versions']:<20} | {result['incompatible_versions']:<20}"
                )
            else:
                print(f"{name:<40} | {'Skipped (No Code)':<20} | {'-'}")
