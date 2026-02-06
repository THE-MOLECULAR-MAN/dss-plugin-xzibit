import dataiku
import tempfile
import os
import logging
from typing import Dict, List, Any

# Vermin imports
try:
    from vermin import detect, Config
except ImportError:
    raise ImportError(
        "The 'vermin' package is not installed. Please add 'vermin' to your requirements."
    )

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger()


class RecipeAnalyzer:
    """
    A class to analyze Dataiku Python recipes for Python version compatibility using Vermin.
    """

    def __init__(self):
        self.client = dataiku.api_client()
        # Use default project context as per instructions
        self.project = self.client.get_project("DEPRECATED_TESTS_DSS_V12")
        self.vermin_config = Config()
        self.vermin_config.set_verbose(0)  # 0 usually suppresses most non-result output

    def get_python_recipes(self) -> List[Dict[str, Any]]:
        """
        Retrieves a list of all Python code recipes in the current project.
        """
        recipes = self.project.list_recipes()
        return [r for r in recipes if r["type"] == "python"]

    def get_recipe_code(self, recipe_name: str) -> str:
        """
        Fetches the actual Python script content from a specific recipe.
        """
        recipe = self.project.get_recipe(recipe_name)
        settings = recipe.get_settings()

        try:
            return settings.get_code()
        except AttributeError:
            logger.warning(
                f"Could not retrieve code for {recipe_name}. It may not be a standard code recipe."
            )
            return ""

    def analyze_code_compatibility(self, code_content: str) -> Dict[str, str]:
        """
        Writes code to a temp file and runs Vermin analysis to find min/incompatible versions.
        """
        if not code_content.strip():
            return {"min_versions": "N/A", "incompatible_versions": "N/A"}

        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".py", delete=False
            ) as tmp_file:
                tmp_file.write(code_content)
                tmp_path = tmp_file.name

            # detect returns a structure; we capture it safely
            vermin_results = detect(code_content, config=self.vermin_config)

            # Inspect structure to handle variations in vermin return values
            # Case A: vermin_results is [(2, 0), None] (The 'mins' list itself)
            if isinstance(vermin_results, list):
                mins = vermin_results
                incomp = (
                    []
                )  # Incompatible info likely not present in this return format
            # Case B: vermin_results is ((mins), (incomp), ...) (Standard Tuple)
            elif isinstance(vermin_results, tuple):
                mins = vermin_results[0]
                incomp = vermin_results[1]
            else:
                mins = []
                incomp = []

            # Safe formatting for Minimum Versions
            # Handles v being None (no req) or a Tuple (major, minor)
            min_ver_list = []
            if mins:
                for v in mins:
                    if v is None:
                        continue
                    if isinstance(v, tuple) and len(v) >= 2:
                        min_ver_list.append(f"{v[0]}.{v[1]}")
                    elif isinstance(v, int):
                        # Fallback if a single int creeps in
                        min_ver_list.append(str(v))

            min_ver_str = ", ".join(min_ver_list) if min_ver_list else "Any"

            # Safe formatting for Incompatible Versions
            incomp_ver_list = []
            if incomp:
                for v in incomp:
                    if v is None:
                        continue
                    # Handle if v is a single int version or tuple
                    ver_str = (
                        f"{v[0]}.{v[1]}"
                        if (isinstance(v, tuple) and len(v) > 1)
                        else str(v)
                    )
                    incomp_ver_list.append(ver_str)

            incomp_ver_str = ", ".join(incomp_ver_list) if incomp_ver_list else "None"

            return {
                "min_versions": min_ver_str,
                "incompatible_versions": incomp_ver_str,
            }

        except Exception as e:
            logger.error(f"Error analyzing code: {e}")
            return {"min_versions": "Error", "incompatible_versions": "Error"}

        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    def run(self):
        """
        Main execution method to list and analyze recipes.
        """
        logger.info(
            f"Scanning project key: {self.project.project_key} for Python recipes..."
        )
        python_recipes = self.get_python_recipes()

        if not python_recipes:
            logger.info("No Python recipes found in this project.")
            return

        # Header for readability
        logger.info(
            f"{'Recipe Name':<40} | {'Min Required':<20} | {'Incompatible':<20}"
        )
        logger.info("-" * 86)

        for recipe_meta in python_recipes:
            name = recipe_meta["name"]
            code = self.get_recipe_code(name)

            if code:
                result = self.analyze_code_compatibility(code)
                logger.info(
                    f"{name:<40} | {result['min_versions']:<20} | {result['incompatible_versions']:<20}"
                )
            else:
                logger.info(f"{name:<40} | {'Skipped (No Code)':<20} | {'-'}")


# # --- Execution Entry Point ---
# if __name__ == "__main__":
#     analyzer = RecipeAnalyzer()
#     analyzer.run()
