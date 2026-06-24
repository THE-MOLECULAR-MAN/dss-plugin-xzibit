"""Connector that provides a dataset of all recipes across every project."""

from dataiku import api_client

from xzibit.base_connector import XzibitBaseConnector
from xzibit.utils import get_dss_base_url, get_python_recipe_code_env
from xzibit.deprecations import (
    DEPRECATED_PREPROCESSORS,
    load_local_csv_as_dataframe,
    lookup_recipe_deprecation_status,
)


def get_unique_types(data_list):
    unique_types = {item.get("type") for item in data_list if "type" in item}
    return unique_types


def get_preprocessors_in_prepare_recipe(prepare_recipe_handle):
    try:
        recipe_settings_handle = prepare_recipe_handle.get_settings()
        recipe_type = recipe_settings_handle.type
        if recipe_type == "shaker":  # prepare recipe
            prepare_recipe_payload = recipe_settings_handle.obj_payload
            steps = prepare_recipe_payload.get("steps", [])
            return get_unique_types(steps)
        return
    except Exception:
        return


def prepare_recipe_has_deprecated_preprocessors(prepare_recipe_handle):
    preprocessors_unique = get_preprocessors_in_prepare_recipe(prepare_recipe_handle)
    if isinstance(preprocessors_unique, set):
        found_dep = preprocessors_unique.intersection(DEPRECATED_PREPROCESSORS)
        if isinstance(preprocessors_unique, set) and len(found_dep) > 0:
            return list(found_dep)
    return []


class ConnectorRecipes(XzibitBaseConnector):
    """Connector that provides a dataset of all recipes across every project."""

    def __init__(self, config, plugin_config):
        super().__init__(config, plugin_config)
        self.__client = api_client()
        self.__objects_list = {}
        self.__baseurl = get_dss_base_url()
        self.__df_dss_recipes = load_local_csv_as_dataframe(
            "DSS_recipe_deprecation_status.csv"
        )

        for pk in self.__client.list_project_keys():
            project_handle = self.__client.get_project(pk)
            self.__objects_list[pk] = project_handle.list_recipes(as_type="objects")

    def get_url(self, recipe_id, project_key):
        """Returns the DSS UI URL for the recipe, or None if inputs are missing."""
        if any(v is None for v in (self.__baseurl, recipe_id, project_key)):
            return None
        return f"{self.__baseurl}/projects/{project_key}/recipes/{recipe_id}/"

    def _process_single_recipe(self, pk, r, project_handle):
        """Build a metadata row for a single recipe.

        Inner exceptions are caught and printed so that a failure on one recipe's
        inputs or outputs does not prevent other recipes from being yielded.
        """
        recipe_handle = project_handle.get_recipe(r.id)
        recipe_settings_handle = recipe_handle.get_settings()
        raw_data = recipe_settings_handle.get_recipe_raw_definition()

        next_row = {
            "projectKey": pk,
            "recipe_id": r.id,
            "recipe_type": raw_data["type"],
            "recipe_name": recipe_handle.name,
            "tags": raw_data["tags"],
            "url": self.get_url(r.id, pk),
        }
        try:
            # GUI sometimes raises when iterating inputs for certain recipe types
            # (e.g. Export To Folder with no Parameters dataset configured).
            next_row["engine_parameters"] = raw_data.get("params", {}).get(
                "engineParams", None
            )
            next_row["last_modified_user"] = (
                raw_data.get("versionTag", {})
                .get("lastModifiedBy", {})
                .get("login", None)
            )
            next_row["input_datasets"] = recipe_settings_handle.get_flat_input_refs()

            try:
                next_row["code_env"] = get_python_recipe_code_env(recipe_handle)
                deprecated_preprocessors = prepare_recipe_has_deprecated_preprocessors(
                    recipe_handle
                )
                if len(deprecated_preprocessors) > 0:
                    next_row["deprecation_status"] = (
                        "Prepare recipe uses deprecated preprocessors"
                    )
                else:
                    next_row["deprecation_status"] = lookup_recipe_deprecation_status(
                        next_row["recipe_type"], self.__df_dss_recipes
                    )
                next_row["output_datasets"] = recipe_settings_handle.get_flat_output_refs()
            except Exception as e:
                print(
                    f"[recipes-generate_rows] [EXPECTED EXCEPTION] Exception in Recipe "
                    f"output dataset, project_key: {pk}, recipe_id: {r.id}: {e}"
                )
        except Exception as e:
            print(
                f"[recipes-generate_rows] [EXPECTED EXCEPTION] Exception in Recipe "
                f"input dataset, project_key: {pk}, recipe_id: {r.id}: {e}"
            )

        return next_row

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """Yields one metadata row per recipe across all projects."""
        records_generated = 0
        for pk, proj_recipes in self.__objects_list.items():
            if records_limit > 0 and records_generated >= records_limit:
                return

            project_handle = self.__client.get_project(pk)

            for r in proj_recipes:
                if records_limit > 0 and records_generated >= records_limit:
                    return

                # Initialise with safe defaults so the finally block always has
                # something to yield even if _process_single_recipe raises.
                next_row = {"projectKey": pk, "recipe_id": r.id}
                try:
                    next_row = self._process_single_recipe(pk, r, project_handle)
                except Exception as e:
                    print(
                        f"[recipes-generate_rows] [UNEXPECTED EXCEPTION] "
                        f"project_key: {pk}, recipe_id: {r.id}: {e}"
                    )
                finally:
                    records_generated += 1
                    yield next_row

    def get_read_schema(self):
        return None
