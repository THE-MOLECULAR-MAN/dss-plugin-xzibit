####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import get_dss_base_url


class ConnectorRecipes(Connector):
    """TBD"""

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__objects_list = {}
        self.__baseurl = get_dss_base_url()

        for pk in self.__client.list_project_keys():
            project_handle = self.__client.get_project(pk)
            self.__objects_list[pk] = project_handle.list_recipes(as_type="objects")

    def get_url(self, id, project_key):
        """Create a URL to the DSS object in question in this specific DSS instance.
        Return None if any of the inputs are None."""
        # at least one is None, return None
        if any(v is None for v in (self.__baseurl, id, project_key)):
            return None
        return f"{self.__baseurl}/projects/{project_key}/recipes/{id}/"

    def get_recipe_url(self, project_key, recipe_id):
        """TBD"""
        # https://honker-design-2.se-platform.dataiku-sandbox.io/projects/PMMOPTIMIZINGOMNICHANNELMARKETINGLLM/recipes/compute_Product_sales_by_acc_joined/
        try:
            if self.__baseurl is None or project_key is None or recipe_id is None:
                return None
            return f"{self.__baseurl}/projects/{project_key}/recipes/{recipe_id}/"
        except Exception:  # yeah, I know this is bad practice
            return None

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        # iterate through each object
        for pk, proj_recipes in self.__objects_list.items():
            project_handle = self.__client.get_project(pk)

            for r in proj_recipes:
                recipe_handle = project_handle.get_recipe(r.id)
                recipe_settings_handle = recipe_handle.get_settings()
                raw_data = recipe_settings_handle.get_recipe_raw_definition()

                next_row = {
                    "projectKey": pk,
                    "id": r.id,
                    "type": raw_data["type"],
                    "name": recipe_handle.name,
                    "tags": raw_data["tags"],
                    # "url_recipe": self.get_recipe_url(pk, r.id),
                    "url": self.get_url(r.id, pk),
                }
                try:
                    next_row["input_datasets"] = (
                        recipe_settings_handle.get_flat_input_refs()
                    )
                    next_row["output_datasets"] = (
                        recipe_settings_handle.get_flat_output_refs()
                    )
                except Exception:
                    print("Exception in Recipe input/output datasets.")
                finally:
                    yield next_row

    def get_read_schema(self):
        """TBD"""
        return {
            "columns": [
                {"name": "projectKey", "type": "string", "meaning": "Text"},
                {"name": "id", "type": "string", "meaning": "Text"},
                {"name": "type", "type": "string", "meaning": "Text"},
                {"name": "name", "type": "string", "meaning": "Text"},
                {"name": "tags", "type": "string", "meaning": "JSONArrayMeaning"},
                {
                    "name": "input_datasets",
                    "type": "string",
                    "meaning": "JSONArrayMeaning",
                },
                {
                    "name": "output_datasets",
                    "type": "string",
                    "meaning": "JSONArrayMeaning",
                },
                {"name": "url", "type": "string", "meaning": "URL"},
            ]
        }

    ####################################################################
    # Intentionally not implemented, not needed for this type
    ####################################################################
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
