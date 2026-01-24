"""TBD"""

####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import get_dss_base_url, pp

def get_unique_types(data_list):
    unique_types = {item.get('type') for item in data_list if 'type' in item}
    return unique_types

def get_preprocessors_in_prepare_recipe(prepare_recipe_handle):
    try:
        recipe_settings_handle = recipe_handle.get_settings()
        recipe_type = recipe_settings_handle.type
        if recipe_type == 'shaker': # prepare recipe
            prepare_recipe_payload = recipe_settings_handle.obj_payload
            steps = prepare_recipe_payload.get("steps",[])
            return get_unique_types(steps)
        return
    except:
        return
    
def prepare_recipe_has_deprecated_preprocessors(prepare_recipe_handle):
    deprecated_preprocessors = {'AnonymizerProcessor', 'MemoryEquiJoiner', 'MemoryEquiJoinerFuzzy', 'UseRowAsHeader', 'NearestNeighbourGeoJoiner'}
    preprocessors_unique = get_preprocessors_in_prepare_recipe(prepare_recipe_handle)
    if isinstance(preprocessors_unique, set):
        found_dep = preprocessors_unique.intersection(deprecated_preprocessors)
        if isinstance(preprocessors_unique, set) and len(found_dep) > 0:
            return f"Deprecated preprocessors found: {found_dep}"
    return None


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

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        records_generated = 0
        # iterate through each object
        for pk, proj_recipes in self.__objects_list.items():
            if records_limit > 0 and records_generated >= records_limit:
                return

            project_handle = self.__client.get_project(pk)

            for r in proj_recipes:
                if records_limit > 0 and records_generated >= records_limit:
                    return
                
                print(f"[recipes-generate_rows] START LOOP")

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
                    "deprecated_prepare_steps_found": "INITIALIZED"
                }
                try:
                    # GUI produces this error message when visiting this recipe's inputs/utputs
                    # An invalid argument has been encountered : Failed to iterate, caused by: IllegalArgumentException: No parameters dataset selected for repeating dataset/recipe
                    # Seems to happen with the Export To Folder recipe, which exports files to folder.
                    # if the user has not set the "Parameters dataset" option for this recipe, or maybe if that dataset has been deleted, then it will throw an exception.

                    next_row["engine_parameters"] = raw_data.get("params", {}).get(
                        "engineParams", None
                    )
                    next_row["deprecated_prepare_steps_found"]: "b"
                    next_row["last_modified_user"] = (
                        raw_data.get("versionTag", {})
                        .get("lastModifiedBy", {})
                        .get("login", None)
                    )
                    next_row["deprecated_prepare_steps_found"]: "c"
                    next_row["input_datasets"] = (
                        recipe_settings_handle.get_flat_input_refs()
                    )
                    next_row["deprecated_prepare_steps_found"]: "d"                    
                    try:
                        next_row["output_datasets"] = (
                            recipe_settings_handle.get_flat_output_refs()
                        )
                        next_row["deprecated_prepare_steps_found"]= "about to run"
                        
                        #next_row["deprecated_prepare_steps_found"] = prepare_recipe_has_deprecated_preprocessors(recipe_handle)
                        #next_row["deprecated_prepare_steps_found"]= "ran without exception"
                    except Exception as e:
                        next_row["deprecated_prepare_steps_found"] = "exception 1"                        
                        # this occurs often on Dev-Design.
                        print(
                            f"[recipes-generate_rows] [EXPECTED EXCEPTION] Exception in Recipe output dataset, project_key: {pk}, recipe_id: {r.id}: {e}"
                        )
                except Exception as e:
                    # this occurs often on Dev-Design.
                    next_row["deprecated_prepare_steps_found"] = "exception 2"                        
                    print(
                        f"[recipes-generate_rows] [EXPECTED EXCEPTION] Exception in Recipe input dataset, project_key: {pk}, recipe_id: {r.id}: {e}"
                    )
                finally:
                    records_generated += 1
                    yield next_row

    def get_read_schema(self):
        """Returns the read schema for TBD"""
        return None
#         return {
#             "columns": [
#                 {"meaning": "Text", "name": "recipe_id", "type": "string"},
#                 {"meaning": "Text", "name": "recipe_name", "type": "string"},
#                 {"meaning": "Text", "name": "projectKey", "type": "string"},
#                 {"meaning": "Text", "name": "recipe_type", "type": "string"},
#                 {
#                     "meaning": "JSONArrayMeaning",
#                     "name": "input_datasets",
#                     "type": "string",
#                 },
#                 {
#                     "meaning": "JSONArrayMeaning",
#                     "name": "output_datasets",
#                     "type": "string",
#                 },
#                 {"meaning": "JSONArrayMeaning", "name": "tags", "type": "string"},
#                 {
#                     "meaning": "JSONObjectMeaning",
#                     "name": "engine_parameters",
#                     "type": "string",
#                 },
#                 {"meaning": "Text", "name": "last_modified_user", "type": "string"},
#                 {"meaning": "Text", "name": "deprecated_prepare_steps_found", "type": "string"},
#                 {"meaning": "URL", "name": "url", "type": "string"},
#             ]
#         }

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
