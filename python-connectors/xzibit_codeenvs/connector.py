####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import get_dss_base_url, flatten_dict, get_values_for_key


class ConnectorCodeEnvs(Connector):
    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()

    def get_url(self, env_name, env_lang="python"):
        """Create a URL to the DSS object in question in this specific DSS instance.
        Return None if any of the inputs are None."""
        # at least one is None, return None
        if any(v is None for v in (self.__baseurl, env_lang, env_name)):
            return None
        # trailing slash is MANDATORY for Code Envs
        return f"{self.__baseurl}/admin/code-envs/design/{env_lang.lower()}/{env_name}/"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        records_generated = 0
        
        for code_env_handle in self.__client.list_code_envs(as_objects=True):
            if records_limit > 0 and records_generated >= records_limit:
                break
            settings = code_env_handle.get_settings()

            next_row = {
                "envLang": settings.env_lang,
                "env_name": settings.env_name
            }
            records_generated += 1
            yield next_row

    
#     def generate_rows(
#         self,
#         dataset_schema=None,
#         dataset_partitioning=None,
#         partition_id=None,
#         records_limit=-1,
#     ):
#         """TBD"""
#         records_generated = 0
#         keys = [
#             "envName",
#             "envLang",
#             "deploymentMode",
#             "pythonInterpreter",
#             "owner",
#         ]

#         # iterate through each object
#         for item_info in self.__client.list_code_envs():
#             if records_limit > 0 and records_generated >= records_limit:
#                 break

#             next_row = flatten_dict(item_info, include_keys=keys)

#             env_lang = next_row["envLang"]
#             env_name = next_row["envName"]
#             try:
#                 # next_row["codeenv_url"] = self.get_codeenv_url(env_name, env_lang)
#                 next_row["url"] = self.get_url(env_name, env_lang)
#                 code_env_handle = self.__client.get_code_env(env_lang, env_name)
#                 settings = code_env_handle.get_settings().get_raw()
#                 next_row["corePackagesSet"] = settings.get("desc", []).get(
#                     "corePackagesSet", []
#                 )
#                 next_row["path"] = settings.get("path", None)

#                 #                 next_row["disk_size_megabytes"] = get_path_size_megabytes(
#                 #                     next_row["path"]
#                 #                 )

#                 list_of_usages = code_env_handle.list_usages()

#                 if len(list_of_usages) == 0:
#                     next_row["usages"] = []
#                 else:
#                     next_row["usages"] = list(
#                         get_values_for_key(list_of_usages, "projectKey")
#                     )

#             except Exception as e:
#                 print(
#                     f"[codeenvs-generate_rows] [UNEXPECTED EXCEPTION] {e} on {env_name}"
#                 )

#             finally:
#                 # return a single row
#                 records_generated += 1
#                 yield next_row

    def get_read_schema(self):
        """TBD"""
        return None
#         return {
#             "columns": [
#                 {"meaning": "Text", "name": "envName", "type": "string"},
#                 {"meaning": "Text", "name": "envLang", "type": "string"},
#                 {"meaning": "Text", "name": "deploymentMode", "type": "string"},
#                 {"meaning": "Text", "name": "owner", "type": "string"},
#                 {"meaning": "Text", "name": "pythonInterpreter", "type": "string"},
#                 {"meaning": "Text", "name": "corePackagesSet", "type": "string"},
#                 {"meaning": "Text", "name": "path", "type": "string"},
#                 #              {'meaning': 'DoubleMeaning',
#                 #               'name': 'disk_size_megabytes',
#                 #               'type': 'double'},
#                 {"meaning": "JSONArrayMeaning", "name": "usages", "type": "string"},
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
