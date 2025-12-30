"""TBD"""

####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import get_dss_base_url, flatten_dict, get_values_for_key, get_path_size_megabytes, pp


class ConnectorCodeEnvs(Connector):
    """TBD"""

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()
        self.__compute_codeenv_disk_space_usage = self.config.get("compute_codeenv_disk_space_usage", False)
        # self.__include_usages = False # for possible future configuration option

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
            try:
                if records_limit > 0 and records_generated >= records_limit:
                    return
                settings = code_env_handle.get_settings()

                code_env_name = settings.env_name
                code_env_lang = settings.env_lang

                next_row = {
                    "code_env_name": code_env_name,
                    "code_env_lang": code_env_lang,
                }
                next_row["url"] = self.get_url(code_env_name, code_env_lang)

                settings_raw = settings.get_raw()
                # pp(settings_raw)
                next_row["deployment_mode"] = settings_raw.get("deploymentMode", None)
                next_row["python_interpreter"] = settings_raw.get("desc", {}).get(
                    "pythonInterpreter", None
                )
                next_row["owner"] = settings_raw.get("owner", None)
                next_row["core_packages_set"] = settings_raw.get("desc", {}).get(
                    "corePackagesSet", None
                )
                next_row["path"] = settings_raw.get("path", None)
                
                if self.__compute_codeenv_disk_space_usage and next_row["path"]:
                    if next_row["path"]:
                        
                    x = get_path_size_megabytes()
                else:
                    next_row["size_in_MB"] = None


                # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                # adding list_usages for code environments on DevDesign (600 code env at an
                # average of 30 sec per code env to list all its usages across 2,362 projects), increases
                # the dataset's built time from 2 min 30 sec to 5 hours!!!
                # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            #                if self.__include_usages:
            #                 print("starting code env list usages")
            #                 usages = code_env_handle.list_usages()
            #                 print("finished code env list usages")
            #                 num_usages = len(usages)
            #                 if len(usages) == 0:
            #                     pk_usages = None
            #                 else:
            #                     pk_usages = list(
            #                         get_values_for_key(usages, "projectKey")
            #                      )
            #                 next_row["project_keys_where_plugin_used"] = pk_usages
            #                 next_row["num_projects_that_use_this_plugin"] = pk_usages

            except Exception as e:
                print(f"codeenvs - generate_rows EXCEPTION: {e}")
            finally:
                records_generated += 1
                yield next_row

    def get_read_schema(self):
        """TBD"""
        return {
            "columns": [
                {"meaning": "Text", "name": "code_env_name", "type": "string"},
                {"meaning": "Text", "name": "code_env_lang", "type": "string"},
                {"meaning": "Text", "name": "deployment_mode", "type": "string"},
                {"meaning": "Text", "name": "owner", "type": "string"},
                {"meaning": "Text", "name": "python_interpreter", "type": "string"},
                {"meaning": "Text", "name": "core_packages_set", "type": "string"},
                {"meaning": "Text", "name": "path", "type": "string"},
                {"meaning": "URL", "name": "url", "type": "string"},
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
