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

    def get_codeenv_url(self, env_name, env_lang="python"):
        # https://beta-design.se-platform.dataiku-sandbox.io/admin/code-envs/design/python/brave-mcp/
        # https://honker-design-2.se-platform.dataiku-sandbox.io/admin/code-envs/design/python/CausalModels
        # https://honker-design-2.se-platform.dataiku-sandbox.io/admin/code-envs/design/python/CausalModels/
        try:
            if self.__baseurl is None or env_lang is None or env_name is None:
                return None
            # trailing slash is MANDATORY
            return f"{self.__baseurl}/admin/code-envs/design/{env_lang.lower()}/{env_name}/"
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
        keys = [
            "envName",
            "envLang",
            "deploymentMode",
            "pythonInterpreter",
            "owner",
        ]

        # iterate through each object
        for item_info in self.__client.list_code_envs():
            next_row = flatten_dict(item_info, include_keys=keys)

            env_lang = next_row["envLang"]
            env_name = next_row["envName"]
            try:
                # next_row["codeenv_url"] = self.get_codeenv_url(env_name, env_lang)
                next_row["url"] = self.get_url(env_name, env_lang)
                code_env_handle = self.__client.get_code_env(env_lang, env_name)
                settings = code_env_handle.get_settings().get_raw()
                next_row["corePackagesSet"] = settings.get("desc", []).get(
                    "corePackagesSet", []
                )
                next_row["path"] = settings.get("path", None)

                #                 next_row["disk_size_megabytes"] = get_path_size_megabytes(
                #                     next_row["path"]
                #                 )

                list_of_usages = code_env_handle.list_usages()

                if len(list_of_usages) == 0:
                    next_row["usages"] = []
                else:
                    next_row["usages"] = list(
                        get_values_for_key(list_of_usages, "projectKey")
                    )

            except Exception as e:
                print(f"Exception {e} with code_env_info:")
                # pp(item_info)

            finally:
                # return a single row
                yield next_row

    def get_read_schema(self):
        """TBD"""
        return {
            "columns": [
                {"meaning": "Text", "name": "envName", "type": "string"},
                {"meaning": "Text", "name": "envLang", "type": "string"},
                {"meaning": "Text", "name": "deploymentMode", "type": "string"},
                {"meaning": "Text", "name": "owner", "type": "string"},
                {"meaning": "Text", "name": "pythonInterpreter", "type": "string"},
                # {"meaning": "URL", "name": "codeenv_url", "type": "string"},
                {"meaning": "URL", "name": "url", "type": "string"},
                {"meaning": "Text", "name": "corePackagesSet", "type": "string"},
                {"meaning": "Text", "name": "path", "type": "string"},
                #              {'meaning': 'DoubleMeaning',
                #               'name': 'disk_size_megabytes',
                #               'type': 'double'},
                {"meaning": "JSONArrayMeaning", "name": "usages", "type": "string"},
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
