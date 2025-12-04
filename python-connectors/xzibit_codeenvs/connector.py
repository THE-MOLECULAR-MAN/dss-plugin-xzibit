####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import get_dss_base_url, flatten_dict, get_values_for_key


def get_codeenv_url(env_name, env_lang="python"):
    # https://beta-design.se-platform.dataiku-sandbox.io/admin/code-envs/design/python/brave-mcp/
    # https://honker-design-2.se-platform.dataiku-sandbox.io/admin/code-envs/design/python/CausalModels
    # https://honker-design-2.se-platform.dataiku-sandbox.io/admin/code-envs/design/python/CausalModels/
    try:
        base_url = get_dss_base_url()
        if base_url is None or env_lang is None or base_url is None:
            return None
        # trailing slash is MANDATORY
        return f"{base_url}/admin/code-envs/design/{env_lang.lower()}/{env_name}/"
    except Exception:  # yeah, I know this is bad practice
        return None


class ConnectorCodeEnvs(Connector):
    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__keys = [
            "envName",
            "envLang",
            "deploymentMode",
            "pythonInterpreter",
            "owner",
        ]

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        # iterate through each object
        for item_info in self.__client.list_code_envs():
            next_row = flatten_dict(item_info, include_keys=self.__keys)

            env_lang = next_row["envLang"]
            env_name = next_row["envName"]
            try:
                next_row["codeenv_url"] = get_codeenv_url(env_name, env_lang)
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
                {"meaning": "URL", "name": "codeenv_url", "type": "string"},
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
    def get_partitioning(self):
        """TBD"""
        raise NotImplementedError

    def list_partitions(self, partitioning):
        """TBD"""
        return []

    def partition_exists(self, partitioning, partition_id):
        """TBD"""
        raise NotImplementedError

    def get_records_count(self, partitioning=None, partition_id=None):
        """This never runs"""
        return None
