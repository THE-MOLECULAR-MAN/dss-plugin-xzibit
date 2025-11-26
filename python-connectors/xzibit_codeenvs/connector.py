####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import *


def get_cluster_url(cluster_id):
    # https://beta-design.se-platform.dataiku-sandbox.io/admin/code-envs/design/python/brave-mcp/    
    base_url = get_dss_base_url()
    if cluster_id is None or base_url is None:
        return None
    return f"{base_url}/admin/clusters/{cluster_id}"




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

            # custom things for this specific class:
            env_lang = next_row["envLang"]
            env_name = next_row["envName"]
            try:
                code_env_handle = self.__client.get_code_env(env_lang, env_name)
                settings = code_env_handle.get_settings().get_raw()
                next_row["corePackagesSet"] = settings.get("desc", []).get(
                    "corePackagesSet", []
                )
                next_row["path"] = settings.get("path", None)

                next_row["disk_size_megabytes"] = get_path_size_megabytes(
                    next_row["path"]
                )

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

    def get_records_count(self, partitioning=None, partition_id=None):
        """TBD"""
        return len(self.__client.list_code_envs())

    def get_read_schema(self):
        """TBD"""
        return None

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
