"""TBD"""

####################################################################
# Same imports for all dataset Classes
####################################################################

from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import (
    get_dss_base_url,
    flatten_dict,
    remove_prefix_from_keys,
    list_to_error_dict,
    pp,
)

from xzibit.deprecations import DEPRECATED_PLUGIN_IDS, DSS_BUILT_IN_PLUGIN_IDS


def extract_allow_keys(d: dict) -> dict:
    """
    Filters a dictionary for keys starting with 'allow' and returns a new
    dictionary with the 'allow' prefix stripped from the keys.

    Args:
        d (dict): The input dictionary.

    Returns:
        dict: A new dictionary with transformed keys.
    """
    if not isinstance(d, dict):
        raise ValueError("Input must be a dictionary.")

    # Iterate through items, check prefix, and slice the key string
    # len('allow') is 5, so [5:] removes the prefix
    return {key[5:]: value for key, value in d.items() if key.startswith("allow")}


class ConnectorPlugins(Connector):
    """TBD"""

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()

    def get_url(self, plugin_id):
        """Create a URL to the DSS object in question in this specific DSS instance.
        Return None if any of the inputs are None."""
        # at least one is None, return None
        if any(v is None for v in (self.__baseurl, plugin_id)):
            return ""
        return f"{self.__baseurl}/plugins/{plugin_id}/summary/"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        records_generated = 0
        keys = [
            "id",
            "meta.label",
            "version",
            "meta.author",
            "meta.tags",
            "meta.description",
            "isDev",
        ]

        # iterate through each object
        # list plugins does not take any parameters like object vs list:
        # https://developer.dataiku.com/latest/api-reference/python/client.html#dataikuapi.DSSClient.list_plugins
        # even in the source code, no parameters:
        # https://github.com/dataiku/dataiku-api-client-python/blob/master/dataikuapi/dssclient.py#L273
        # There's not an easy way to speed up the next, very slow line

        # list_plugins does not offer any parameters
        # list_plugins returns a list of dict. Each dict contains at least a ‘id’ field
        for item_info in self.__client.list_plugins():
            try:
                if records_limit > 0 and records_generated >= records_limit:
                    return

                plugin_id = item_info.get("id", "NO_PLUGIN_ID")
                # print(f"[plugins-generate_rows] Start plugin ID: {plugin_id}")

                next_row = flatten_dict(item_info, include_keys=keys)
                next_row = remove_prefix_from_keys(next_row, "meta.")

                next_row["url"] = self.get_url(plugin_id)
                next_row["is_built_in_plugin"] = plugin_id in DSS_BUILT_IN_PLUGIN_IDS

                plugin_handle = self.__client.get_plugin(plugin_id)
                settings_raw = plugin_handle.get_settings().get_raw()
                # pp(settings_raw)
                next_row["code_env_name"] = settings_raw.get("codeEnvName", None)

                # TODO: replace with CSV file method
                next_row["plugin_is_deprecated"] = plugin_id in DEPRECATED_PLUGIN_IDS

            except Exception as e:
                print(
                    f"[plugins-generate_rows] [UNEXPECTED EXCEPTION] {e} with plugin {next_row['id']}"
                )
                next_row = list_to_error_dict(keys)
            finally:
                records_generated += 1
                yield next_row

    def get_read_schema(self):
        """TBD"""
        return {
            "columns": [
                {"name": "id", "type": "string", "meaning": "Text"},
                {"name": "label", "type": "string", "meaning": "Text"},
                {"name": "code_env_name", "type": "string", "meaning": "Text"},
                {"name": "version", "type": "string", "meaning": "Text"},
                {"name": "author", "type": "string", "meaning": "Text"},
                {"name": "tags", "type": "string", "meaning": "JSONArrayMeaning"},
                {"name": "description", "type": "string", "meaning": "FreeText"},
                {"name": "isDev", "type": "boolean", "meaning": "Boolean"},
                {"name": "is_built_in_plugin", "type": "boolean", "meaning": "Boolean"},
                {
                    "name": "plugin_is_deprecated",
                    "type": "boolean",
                    "meaning": "Boolean",
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
