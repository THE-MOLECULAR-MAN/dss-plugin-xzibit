"""Connector that provides a dataset of all Plugins installed on the DSS instance."""

from dataiku import api_client

from xzibit.base_connector import XzibitBaseConnector
from xzibit.utils import (
    get_dss_base_url,
    flatten_dict,
    remove_prefix_from_keys,
    list_to_error_dict,
    pp,
)

from xzibit.deprecations import DEPRECATED_PLUGIN_IDS, DSS_BUILT_IN_PLUGIN_IDS


def extract_allow_keys(d: dict) -> dict:
    """Filters a dictionary for keys starting with 'allow' and returns a new
    dictionary with the 'allow' prefix stripped from the keys.

    Args:
        d (dict): The input dictionary.

    Returns:
        dict: A new dictionary with transformed keys.
    """
    if not isinstance(d, dict):
        raise ValueError("Input must be a dictionary.")

    # len('allow') is 5, so [5:] removes the prefix
    return {key[5:]: value for key, value in d.items() if key.startswith("allow")}


class ConnectorPlugins(XzibitBaseConnector):
    """Connector that provides a dataset of all Plugins installed on the DSS instance."""

    def __init__(self, config, plugin_config):
        super().__init__(config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()

    def get_url(self, plugin_id):
        """Returns the DSS UI URL for the plugin, or None if inputs are missing."""
        if any(v is None for v in (self.__baseurl, plugin_id)):
            return None
        return f"{self.__baseurl}/plugins/{plugin_id}/summary/"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
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

        # list_plugins does not offer any parameters
        # list_plugins returns a list of dict. Each dict contains at least a 'id' field
        for item_info in self.__client.list_plugins():
            try:
                if records_limit > 0 and records_generated >= records_limit:
                    return

                plugin_id = item_info.get("id", "NO_PLUGIN_ID")

                next_row = flatten_dict(item_info, include_keys=keys)
                next_row = remove_prefix_from_keys(next_row, "meta.")

                next_row["url"] = self.get_url(plugin_id)
                next_row["is_built_in_plugin"] = plugin_id in DSS_BUILT_IN_PLUGIN_IDS

                plugin_handle = self.__client.get_plugin(plugin_id)
                settings_raw = plugin_handle.get_settings().get_raw()
                next_row["code_env_name"] = settings_raw.get("codeEnvName", None)

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
