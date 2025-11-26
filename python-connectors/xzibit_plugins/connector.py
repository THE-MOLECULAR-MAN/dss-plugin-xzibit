####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import *


class ConnectorPlugins(Connector):

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__keys = [
            "id",
            "meta.label",
            "version",
            "meta.author",
            "meta.tags",
            "meta.description",
            "isDev",
        ]

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        # iterate through each object
        for item_info in self.__client.list_plugins():
            try:
                next_row = flatten_dict(item_info, include_keys=self.__keys)
                next_row = remove_prefix_from_keys(next_row, "meta.")
                plugin_handle = self.__client.get_plugin(next_row["id"])
                list_of_usages = plugin_handle.list_usages().get_raw()["usages"]

                if len(list_of_usages) == 0:
                    next_row["project_usages"] = []
                else:
                    next_row["project_usages"] = list(
                        get_values_for_key(list_of_usages, "projectKey")
                    )

                next_row["total_usages"] = len(list_of_usages)
            except Exception as e:
                print(f"Exception {e} with plugin_info:")
                pprint(plugin_info)
                next_row = list_to_error_dict(self.__keys)
            finally:
                yield next_row

    def get_read_schema(self):
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText, LongMeaning
        return {
            "columns": [
                {"name": "id", "type": "string", "meaning": "Text"},
                {"name": "label", "type": "string", "meaning": "Text"},
                {"name": "version", "type": "string", "meaning": "Text"},
                {"name": "author", "type": "string", "meaning": "Text"},
                {"name": "tags", "type": "string", "meaning": "JSONArrayMeaning"},
                {"name": "description", "type": "string", "meaning": "FreeText"},
                {
                    "name": "project_usages",
                    "type": "string",
                    "meaning": "JSONArrayMeaning",
                },
                {"name": "isDev", "type": "boolean", "meaning": "Boolean"},
                {"name": "total_usages", "type": "integer", "meaning": "LongMeaning"},
            ]
        }

    def get_records_count(self, partitioning=None, partition_id=None):
        return len(self.__client.list_plugins())

    ####################################################################
    # Intentionally not implemented, not needed for this type
    ####################################################################
    def get_partitioning(self):
        raise NotImplementedError

    def list_partitions(self, partitioning):
        return []

    def partition_exists(self, partitioning, partition_id):
        raise NotImplementedError
