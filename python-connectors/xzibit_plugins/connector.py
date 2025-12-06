####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import (
    get_dss_base_url,
    flatten_dict,
    get_values_for_key,
    remove_prefix_from_keys,
    list_to_error_dict,
    pp,
)


class ConnectorPlugins(Connector):

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()

    def get_url(self, id):
        """Create a URL to the DSS object in question in this specific DSS instance.
        Return None if any of the inputs are None."""
        # at least one is None, return None
        if any(v is None for v in (self.__baseurl, id)):
            return None
        return f"{self.__baseurl}/plugins/{id}/summary/"

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

        # iterate through each object
        # list plugins does not take any parameters like object vs list:
        # https://developer.dataiku.com/latest/api-reference/python/client.html#dataikuapi.DSSClient.list_plugins
        # even in the source code, no parameters:
        # https://github.com/dataiku/dataiku-api-client-python/blob/master/dataikuapi/dssclient.py#L273
        for item_info in self.__client.list_plugins():
            try:
                if records_limit > 0 and records_generated >= records_limit:
                    break

                next_row = flatten_dict(item_info, include_keys=keys)
                next_row = remove_prefix_from_keys(next_row, "meta.")
                
                # this is so slow!!!!
                #plugin_handle = self.__client.get_plugin(next_row["id"])
                #list_of_usages = plugin_handle.list_usages().get_raw()["usages"]

#                 if len(list_of_usages) == 0:
#                     next_row["project_usages"] = []
#                 else:
#                     next_row["project_usages"] = list(
#                         get_values_for_key(list_of_usages, "projectKey")
#                     )

#                 next_row["total_usages"] = len(list_of_usages)
                next_row["url"] = self.get_url(next_row["id"])
            except Exception as e:
                print(
                    f"[plugins-generate_rows] [UNEXPECTED EXCEPTION] {e} with plugin {next_row['id']}"
                )
                # pp(item_info)
                next_row = list_to_error_dict(keys)
            finally:
                records_generated += 1
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
#                 {
#                     "name": "project_usages",
#                     "type": "string",
#                     "meaning": "JSONArrayMeaning",
#                 },
                {"name": "isDev", "type": "boolean", "meaning": "Boolean"},
                # {"name": "total_usages", "type": "bigint", "meaning": "LongMeaning"},
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
