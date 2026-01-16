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
    jd,
    get_values_for_key,
)


class ConnectorPlugins(Connector):
    """TBD"""

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        # self.__baseurl = get_dss_base_url()

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        records_generated = 0
        
        # list_plugins does not offer any parameters
        # list_plugins returns a list of dict. Each dict contains at least a ‘id’ field
        for item_info in self.__client.list_plugins():
            try:
                if records_limit > 0 and records_generated >= records_limit:
                    return
                
                plugin_id = item_info.get("id", "NO_PLUGIN_ID")
                plugin_handle = self.__client.get_plugin(plugin_id)
                settings_raw = plugin_handle.get_settings().get_raw()
                list_of_usages = plugin_handle.list_usages()
                
                for usage in list_of_usages.usages:
                    try:
                        if records_limit > 0 and records_generated >= records_limit:
                            return

                        next_row = {"plugin_id":   plugin_id}
                        next_row["element_kind"] =  usages.element_kind
                        next_row["element_type"] =  usages.element_type
                        next_row["object_id"] =   usages.object_id
                        next_row["object_type"] =  usages.object_type
                        next_row["project_key"] =  usages.project_key

                    except Exception as e:
                        print(
                            f"[plugin_usages-generate_rows] ! [UNEXPECTED EXCEPTION] {e} with plugin {next_row['id']}"
                        )
                    finally:
                        records_generated += 1
                        yield next_row
            except Exception as e:
                print(
                    f"[plugin_usages-generate_rows] [UNEXPECTED EXCEPTION] {e} with plugin {next_row['id']}"
                )

    def get_read_schema(self):
        """TBD"""
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText, LongMeaning
        return None

#         return {
#             "columns": [
#                 {"name": "id", "type": "string", "meaning": "Text"},
#                 {"name": "label", "type": "string", "meaning": "Text"},
#                 {"name": "code_env_name", "type": "string", "meaning": "Text"},
#                 {"name": "version", "type": "string", "meaning": "Text"},
#                 {"name": "author", "type": "string", "meaning": "Text"},
#                 {"name": "tags", "type": "string", "meaning": "JSONArrayMeaning"},
#                 {"name": "description", "type": "string", "meaning": "FreeText"},
#                 {
#                     "name": "plugin_used_in_projectkeys",
#                     "type": "string",
#                     "meaning": "JSONArrayMeaning",
#                 },
#                 {"name": "isDev", "type": "boolean", "meaning": "Boolean"},
#                 {"name": "total_usages", "type": "bigint", "meaning": "LongMeaning"},
#                 {"name": "is_built_in_plugin", "type": "boolean", "meaning": "Boolean"},
#                 {"name": "url", "type": "string", "meaning": "URL"},
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
