"""TBD"""

####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import (
    remove_prefix_from_keys,
    flatten_dict,
    get_dss_base_url,
    recursive_search_all,
    pp,
)


####################################################################
# Unique imports for this Class
####################################################################
from datetime import datetime


class ConnectorProjects(Connector):
    """TBD"""

    ####################################################################
    # Code that has to be customized for this specific class
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        
        records_generated = 0

        # iterate through each object
        for connection_handle in self.__client.list_connections(as_type="objects"):
            try:
                if records_limit > 0 and records_generated >= records_limit:
                    break

                connection_settings = connection_handle.get_settings().get_raw()
                connection_type = connection_settings.get("type", None)
                
                # https://developer.dataiku.com/latest/api-reference/python/connections.html#dataikuapi.dss.admin.DSSConnection
                if connection_type in ["OpenAI", "AzureOpenAI", "VertexAILLM"]:
                    pp(connection_settings)
                    connection_name = connection_settings.get("name", None)
                    connection_params = connection_settings.get("params",{})

                    for k,v in connection_params.items():
                        if k.startswith('allow'):
                            stripped_key = k[5:]
                            next_row = {
                                'connection_name': connection_name,
                                'connection_type': connection_type}
                            
                            next_row['llm_name'] = stripped_key
                            next_row['llm_enabled'] = v  
        
            except Exception as e:
                print(f"[llmmodelsbyconnection-generate_row] UNHANDLED EXCEPTION {e}")

            finally:
                records_generated += 1
                yield next_row
    def get_read_schema(self):
        """Returns the read schema for TBD"""
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText, LongMeaning
        return {
            "columns": [
                {"meaning": "Text", "name": "projectKey", "type": "string"},
                {"meaning": "Text", "name": "dss_object_type", "type": "string"},
                {"meaning": "Text", "name": "dss_object_id", "type": "string"},
                {"meaning": "Text", "name": "dss_object_name", "type": "string"},
                {"meaning": "JSONArrayMeaning", "name": "llmId", "type": "string"},
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
