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
        for item_info in self.__client.list_connections(as_type="objects"):
            try:
                if records_limit > 0 and records_generated >= records_limit:
                    break

                next_row = flatten_dict(item_info, include_keys=keys)
                connection_type = next_row.get("type", "unknown type")
                connection_error_msg = None
                
                # https://developer.dataiku.com/latest/api-reference/python/connections.html#dataikuapi.dss.admin.DSSConnection
                if connection_type in ["OpenAI", "AzureOpenAI", "VertexAILLM"]:
                    connection_handle = self.__client.get_connection(next_row["name"])
                    connection_info = connection_handle.get_info().get_params()
                    connection_settings = connection_handle.get_settings().get_raw()
                    # outputs for these are VERY long
                    print(f"Connection_INFO for {connection_type}")
                    pp(connection_info)
                    print(f"Connection_SETTINGS for {connection_type}")
                    pp(connection_settings)

                next_row["url"] = self.get_url(next_row["name"])

                obj_handle = self.__client.get_connection(next_row["name"])
                connection_test_dict = obj_handle.test()  # error
                if connection_test_dict.get("connectionOK", False):
                    connection_test_result = "PASSED"
                else:
                    connection_test_result = "FAILED"
                    connection_error_msg = connection_test_dict.get(
                        "connectionError", {}
                    ).get("detailedMessage", "Unable to fetch error message")

            except Exception as e:
                # Not all connection types have a .test() method implemented, by design, like filesystem.
                # This catches them and marks their test result as N/A.
                if JAVA_NOT_IMPLEMENTED in str(e):
                    connection_test_result = "NOT_TESTABLE"
                    connection_error_msg = None
                else:
                    print(f"[Connections-generate_row] UNHANDLED EXCEPTION {e}")
                    connection_test_result = "FAILED - EXCEPTION"
                    connection_error_msg = connection_test_dict
                    # pp(connection_test_dict)
            finally:
                next_row["connection_test_status"] = connection_test_result
                next_row["connection_test_error_msg"] = connection_error_msg
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
