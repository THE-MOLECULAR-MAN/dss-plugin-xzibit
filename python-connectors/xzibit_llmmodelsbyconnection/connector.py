"""TBD"""

####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import (
    get_dss_base_url,
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
        # https://developer.dataiku.com/latest/api-reference/python/connections.html#dataikuapi.dss.admin.DSSConnection
        for connection_handle in self.__client.list_connections(as_type="objects"):
            try:
                if records_limit > 0 and records_generated >= records_limit:
                    break

                connection_settings = connection_handle.get_settings().get_raw()
                connection_type = connection_settings.get("type", None)

                connection_name = connection_settings.get("name", None)
                connection_params = connection_settings.get("params", {})

                for k, v in connection_params.items():
                    if k.startswith("allow") and k != "allowFinetuning":
                        try:
                            next_row = {
                                "connection_name": connection_name,
                                "connection_type": connection_type,
                            }

                            stripped_key = k[5:]
                            next_row["llm_name"] = stripped_key
                            next_row["llm_enabled"] = v
                        except Exception as e:
                            print(
                                f"[llmmodelsbyconnection-generate_row] UNHANDLED EXCEPTION at model level {e}"
                            )
                        finally:
                            records_generated += 1
                            yield next_row

            except Exception as e:
                print(
                    f"[llmmodelsbyconnection-generate_row] UNHANDLED EXCEPTION at connection level: {e}"
                )
                continue

    def get_read_schema(self):
        """Returns the read schema for TBD"""
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText, LongMeaning
        return {
            "columns": [
                {"meaning": "Text", "name": "connection_name", "type": "string"},
                {"meaning": "Text", "name": "connection_type", "type": "string"},
                {"meaning": "Text", "name": "llm_name", "type": "string"},
                {"meaning": "Boolean", "name": "llm_enabled", "type": "boolean"},
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
