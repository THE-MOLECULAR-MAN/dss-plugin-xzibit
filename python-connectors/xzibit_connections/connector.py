"""TBD"""

####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import flatten_dict, get_dss_base_url, pp


class ConnectorConnections(Connector):
    """TBD"""

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
        # Trailing slash for Connections is Mandatory.
        return f"{self.__baseurl}/admin/connections/{id}/"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        keys = [
            "name",  # for Connections, name is the ID, it is immutable after creation
            "type",
            "usableBy",
            "allowWrite",
            "credentialsMode",
            "params.credentialsMode",
            "creationTag.lastModifiedBy.login",
            "creationTag.lastModifiedBy.lastModifiedOn",
            "params.authType",
            "params.db",
            "params.defaultSchema",
            "params.role",
            "params.warehouse",
            "params.scope",
        ]
        records_generated = 0
        # iterate through each object
        for item_info in self.__client.list_connections(as_type="listitems"):
            try:
                if records_limit > 0 and records_generated >= records_limit:
                    break

                next_row = flatten_dict(item_info, include_keys=keys)
                next_row["url"] = self.get_url(next_row["name"])
                
                connection_test_result = 'fail'
                obj_handle = self.__client.get_connection(next_row["name"])
                c = obj_handle.test()
                if c.get("connectionOK",False):
                    connection_test_result = 'pass'
                else:
                    pp(c)
                
                
            except Exception as e:
                print(f"[Connections-generate_row] EXCEPTION {e}")
            finally:
                next_row["connection_test_status"] = connection_test_result
                records_generated += 1
                yield next_row

    def get_read_schema(self):
        """TBD"""
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText, LongMeaning
        return {
            "columns": [
                {"meaning": "Text", "name": "name", "type": "string"},
                {"meaning": "Text", "name": "type", "type": "string"},
                {"meaning": "Text", "name": "credentialsMode", "type": "string"},
                {"meaning": "Text", "name": "connection_test_status", "type": "string"},
                {"meaning": "Text", "name": "usableBy", "type": "string"},
                {"meaning": "Text", "name": "params.credentialsMode", "type": "string"},
                {"meaning": "Text", "name": "params.authType", "type": "string"},
                {"meaning": "Text", "name": "params.scopes", "type": "string"},
                {"meaning": "Text", "name": "params.scope", "type": "string"},
                {"meaning": "Text", "name": "params.warehouse", "type": "string"},
                {"meaning": "Text", "name": "params.defaultSchema", "type": "string"},
                {"meaning": "Text", "name": "params.role", "type": "string"},
                {"meaning": "Text", "name": "params.db", "type": "string"},
                {
                    "meaning": "Text",
                    "name": "creationTag.lastModifiedBy.login",
                    "type": "string",
                },
                {"meaning": "Boolean", "name": "allowWrite", "type": "boolean"},
                {"meaning": "URL", "name": "url", "type": "string"},
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
