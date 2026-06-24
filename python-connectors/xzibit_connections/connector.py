"""Connector that provides a dataset of all Connections on the DSS instance."""

from dataiku import api_client

from xzibit.base_connector import XzibitBaseConnector
from xzibit.utils import (
    flatten_dict,
    get_dss_base_url,
    JAVA_NOT_IMPLEMENTED,
    DataikuException,
    pp,
)


class ConnectorConnections(XzibitBaseConnector):
    """Connector that provides a dataset of all Connections on the DSS instance."""

    def __init__(self, config, plugin_config):
        super().__init__(config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()

    def get_url(self, connection_name):
        """Returns the DSS UI URL for the connection, or None if inputs are missing.

        Connection URLs require a trailing slash.
        """
        if any(v is None for v in (self.__baseurl, connection_name)):
            return None
        return f"{self.__baseurl}/admin/connections/{connection_name}/"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
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
        for item_info in self.__client.list_connections(as_type="listitems"):
            try:
                if records_limit > 0 and records_generated >= records_limit:
                    break

                next_row = flatten_dict(item_info, include_keys=keys)
                next_row["url"] = self.get_url(next_row["name"])

            finally:
                records_generated += 1
                yield next_row

    def get_read_schema(self):
        return {
            "columns": [
                {"meaning": "Text", "name": "name", "type": "string"},
                {"meaning": "Text", "name": "type", "type": "string"},
                {"meaning": "Text", "name": "credentialsMode", "type": "string"},
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
