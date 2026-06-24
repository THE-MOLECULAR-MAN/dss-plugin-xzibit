"""Connector that provides a dataset of all custom Meanings on the DSS instance."""

from dataiku import api_client
from dataiku.connector import Connector

from xzibit.base_connector import XzibitBaseConnector
from xzibit.utils import flatten_dict

# Meanings do not have URLs in the DSS UI, so get_dss_base_url is not needed.


class ConnectorMeanings(XzibitBaseConnector, Connector):
    """Connector that provides a dataset of all custom Meanings on the DSS instance."""

    def __init__(self, config, plugin_config):
        super().__init__(config, plugin_config)
        self.__client = api_client()

    def get_url(self):
        """Meanings do not have dedicated URLs in the DSS UI."""
        return None

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        records_generated = 0
        keys = [
            "label",
            "description",
            "detectable",
            "type",
            "id",
            "normalizationMode",
        ]
        for item_info in self.__client.list_meanings():
            if records_limit > 0 and records_generated >= records_limit:
                return
            next_row = flatten_dict(item_info, include_keys=keys)
            records_generated += 1
            yield next_row

    def get_read_schema(self):
        return {
            "columns": [
                {"name": "id", "type": "string", "meaning": "Text"},
                {"name": "label", "type": "string", "meaning": "Text"},
                {"name": "detectable", "type": "boolean", "meaning": "Boolean"},
                {"name": "type", "type": "string", "meaning": "Text"},
                {"name": "normalizationMode", "type": "string", "meaning": "Text"},
                {"name": "description", "type": "string", "meaning": "FreeText"},
                # Meanings do not have URLs
            ]
        }
