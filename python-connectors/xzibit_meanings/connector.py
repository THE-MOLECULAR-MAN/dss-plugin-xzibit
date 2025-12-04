####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import get_dss_base_url, flatten_dict

# Meanings don't have an obvious, dedicated URL


def get_dataset_url(project_key, dataset_id):
    """TBD"""
    # https://honker-design-2.se-platform.dataiku-sandbox.io/projects/PMMOPTIMIZINGOMNICHANNELMARKETINGLLM/datasets/Sales_Marketing_queries/explore/
    try:
        base_url = get_dss_base_url()
        if base_url is None or project_key is None or dataset_id is None:
            return None
        # trailing slash is MANDATORY
        return f"{base_url}/projects/{project_key}/datasets/{dataset_id}/explore/"
    except Exception:  # yeah, I know this is bad practice
        return None


class ConnectorMeanings(Connector):
    """TBD"""

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)

        self.__client = api_client()
        self.__keys = [
            "label",
            "description",
            "detectable",
            "type",
            "id",
            "normalizationMode",
        ]

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        # iterate through each object
        for item_info in self.__client.list_meanings():
            next_row = flatten_dict(item_info, include_keys=self.__keys)
            yield next_row

    def get_read_schema(self):
        """TBD"""
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText, LongMeaning
        return {
            "columns": [
                {"name": "label", "type": "string", "meaning": "Text"},
                {"name": "description", "type": "string", "meaning": "FreeText"},
                {"name": "detectable", "type": "boolean", "meaning": "Boolean"},
                {"name": "type", "type": "string", "meaning": "Text"},
                {"name": "id", "type": "string", "meaning": "Text"},
                {"name": "normalizationMode", "type": "string", "meaning": "Text"},
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
