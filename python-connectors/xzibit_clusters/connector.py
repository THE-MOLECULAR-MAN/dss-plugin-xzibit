####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import *

def get_cluster_url(cluster_id):
    # https://beta-design.se-platform.dataiku-sandbox.io/admin/clusters/k8s-gpu-small
    base_url = get_dss_base_url()
    if cluster_id is None or base_url is None:
        return None
    return f"{base_url}/admin/clusters/{cluster_id}"


class ConnectorClusters(Connector):
    """TBD"""

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__keys = [
            "id",
            "architecture",
            "name",
            "owner",
            "state",
            "type",
            "usedInProjects",
            "usedInScenarios",
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
        for item_info in self.__client.list_clusters():
            next_row = flatten_dict(item_info, include_keys=self.__keys)
            # return a single row
            next_row['cluster_url'] = get_cluster_url(next_row['id'])
            yield next_row

    ####################################################################
    # Same for all instances:
    ####################################################################
    def get_records_count(self, partitioning=None, partition_id=None):
        """
        Length of the list of items
        """
        return len(self.__client.list_clusters())

    ####################################################################
    # Intentionally not implemented, not needed for this type
    ####################################################################
    def get_partitioning(self):
        """TBD"""
        raise NotImplementedError

    def list_partitions(self, partitioning):
        """TBD"""
        return []

    def partition_exists(self, partitioning, partition_id):
        """TBD"""
        raise NotImplementedError

    def get_read_schema(self):
        """TBD"""
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText, LongMeaning
        return {'columns': [{'meaning': 'Text', 'name': 'owner', 'type': 'string'},
             {'meaning': 'Text', 'name': 'id', 'type': 'string'},
             {'meaning': 'Text', 'name': 'name', 'type': 'string'},
             {'meaning': 'Text', 'name': 'type', 'type': 'string'},
             {'meaning': 'Text', 'name': 'architecture', 'type': 'string'},
             {'meaning': 'Text', 'name': 'state', 'type': 'string'},
             {'meaning': 'LongMeaning',
              'name': 'usedInScenarios',
              'type': 'int64'},
             {'meaning': 'LongMeaning',
              'name': 'usedInProjects',
              'type': 'int64'},
             {'meaning': 'URL', 'name': 'cluster_url', 'type': 'string'}]}
        }
