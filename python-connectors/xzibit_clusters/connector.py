"""Connector that provides a dataset of all Clusters on the DSS instance."""

from dataiku import api_client
from dataiku.connector import Connector

from xzibit.base_connector import XzibitBaseConnector
from xzibit.utils import flatten_dict, get_dss_base_url


class ConnectorClusters(XzibitBaseConnector, Connector):
    """Connector that provides a dataset of all Clusters on the DSS instance."""

    def __init__(self, config, plugin_config):
        super().__init__(config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()

    def get_url(self, cluster_id):
        """Returns the DSS UI URL for the cluster, or None if inputs are missing.

        Cluster URLs must NOT have a trailing slash.
        """
        if any(v is None for v in (self.__baseurl, cluster_id)):
            return None
        return f"{self.__baseurl}/admin/clusters/{cluster_id}"

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
            "architecture",
            "name",
            "owner",
            "state",
            "type",
            "usedInProjects",
            "usedInScenarios",
        ]
        for item_info in self.__client.list_clusters():
            if records_limit > 0 and records_generated >= records_limit:
                return
            next_row = flatten_dict(item_info, include_keys=keys)
            next_row["url"] = self.get_url(next_row["id"])
            records_generated += 1
            yield next_row

    def get_read_schema(self):
        return {
            "columns": [
                {"meaning": "Text", "name": "id", "type": "string"},
                {"meaning": "Text", "name": "name", "type": "string"},
                {"meaning": "Text", "name": "type", "type": "string"},
                {"meaning": "Text", "name": "architecture", "type": "string"},
                {"meaning": "Text", "name": "state", "type": "string"},
                {"meaning": "LongMeaning", "name": "usedInScenarios", "type": "int"},
                {"meaning": "LongMeaning", "name": "usedInProjects", "type": "int"},
                {"meaning": "Text", "name": "owner", "type": "string"},
                {"meaning": "URL", "name": "url", "type": "string"},
            ]
        }
