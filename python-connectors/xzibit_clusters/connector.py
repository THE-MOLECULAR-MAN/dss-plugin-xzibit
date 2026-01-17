"""TBD"""

####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import flatten_dict, get_dss_base_url


class ConnectorClusters(Connector):
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
        # Clusters MUST NOT HAVE trailing slash
        return f"{self.__baseurl}/admin/clusters/{id}"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
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
        # iterate through each object
        for item_info in self.__client.list_clusters():
            next_row = flatten_dict(item_info, include_keys=keys)
            next_row["url"] = self.get_url(next_row["id"])
            yield next_row

    def get_read_schema(self):
        """TBD"""
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
