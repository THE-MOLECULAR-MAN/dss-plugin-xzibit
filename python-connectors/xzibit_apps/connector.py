####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import flatten_dict, get_dss_base_url


class ConnectorApps(Connector):
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
        # CANNOT have trailing slash on Apps
        return f"{self.__baseurl}/apps/{id}"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        records_generated = 0
        keys = [
            "appId",
            "appVersion",
            "label",
            "origin",
            "shortDesc",
            "tags",
            "isAppImg",
            "instanceCount",
            "useAsRecipe",
            "onlyLimitedVisibility",
        ]
        # iterate through each object
        for item_info in self.__client.list_apps():
            if records_limit > 0 and records_generated >= records_limit:
                return

            next_row = flatten_dict(item_info, include_keys=keys)
            next_row["url"] = self.get_url(next_row["appId"])
            records_generated += 1
            yield next_row

    def get_read_schema(self):
        """TBD"""
        return {
            "columns": [
                {
                    "meaning": "Boolean",
                    "name": "onlyLimitedVisibility",
                    "type": "boolean",
                },
                {"meaning": "Text", "name": "appId", "type": "string"},
                {"meaning": "Text", "name": "appVersion", "type": "string"},
                {"meaning": "Text", "name": "label", "type": "string"},
                {"meaning": "FreeText", "name": "shortDesc", "type": "string"},
                {"meaning": "JSONArrayMeaning", "name": "tags", "type": "string"},
                {"meaning": "Boolean", "name": "isAppImg", "type": "boolean"},
                {"meaning": "Text", "name": "origin", "type": "string"},
                {"meaning": "Text", "name": "originProjectKey", "type": "string"},
                {"meaning": "LongMeaning", "name": "instanceCount", "type": "int"},
                {"meaning": "Boolean", "name": "useAsRecipe", "type": "boolean"},
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
